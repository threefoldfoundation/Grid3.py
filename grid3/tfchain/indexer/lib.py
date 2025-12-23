import functools
import sqlite3
from multiprocessing import JoinableQueue, Process
from threading import Thread

import substrateinterface

from .. import tfchain
from .archiver import Archiver


class Indexer:
    def __init__(
        self,
        file,
        db_timeout,
        max_workers,
        min_workers,
        sleep_time,
        post_period,
        retries,
        archive_mode=False,
    ):
        self.file = file
        self.db_timeout = db_timeout
        self.max_workers = max_workers
        self.min_workers = min_workers
        self.sleep_time = sleep_time
        self.post_period = post_period
        self.retries = retries
        self.archive_mode = archive_mode
        self.archive_compressor = None

        # Initialize queues
        self.block_queue = JoinableQueue()
        self.block_queue.cancel_join_thread()
        self.write_queue = JoinableQueue()
        self.write_queue.cancel_join_thread()

        if archive_mode:
            self.archive_compressor = Archiver(file)

    def load_queue(self, con, start_number, end_number):
        missing_blocks = self.find_missing(con, start_number, end_number)

        for i in missing_blocks:
            self.block_queue.put(i)
        return len(missing_blocks)

    def find_missing(self, con, start_block, end_block):
        results = con.execute(
            """
            WITH RECURSIVE range(value) AS (
                SELECT ?
                UNION ALL
                SELECT value + 1 FROM range WHERE value < ?
            )

            SELECT value FROM range
            EXCEPT
            SELECT block_number FROM processed_blocks
            ORDER BY value
            """,
            (start_block, end_block),
        ).fetchall()

        return [row[0] for row in results]

    def db_writer(self):
        con = self.new_connection()

        while 1:
            job = self.write_queue.get()
            if job is None:
                return

            try:
                if job[0] == "archive_batch":
                    # Handle archive batch storage
                    self._write_archive_batch(con, job[1])
                else:
                    # Handle regular block processing
                    block_number, updates, spec_version = job
                    for update in updates:
                        con.execute(*update)
                    con.execute(
                        "INSERT OR IGNORE INTO processed_blocks VALUES(?, ?)",
                        (block_number, spec_version),
                    )
            except Exception as e:
                print("Got an exception in write loop:", e)
                print("While processing job:", job)
            finally:
                self.write_queue.task_done()

    def _write_archive_batch(self, con, batch_data):
        """Write an archive batch to the database."""
        try:
            # Calculate batch ID (could be start_block or a separate counter)
            batch_id = batch_data["start_block"] // 10  # Simple batch ID calculation

            # Store the compressed batch
            con.execute(
                """
                INSERT OR REPLACE INTO archive_blocks
                (batch_id, start_block, end_block, compressed_data, spec_version)
                VALUES (?, ?, ?, ?, ?)
            """,
                (
                    batch_id,
                    batch_data["start_block"],
                    batch_data["end_block"],
                    batch_data["compressed_data"],
                    batch_data["spec_version"],
                ),
            )

            print(
                f"Stored archive batch {batch_id}: blocks {batch_data['start_block']}-{batch_data['end_block']}"
            )

        except Exception as e:
            print(f"Error writing archive batch: {e}")
            raise

    def fetch_powers(self, block_number):
        # To emulating minting properly, we need to know the power state and target of each node at the beginning of the minting period
        # We also look up and store the timestamp of the block when a node went to sleep if it's asleep at the beginning of the period, since it can be essential to computing violations in some rarer cases. Storing the timestamp of all blocks along with their number in the processed blocks table could be a way to make this faster at the expense of a marginal amount of extra disk space, but this is overall not a huge part of the data fetched so I didn't bother with that for now.

        # Retry forever until we got all the data. I didn't see an error yet for this function, but we don't have any retry logic in the main loop for this part
        while 1:
            try:
                # Get our own clients so this can run in a thread
                con = self.new_connection()
                client = tfchain.TFChain()

                block = client.sub.get_block(block_number=block_number)
                block_hash = block["header"]["hash"]

                # This covers the fact that farmerbot related functions were added
                # later. It also saves us from the case of block 0 where there is no
                # timestamp extrinsic. This raises the question of what happens
                # during the first month that farmerbot was live, but that has to be
                # handled by consumer of this data.
                try:
                    client.get_node_power(1, block_hash)
                except substrateinterface.exceptions.StorageFunctionNotFound:
                    print(
                        f"Skipping power fetch for block {block_number} because storage function not found for power state, suggesting farmerbot was not live at this time."
                    )
                    return

                timestamp = client.get_timestamp(block) // 1000

                max_node = client.get_node_id(block_hash)
                nodes = set(range(1, max_node + 1))
                existing_powers = con.execute(
                    "SELECT node_id FROM PowerState WHERE block=?", (block_number,)
                ).fetchall()
                nodes -= {p[0] for p in existing_powers}

                if not nodes:
                    break

                print("Fetching node powers for", len(nodes), "nodes")
                for node in nodes:
                    if node % 500 == 0:
                        print("Processed", node, "initial power states/targets")
                    power = client.get_node_power(node, block_hash)
                    # I seem to remember there being some None values in here at some point, but it seems now that all nodes get a default of Up, Up
                    if power["state"] == "Up":
                        state = "Up"
                        down_block_number = None
                        down_time = None
                    else:
                        state = "Down"
                        down_block_number = power["state"]["Down"]
                        down_block = client.sub.get_block(
                            block_number=down_block_number
                        )
                        down_time = client.get_timestamp(down_block) // 1000
                    con.execute(
                        "INSERT INTO PowerState VALUES(?, ?, ?, ?, ?, ?, ?)",
                        (
                            node,
                            state,
                            down_block_number,
                            down_time,
                            power["target"],
                            block_number,
                            timestamp,
                        ),
                    )
                    con.commit()
            except Exception as e:
                print("Got exception while fetching powers:", e)

    def get_block_data(self, client, block_number):
        # Sometimes we get None here (but only on remote VM?)
        # Maybe better to handle gracefully rather than let proc die
        block = client.sub.get_block(block_number=block_number)
        block_hash = block["header"]["hash"]
        events = client.sub.get_events(block_hash)
        spec_version = client.sub.get_block_runtime_version(block_hash)["specVersion"]
        return block, events, spec_version

    def get_processed_blocks(self, con):
        result = con.execute("SELECT block_number FROM processed_blocks").fetchall()
        return [x[0] for x in result]

    def new_connection(self):
        con = sqlite3.connect(self.file, timeout=self.db_timeout)
        con.execute("PRAGMA journal_mode=wal")
        return con

    def process_block(self, block, events):
        updates = []
        block_number = block["header"]["number"]
        extrinsics = block["extrinsics"]

        # Block 0 has no extrinsics, not even a timestamp. We will still mark it as
        # processed
        if len(extrinsics) == 0:
            return updates

        # Guard here in case the block format ever changes
        call_module = extrinsics[0].value["call"]["call_module"]
        call_function = extrinsics[0].value["call"]["call_function"]
        if call_module != "Timestamp" or call_function != "set":
            raise ValueError(
                f"Expected Timestamp.set extrinsic in position 0, got {call_module}.{call_function}"
            )

        timestamp = extrinsics[0].value["call"]["call_args"][0]["value"] // 1000

        events_by_extrinsic = [[] for _ in extrinsics]

        for i, event in enumerate(events):
            if event.value["phase"] == "ApplyExtrinsic":
                events_by_extrinsic[event.extrinsic_idx].append((i, event))

        for i in range(1, len(extrinsics)):
            extrinsic = extrinsics[i]
            extrinsic_events = events_by_extrinsic[i]

            call_module = extrinsic.value["call"]["call_module"]
            call_function = extrinsic.value["call"]["call_function"]

            # We do some special handling here because ReserveRepatriated events can
            # be also emitted in other cases, namely twin transfers at the moment
            # but possibly others in the future since this is a generic event. So we
            # need to know when ReserveRepatriated is linked to billing, by checking
            # what kind of extrinsic it's a part of
            if (
                call_module == "smartContractModule"
                and call_function == "billContractForBlock"
            ):
                contract_billed = None
                contract_billed_index = None
                contract_id = None
                rewards_distributed = []
                reserves_repatriated = []

                # Probably the events are always in the same order, with
                # ContractBilled coming ahead of the other, and it thus it would
                # work to just create the update inside this loop. But I'm not 100%
                # sure and it could change later, so we collect first then process
                # below
                for event_index, event in extrinsic_events:
                    event_id = event.value["event_id"]
                    attributes = event.value["attributes"]

                    if event_id == "ContractBilled":
                        contract_billed = event
                        contract_billed_index = event_index
                        contract_id = (contract_billed["attributes"]["contract_id"],)
                    elif event_id == "RewardDistributed":
                        rewards_distributed.append((event_index, event))
                    elif event_id == "ReserveRepatriated":
                        reserves_repatriated.append((event_index, event))

                updates.append(
                    (
                        "INSERT INTO ContractBilled(contract_id, billing_timestamp, discount_level, amount_billed, standard_rewards, additional_rewards, block, event_index, timestamp) VALUES(?, ?, ?, ?, ?, ?, ?, ?, ?)",
                        (
                            contract_billed["attributes"]["contract_id"],
                            contract_billed["attributes"]["billing_timestamp"],
                            contract_billed["attributes"]["discount_level"],
                            contract_billed["attributes"]["amount_billed"],
                            contract_billed["attributes"]["standard_rewards"],
                            contract_billed["attributes"]["additional_rewards"],
                            block_number,
                            event_index,
                            timestamp,
                        ),
                    )
                )

                for event_index, repatriation in reserves_repatriated:
                    updates.append(
                        (
                            "INSERT INTO BillingRepatriationEvents(contract_id, from_account, to_account, amount, block, event_index, billing_event_index, timestamp) VALUES(?, ?, ?, ?, ?, ?, ?)",
                            (
                                contract_id,
                                repatriation["attributes"]["from_account"],
                                repatriation["attributes"]["to_account"],
                                repatriation["attributes"]["amount"],
                                block_number,
                                event_index,
                                contract_billed_index,
                                timestamp,
                            ),
                        )
                    )

                for event_index, reward in rewards_distributed:
                    updates.append(
                        (
                            "INSERT INTO RewardDistributed(contract_id, standard_rewards, additional_rewards, block, event_index, billing_event_index, timestamp) VALUES(?, ?, ?, ?, ?, ?, ?)",
                            (
                                reward["attributes"]["contract_id"],
                                reward["attributes"]["standard_rewards"],
                                reward["attributes"]["additional_rewards"],
                                block_number,
                                event_index,
                                contract_billed_index,
                                timestamp,
                            ),
                        )
                    )
            else:
                for event_index, event in extrinsic_events:
                    event_id = event.value["event_id"]
                    attributes = event.value["attributes"]

                    if event_id == "NodeUptimeReported":
                        # Handle both old and new versions of the uptime reporting extrinsic
                        if call_function == "report_uptime":
                            # Old format: attributes is a list of dicts
                            node_id = attributes[0]["value"]
                            uptime = attributes[2]["value"]
                            timestamp_hint = attributes[1]["value"]
                        elif call_function == "report_uptime_v2":
                            # New format: attributes is a tuple
                            node_id = attributes[0]
                            uptime = attributes[2]
                            timestamp_hint = attributes[1]
                        else:
                            # Fallback - try to detect format automatically
                            if isinstance(attributes[0], dict):
                                node_id = attributes[0]["value"]
                                uptime = attributes[2]["value"]
                                timestamp_hint = attributes[1]["value"]
                            else:
                                node_id = attributes[0]
                                uptime = attributes[2]
                                timestamp_hint = attributes[1]

                        updates.append(
                            (
                                "INSERT INTO NodeUptimeReported VALUES(?, ?, ?, ?, ?, ?)",
                                (
                                    node_id,
                                    uptime,
                                    timestamp_hint,
                                    block_number,
                                    event_index,
                                    timestamp,
                                ),
                            )
                        )
                    elif event_id == "PowerTargetChanged":
                        updates.append(
                            (
                                "INSERT INTO PowerTargetChanged VALUES(?, ?, ?, ?, ?, ?)",
                                (
                                    attributes["farm_id"],
                                    attributes["node_id"],
                                    attributes["power_target"],
                                    block_number,
                                    event_index,
                                    timestamp,
                                ),
                            )
                        )
                    elif event_id == "PowerStateChanged":
                        if attributes["power_state"] == "Up":
                            state = "Up"
                            down_block = None
                        else:
                            state = "Down"
                            down_block = attributes["power_state"]["Down"]
                        updates.append(
                            (
                                "INSERT INTO PowerStateChanged VALUES(?, ?, ?, ?, ?, ?, ?)",
                                (
                                    attributes["farm_id"],
                                    attributes["node_id"],
                                    state,
                                    down_block,
                                    block_number,
                                    event_index,
                                    timestamp,
                                ),
                            )
                        )

                    elif event_id == "UpdatedUsedResources":
                        used = attributes["used"]
                        updates.append(
                            (
                                "INSERT INTO UpdatedUsedResources VALUES(?, ?, ?, ?, ?, ?, ?, ?)",
                                (
                                    attributes["contract_id"],
                                    used["hru"],
                                    used["sru"],
                                    used["cru"],
                                    used["mru"],
                                    block_number,
                                    event_index,
                                    timestamp,
                                ),
                            )
                        )
                    elif event_id == "NruConsumptionReportReceived":
                        updates.append(
                            (
                                "INSERT INTO NruConsumptionReportReceived VALUES(?, ?, ?, ?, ?, ?, ?)",
                                (
                                    attributes["contract_id"],
                                    attributes["timestamp"],
                                    attributes["window"],
                                    attributes["nru"],
                                    block_number,
                                    event_index,
                                    timestamp,
                                ),
                            )
                        )
                    elif (
                        event_id == "ContractCreated"
                        and event.value["module_id"] == "SmartContractModule"
                    ):
                        contract_type = attributes["contract_type"]
                        if "NodeContract" in contract_type:
                            node_contract = contract_type["NodeContract"]
                            updates.append(
                                (
                                    "INSERT INTO NodeContractCreated VALUES(?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)",
                                    (
                                        attributes["contract_id"],
                                        attributes["twin_id"],
                                        attributes["version"],
                                        attributes["state"],
                                        node_contract["node_id"],
                                        node_contract["deployment_hash"],
                                        node_contract["deployment_data"],
                                        node_contract["public_ips"],
                                        str(node_contract["public_ips_list"]),
                                        block_number,
                                        timestamp,
                                    ),
                                )
                            )
                        elif "RentContract" in contract_type:
                            rent_contract = contract_type["RentContract"]
                            updates.append(
                                (
                                    "INSERT INTO RentContractCreated VALUES(?, ?, ?, ?, ?, ?, ?)",
                                    (
                                        attributes["contract_id"],
                                        attributes["twin_id"],
                                        attributes["version"],
                                        attributes["state"],
                                        rent_contract["node_id"],
                                        block_number,
                                        timestamp,
                                    ),
                                )
                            )
                        elif "NameContract" in contract_type:
                            name_contract = contract_type["NameContract"]
                            updates.append(
                                (
                                    "INSERT INTO NameContractCreated VALUES(?, ?, ?, ?, ?, ?, ?)",
                                    (
                                        attributes["contract_id"],
                                        attributes["twin_id"],
                                        attributes["version"],
                                        attributes["state"],
                                        name_contract["name"],
                                        block_number,
                                        timestamp,
                                    ),
                                )
                            )
                    elif (
                        event_id == "NodeContractCanceled"
                        and event.value["module_id"] == "SmartContractModule"
                    ):
                        updates.append(
                            (
                                "INSERT INTO NodeContractCanceled VALUES(?, ?, ?, ?, ?)",
                                (
                                    attributes["contract_id"],
                                    attributes["node_id"],
                                    attributes["twin_id"],
                                    block_number,
                                    timestamp,
                                ),
                            )
                        )
                    elif (
                        event_id == "RentContractCanceled"
                        and event.value["module_id"] == "SmartContractModule"
                    ):
                        updates.append(
                            (
                                "INSERT INTO RentContractCanceled VALUES(?, ?, ?)",
                                (
                                    attributes["contract_id"],
                                    block_number,
                                    timestamp,
                                ),
                            )
                        )
                    elif (
                        event_id == "NameContractCanceled"
                        and event.value["module_id"] == "SmartContractModule"
                    ):
                        updates.append(
                            (
                                "INSERT INTO NameContractCanceled VALUES(?, ?, ?)",
                                (
                                    attributes["contract_id"],
                                    block_number,
                                    timestamp,
                                ),
                            )
                        )
                    elif (
                        event_id == "ContractUpdated"
                        and event.value["module_id"] == "SmartContractModule"
                    ):
                        contract_type = attributes["contract_type"]
                        if "NodeContract" in contract_type:
                            node_contract = contract_type["NodeContract"]
                            updates.append(
                                (
                                    "INSERT INTO NodeContractUpdated VALUES(?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)",
                                    (
                                        attributes["contract_id"],
                                        attributes["twin_id"],
                                        attributes["version"],
                                        attributes["state"],
                                        node_contract["node_id"],
                                        node_contract["deployment_hash"],
                                        node_contract["deployment_data"],
                                        node_contract["public_ips"],
                                        str(node_contract["public_ips_list"]),
                                        block_number,
                                        timestamp,
                                    ),
                                )
                            )
                        elif "RentContract" in contract_type:
                            rent_contract = contract_type["RentContract"]
                            updates.append(
                                (
                                    "INSERT INTO RentContractUpdated VALUES(?, ?, ?, ?, ?, ?, ?)",
                                    (
                                        attributes["contract_id"],
                                        attributes["twin_id"],
                                        attributes["version"],
                                        attributes["state"],
                                        rent_contract["node_id"],
                                        block_number,
                                        timestamp,
                                    ),
                                )
                            )
                        elif "NameContract" in contract_type:
                            name_contract = contract_type["NameContract"]
                            updates.append(
                                (
                                    "INSERT INTO NameContractUpdated VALUES(?, ?, ?, ?, ?, ?, ?)",
                                    (
                                        attributes["contract_id"],
                                        attributes["twin_id"],
                                        attributes["version"],
                                        attributes["state"],
                                        name_contract["name"],
                                        block_number,
                                        timestamp,
                                    ),
                                )
                            )

        return updates

    def process_block_batch_for_archive(self, blocks_data):
        """Process a batch of blocks for archive mode.

        Args:
            blocks_data: List of tuples (block_number, block, events, spec_version)

        Returns:
            Tuple of (batch_data, spec_version) for storage
        """
        if not self.archive_mode or self.archive_compressor is None:
            raise ValueError("Archive mode not enabled")

        # Extract blocks and prepare for compression
        blocks = []
        start_block = blocks_data[0][0]
        end_block = blocks_data[-1][0]
        spec_version = blocks_data[0][
            3
        ]  # All blocks in batch should have same spec version

        for block_number, block, events, _ in blocks_data:
            # Add events to block for complete archive
            block_with_events = block.copy()
            block_with_events["events"] = events
            blocks.append(block_with_events)

        # Compress the batch
        compressed_data = self.archive_compressor.compress_block_batch(blocks)

        return {
            "start_block": start_block,
            "end_block": end_block,
            "compressed_data": compressed_data,
            "spec_version": spec_version,
            "blocks": blocks,  # Keep original for potential re-processing
        }

    def processor(self):
        # Each processor has its own TF Chain and db connections
        con = self.new_connection()
        client = tfchain.TFChain()

        # For archive mode, we need to collect blocks in batches
        if self.archive_mode:
            return self.archive_processor(con, client)

        while 1:
            block_number = self.block_queue.get()
            if block_number < 0:
                self.block_queue.task_done()
                return

            exists = con.execute(
                "SELECT 1 FROM processed_blocks WHERE block_number=?", [block_number]
            ).fetchone()

            try:
                if exists is None:
                    block, events, spec_version = self.get_block_data(
                        client, block_number
                    )
                    updates = self.process_block(block, events)
                    self.write_queue.put((block_number, updates, spec_version))

            finally:
                # This allows us to join() the queue later to determine when all queued blocks have been attempted, even if processing failed
                self.block_queue.task_done()

    def archive_processor(self, con, client):
        """Processor for archive mode that handles blocks in batches of 10."""
        batch = []
        batch_size = 10

        while 1:
            block_number = self.block_queue.get()
            if block_number < 0:
                # Process any remaining blocks in the batch before exiting
                if batch:
                    self._process_archive_batch(con, batch)
                self.block_queue.task_done()
                return

            exists = con.execute(
                "SELECT 1 FROM processed_blocks WHERE block_number=?", [block_number]
            ).fetchone()

            try:
                if exists is None:
                    block, events, spec_version = self.get_block_data(
                        client, block_number
                    )
                    batch.append((block_number, block, events, spec_version))

                    # Process batch when we have 10 blocks
                    if len(batch) >= batch_size:
                        self._process_archive_batch(con, batch)
                        batch = []

            finally:
                # This allows us to join() the queue later to determine when all queued blocks have been attempted, even if processing failed
                self.block_queue.task_done()

    def _process_archive_batch(self, con, batch):
        """Process a batch of blocks for archive storage."""
        try:
            # Process the batch for archive
            batch_data = self.process_block_batch_for_archive(batch)

            # Also process individual blocks for regular indexing (if needed)
            # This maintains compatibility with existing functionality
            for block_number, block, events, spec_version in batch:
                updates = self.process_block(block, events)
                self.write_queue.put((block_number, updates, spec_version))

            # Store the compressed batch
            self.write_queue.put(("archive_batch", batch_data))

        except Exception as e:
            print(f"Error processing archive batch: {e}")
            # Re-queue individual blocks for regular processing
            for block_number, _, _, _ in batch:
                self.block_queue.put(block_number)

    def parallelize(self, con, start_number, end_number):
        self.load_queue(con, start_number, end_number)

        print(
            "Starting",
            self.max_workers,
            "workers to process",
            self.block_queue.qsize(),
            "blocks, with starting block number",
            start_number,
            "and ending block number",
            end_number,
        )

        processes = [self.spawn_worker() for i in range(self.max_workers)]
        return processes

    def prep_db(self, con):
        # While block number and timestamp of the block are 1-1, converting between
        # them later is not trivial, so it can be helpful to have both. We also
        # store the event index, because the ordering of events within a block can
        # be important from the perspective of minting (in rare cases). For
        # uptime_hint, this is as far as I know always equal to the block timestamp
        # // 1000. Note that we also convert all incoming timestamps to whole
        # second precision
        # Each event should be uniquely identified by its block and event numbers
        con.execute(
            "CREATE TABLE IF NOT EXISTS NodeUptimeReported(node_id, uptime, timestamp_hint, block, event_index, timestamp, UNIQUE(event_index, block))"
        )

        con.execute(
            "CREATE INDEX IF NOT EXISTS NodeUptimeReported_node_id_ts ON NodeUptimeReported(node_id, timestamp)"
        )

        con.execute(
            "CREATE TABLE IF NOT EXISTS PowerTargetChanged(farm_id, node_id, target, block, event_index, timestamp, UNIQUE(event_index, block))"
        )

        con.execute(
            "CREATE INDEX IF NOT EXISTS PowerTargetChanged_node_id_ts ON PowerTargetChanged(node_id, timestamp)"
        )

        con.execute(
            "CREATE TABLE IF NOT EXISTS PowerStateChanged(farm_id, node_id, state, down_block, block, event_index, timestamp, UNIQUE(event_index, block))"
        )

        con.execute(
            "CREATE INDEX IF NOT EXISTS PowerStateChanged_node_id_timestamp ON PowerStateChanged(node_id, timestamp)"
        )

        con.execute(
            "CREATE TABLE IF NOT EXISTS PowerState(node_id, state, down_block, down_time, target, block, timestamp, UNIQUE(node_id, block))"
        )

        con.execute(
            "CREATE TABLE IF NOT EXISTS ContractBilled(contract_id, billing_timestamp, discount_level, amount_billed, standard_rewards, additional_rewards, block, event_index, timestamp, UNIQUE(event_index, block))"
        )

        con.execute(
            "CREATE TABLE IF NOT EXISTS BillingRepatriationEvents(contract_id, from_account, to_account, amount, block, event_index, billing_event_index, timestamp, UNIQUE(event_index, block), FOREIGN KEY(block, billing_event_index) REFERENCES ContractBilled(block, event_index))"
        )

        con.execute(
            "CREATE INDEX IF NOT EXISTS BillingRepatriationEvents_query_idx ON BillingRepatriationEvents(from_account, timestamp)"
        )

        con.execute(
            "CREATE TABLE IF NOT EXISTS RewardDistributed(contract_id, standard_rewards, additional_rewards, block, event_index, billing_event_index, timestamp, UNIQUE(event_index, block), FOREIGN KEY(block, event_index) REFERENCES ContractBilled(block, event_index))"
        )

        con.execute(
            "CREATE INDEX IF NOT EXISTS RewardDistributed_contract_id_ts ON RewardDistributed(contract_id, timestamp)"
        )

        con.execute(
            "CREATE TABLE IF NOT EXISTS UpdatedUsedResources(contract_id, hru, sru, cru, mru, block, event_index, timestamp, UNIQUE(event_index, block))"
        )

        con.execute(
            "CREATE TABLE IF NOT EXISTS NruConsumptionReportReceived(contract_id, report_timestamp, window, nru, block, event_index, timestamp, UNIQUE(event_index, block))"
        )

        con.execute(
            "CREATE TABLE IF NOT EXISTS NodeContractCreated(contract_id, twin_id, version, state, node_id, deployment_hash, deployment_data, public_ips, public_ips_list, block, timestamp, UNIQUE(contract_id))"
        )

        con.execute(
            "CREATE TABLE IF NOT EXISTS RentContractCreated(contract_id, twin_id, version, state, node_id, block, timestamp, UNIQUE(contract_id))"
        )

        con.execute(
            "CREATE TABLE IF NOT EXISTS NameContractCreated(contract_id, twin_id, version, state, name, block, timestamp, UNIQUE(contract_id))"
        )

        con.execute(
            "CREATE TABLE IF NOT EXISTS NodeContractCanceled(contract_id, node_id, twin_id, block, timestamp, UNIQUE(contract_id))"
        )

        con.execute(
            "CREATE TABLE IF NOT EXISTS RentContractCanceled(contract_id, block, timestamp, UNIQUE(contract_id))"
        )

        con.execute(
            "CREATE TABLE IF NOT EXISTS NameContractCanceled(contract_id, block, timestamp, UNIQUE(contract_id))"
        )

        con.execute(
            "CREATE TABLE IF NOT EXISTS NodeContractUpdated(contract_id, twin_id, version, state, node_id, deployment_hash, deployment_data, public_ips, public_ips_list, block, timestamp)"
        )
        con.execute(
            "CREATE TABLE IF NOT EXISTS RentContractUpdated(contract_id, twin_id, version, state, node_id, block, timestamp)"
        )
        con.execute(
            "CREATE TABLE IF NOT EXISTS NameContractUpdated(contract_id, twin_id, version, state, name, block, timestamp)"
        )

        con.execute(
            "CREATE TABLE IF NOT EXISTS processed_blocks(block_number PRIMARY KEY, spec_version)"
        )

        con.execute("CREATE TABLE IF NOT EXISTS kv(key UNIQUE, value)")
        con.execute("INSERT OR IGNORE INTO kv VALUES('checkpoint_block', 0)")
        con.execute("INSERT OR IGNORE INTO kv VALUES('checkpoint_time', 0)")

        con.commit()

    def scale_workers(self, processes):
        if self.block_queue.qsize() < 2 and len(processes) > self.min_workers:
            print("Queue cleared, scaling down workers")
            for i in range(len(processes) - self.min_workers):
                self.block_queue.put(-1 - i)

        if (
            self.block_queue.qsize() < self.max_workers
            and len(processes) < self.min_workers
        ):
            print(
                "Queue is small, but fewer than",
                self.min_workers,
                "workers are alive. Spawning more workers",
            )
            for i in range(self.min_workers - len(processes)):
                processes.append(self.spawn_worker())

        if (
            self.block_queue.qsize() > self.max_workers
            and len(processes) < self.max_workers
        ):
            print(
                "More than",
                self.max_workers,
                "jobs remaining but fewer processes. Spawning more workers",
            )
            for i in range(self.max_workers - len(processes)):
                processes.append(self.spawn_worker())

    def spawn_subscriber(self, client):
        callback = functools.partial(self.subscription_callback)
        sub_thread = Thread(target=client.sub.subscribe_block_headers, args=[callback])
        sub_thread.daemon = True
        sub_thread.start()
        return sub_thread

    def spawn_worker(self):
        process = Process(target=self.processor)
        process.daemon = True
        process.start()
        return process

    def subscription_callback(self, head, update_nr, subscription_id):
        self.block_queue.put(head["header"]["number"])
