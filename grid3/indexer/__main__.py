import sqlite3, datetime, time, logging, functools, argparse
from threading import Thread
from multiprocessing import Process, JoinableQueue
from websocket._exceptions import (
    WebSocketConnectionClosedException,
    WebSocketAddressException,
)
import prometheus_client
from .. import tfchain
from .. minting.period import Period

MIN_WORKERS = 2
SLEEP_TIME = 30
DB_TIMEOUT = 30
POST_PERIOD = 60 * 60

# When querying a fixed period of blocks, how many times to retry missed blocks
RETRIES = 3


def load_queue(con, start_number, end_number, block_queue):
    missing_blocks = find_missing(con, start_number, end_number)

    for i in missing_blocks:
        block_queue.put(i)
    return len(missing_blocks)


def find_missing(con, start_block, end_block):
    results = con.execute(
        """
        WITH RECURSIVE range(value) AS (
            SELECT ?
            UNION ALL
            SELECT value + 1 FROM range WHERE value < ?
        )

        SELECT value FROM range
        EXCEPT
        SELECT * FROM processed_blocks
        ORDER BY value
        """,
        (start_block, end_block),
    ).fetchall()

    return [row[0] for row in results]


def db_writer(write_queue):
    con = new_connection()

    while 1:
        job = write_queue.get()
        if job is None:
            return

        try:
            with con:
                block_number, updates = job
                for update in updates:
                    con.execute(*update)
                con.execute(
                    "INSERT OR IGNORE INTO processed_blocks VALUES(?)", (block_number,)
                )
        except Exception as e:
            print("Got an exception in write loop:", e)
            print("While processing job:", job)
        finally:
            write_queue.task_done()


def fetch_powers(block_number, db_file=None):
    # To emulating minting properly, we need to know the power state and target of each node at the beginning of the minting period
    # We also look up and store the timestamp of the block when a node went to sleep if it's asleep at the beginning of the period, since it can be essential to computing violations in some rarer cases. Storing the timestamp of all blocks along with their number in the processed blocks table could be a way to make this faster at the expense of a marginal amount of extra disk space, but this is overall not a huge part of the data fetched so I didn't bother with that for now.

    # Retry forever until we got all the data. I didn't see an error yet for this function, but we don't have any retry logic in the main loop for this part
    while 1:
        try:
            # Get our own clients so this can run in a thread
            con = new_connection(db_file=db_file)
            client = tfchain.TFChain()

            block = client.sub.get_block(block_number=block_number)
            block_hash = block["header"]["hash"]
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
                    down_block = client.sub.get_block(block_number=down_block_number)
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


def get_block(client, block_number):
    # Sometimes we get None here (but only on remote VM?)
    # Maybe better to handle gracefully rather than let proc die
    block = client.sub.get_block(block_number=block_number)
    events = client.sub.get_events(block["header"]["hash"])
    return block, events


def get_processed_blocks(con):
    result = con.execute("SELECT * FROM processed_blocks").fetchall()
    return [x[0] for x in result]


def new_connection(db_file=None):
    if db_file is None:
        db_file = args.file
    con = sqlite3.connect(db_file, timeout=DB_TIMEOUT)
    con.execute("PRAGMA journal_mode=wal")
    return con


def process_block(block, events):
    block_number = block["header"]["number"]
    timestamp = block["extrinsics"][0].value["call"]["call_args"][0]["value"] // 1000

    updates = []
    for i, event in enumerate(events):
        event = event.value
        event_id = event["event_id"]
        attributes = event["attributes"]
        # TODO: pass these more efficiently than writing the INSERT string for each one
        if event_id == "NodeUptimeReported":
            updates.append(
                (
                    "INSERT INTO NodeUptimeReported VALUES(?, ?, ?, ?, ?, ?)",
                    (
                        attributes[0],
                        attributes[2],
                        attributes[1],
                        block_number,
                        i,
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
                        i,
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
                        i,
                        timestamp,
                    ),
                )
            )

    return updates


def processor(block_queue, write_queue):
    # Each processor has its own TF Chain and db connections
    con = new_connection()
    client = tfchain.TFChain()
    while 1:
        block_number = block_queue.get()
        if block_number < 0:
            block_queue.task_done()
            return

        exists = con.execute(
            "SELECT 1 FROM processed_blocks WHERE block_number=?", [block_number]
        ).fetchone()

        try:
            if exists is None:
                block, events = get_block(client, block_number)
                updates = process_block(block, events)
                write_queue.put((block_number, updates))

        finally:
            # This allows us to join() the queue later to determine when all queued blocks have been attempted, even if processing failed
            block_queue.task_done()


def parallelize(con, start_number, end_number, block_queue, write_queue):
    load_queue(con, start_number, end_number, block_queue)

    print(
        "Starting",
        args.max_workers,
        "workers to process",
        block_queue.qsize(),
        "blocks, with starting block number",
        start_number,
        "and ending block number",
        end_number,
    )

    processes = [
        spawn_worker(block_queue, write_queue) for i in range(args.max_workers)
    ]
    return processes


def prep_db(con):
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

    con.execute("CREATE TABLE IF NOT EXISTS processed_blocks(block_number PRIMARY KEY)")

    con.execute("CREATE TABLE IF NOT EXISTS kv(key UNIQUE, value)")
    con.execute("INSERT OR IGNORE INTO kv VALUES('checkpoint_block', 0)")
    con.execute("INSERT OR IGNORE INTO kv VALUES('checkpoint_time', 0)")

    con.commit()


def scale_workers(processes, block_queue, write_queue):
    if block_queue.qsize() < 2 and len(processes) > MIN_WORKERS:
        print("Queue cleared, scaling down workers")
        for i in range(len(processes) - MIN_WORKERS):
            block_queue.put(-1 - i)

    if block_queue.qsize() < args.max_workers and len(processes) < MIN_WORKERS:
        print(
            "Queue is small, but fewer than",
            MIN_WORKERS,
            "workers are alive. Spawning more workers",
        )
        for i in range(MIN_WORKERS - len(processes)):
            processes.append(spawn_worker(block_queue, write_queue))

    if block_queue.qsize() > args.max_workers and len(processes) < args.max_workers:
        print(
            "More than",
            args.max_workers,
            "jobs remaining but fewer processes. Spawning more workers",
        )
        for i in range(args.max_workers - len(processes)):
            processes.append(spawn_worker(block_queue, write_queue))


def spawn_subscriber(block_queue, client):
    callback = functools.partial(subscription_callback, block_queue)
    sub_thread = Thread(target=client.sub.subscribe_block_headers, args=[callback])
    sub_thread.daemon = True
    sub_thread.start()
    return sub_thread


def spawn_worker(block_queue, write_queue):
    process = Process(target=processor, args=[block_queue, write_queue])
    process.daemon = True
    process.start()
    return process


def subscription_callback(block_queue, head, update_nr, subscription_id):
    block_queue.put(head["header"]["number"])


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument(
        "-f",
        "--file",
        help="Specify the database file name.",
        type=str,
        default="tfchain.db",
    )
    parser.add_argument(
        "-s",
        "--start",
        help="Give a timestamp to start scanning blocks. If omitted, scanning starts from beginning of current minting period",
        type=int,
    )
    parser.add_argument(
        "--start-block", help="Give a block number to start scanning blocks", type=int
    )
    parser.add_argument(
        "-e",
        "--end",
        help="By default, scanning continues to process new blocks as they are generated. When an end timestamp is given, scanning stops at that block height and the program exits",
        type=int,
    )
    parser.add_argument(
        "--end-block",
        help="Specify end by block number rather than timestamp",
        type=int,
    )
    parser.add_argument(
        "-m",
        "--max-workers",
        help="Maximum number of worker processes to spawn",
        type=int,
        default=50,
    )

    args = parser.parse_args()

    print("Staring up, preparing to ingest some blocks, nom nom")

    # Prep database and grab already processed blocks
    con = new_connection()
    prep_db(con)

    # Start tfchain client
    client = tfchain.TFChain()

    if args.start_block:
        start_number = args.start_block
    elif args.start:
        start_number = client.find_block_minting(args.start)
    else:
        # By default, use beginning of current minting period
        start_number = client.find_block_minting(Period().start)

    # Without cancel_join_thread, we can end up deadlocked on trying to flush buffers out to the queue when the program is exiting, since the processes consuming the queue will exit first. We don't care about the data loss implications because all of our data can be fetched again
    block_queue = JoinableQueue()
    block_queue.cancel_join_thread()
    write_queue = JoinableQueue()
    block_queue.cancel_join_thread()

    writer_proc = Process(target=db_writer, args=[write_queue])
    writer_proc.daemon = True
    writer_proc.start()

    powers_thread = Thread(target=fetch_powers, args=[start_number])
    powers_thread.daemon = True
    powers_thread.start()

    if args.end or args.end_block:
        if args.end_block:
            end_number = args.end_block
        else:
            end_number = client.find_block_minting(args.end + POST_PERIOD)

        processes = parallelize(con, start_number, end_number, block_queue, write_queue)

        while (block_qsize := block_queue.qsize()) > 0:
            time.sleep(SLEEP_TIME)
            processes = [t for t in processes if t.is_alive()]
            print(
                datetime.datetime.now(),
                "processed",
                block_qsize - block_queue.qsize(),
                "blocks in",
                SLEEP_TIME,
                "seconds",
                block_queue.qsize(),
                "blocks remaining",
                len(processes),
                "processes alive",
                write_queue.qsize(),
                "write jobs",
            )
            scale_workers(processes, block_queue, write_queue)

        print("Joining blocks queue")
        block_queue.join()
        print("Joining write queue")
        write_queue.join()
        # Retry any missed blocks three times. Since we don't handle errors in the when fetching and processing blocks, it's normal to miss a few
        while missing_count := load_queue(con, start_number, end_number, block_queue):
            print(
                datetime.datetime.now(),
                missing_count,
                "blocks to retry",
                len(processes),
                "processes alive",
            )
            block_queue.join()
            write_queue.join()

        # Finally wait for any remaining jobs to complete
        block_queue.join()
        write_queue.join()
        # Signal remaining processes to exit
        [block_queue.put(-1) for p in processes if p.is_alive()]
        write_queue.put(None)

    else:
        # This is the case where we continue running and fetch all new blocks as they are generated

        # Prep Prometheus instrumentation. We only use this in long running mode
        prometheus_client.start_http_server(8000)
        blocks_counter = prometheus_client.Counter(
            "blocks_processed", "Counts how many blocks have processed successfully"
        )
        blocks_gauge = prometheus_client.Gauge(
            "block_number", "Highest block number processed so far"
        )
        block_queue_gauge = prometheus_client.Gauge(
            "block_queue_length", "How many blocks are queued to be processed"
        )
        write_queue_gauge = prometheus_client.Gauge(
            "write_queue_length", "Current number of items in write queue"
        )

        # Since using the subscribe method blocks, we give it a thread
        sub_thread = spawn_subscriber(block_queue, client)

        # We wait to get the first block number back from the subscribe callback, so that we're sure which block is the end of the historic range we want to queue up
        block_number = block_queue.get()
        block_queue.put(block_number)
        processes = parallelize(
            con, start_number, block_number - 1, block_queue, write_queue
        )

        current_period = Period()
        processed_count = con.execute(
            "SELECT COUNT(1) FROM processed_blocks"
        ).fetchone()[0]

        checkpoint_block = con.execute(
            "SELECT value FROM kv WHERE key='checkpoint_block'"
        ).fetchone()[0]
        if checkpoint_block == 0:
            con.execute(
                "UPDATE kv SET value=? WHERE key='checkpoint_block'", (start_number,)
            )
            con.commit()

        while 1:
            time.sleep(SLEEP_TIME)

            # We can periodically get disconnected from the websocket. On each loop we try once to reconnect in case we need the client below
            if not client.sub.websocket.connected:
                try:
                    client.sub.connect_websocket()
                except WebSocketAddressException as e:
                    print(e)

            # We just discard any processes that have died for any reason. They will be replaced by the auto scaling. In fact, we don't try to handle errors at all in the worker processes--the blocks just get retried later
            processes = [t for t in processes if t.is_alive()]
            new_count = con.execute("SELECT COUNT(1) FROM processed_blocks").fetchone()[
                0
            ]
            processed_this_period = new_count - processed_count
            print(
                "{} processed {} blocks in {} seconds {} blocks queued {} processes alive {} write jobs".format(
                    datetime.datetime.now(),
                    processed_this_period,
                    SLEEP_TIME,
                    block_queue.qsize(),
                    len(processes),
                    write_queue.qsize(),
                )
            )
            processed_count = new_count

            blocks_counter.inc(processed_this_period)
            write_queue_gauge.set(write_queue.qsize())
            block_queue_gauge.set(block_queue.qsize())
            blocks_gauge.set(
                con.execute(
                    "SELECT MAX(block_number) FROM processed_blocks"
                ).fetchone()[0]
            )

            # Check for missing blocks only when the queue is cleared, to avoid placing duplicate entries in the queue. In theory it's possible the queue never empties due to bad conditions, but in practice the resting state is an empty block queue
            # We record the max block for which we have processed all preceding blocks as a "checkpoint" and also the timestamp. This helps keep this computation in check as the size of processed blocks grows. We'll also use the checkpoint timestamps when searching for violations, to see if block processing has fallen behind
            if block_queue.qsize() == 0:
                first_block = con.execute(
                    "SELECT value FROM kv WHERE key='checkpoint_block'"
                ).fetchone()[0]
                print("Block checkpoint is:", first_block)
                print(
                    "Block checkpoint time is:",
                    con.execute(
                        "SELECT value FROM kv WHERE key='checkpoint_time'"
                    ).fetchone()[0],
                )

                last_block = con.execute(
                    "SELECT MAX(block_number) FROM processed_blocks"
                ).fetchone()[0]
                print("Last processed block is:", last_block)
                missing_blocks = find_missing(con, first_block, last_block)

                if missing_blocks:
                    for b in missing_blocks:
                        block_queue.put(b)
                    print("Queued", len(missing_blocks), "missing blocks")
                else:
                    # TODO: Ideally we would store the timestamps of the blocks as they are processed initially rather than querying for it again
                    try:
                        block = client.sub.get_block(block_number=last_block)
                        timestamp = client.get_timestamp(block) // 1000
                        with con:
                            con.execute(
                                "UPDATE kv SET value=? WHERE key='checkpoint_block'",
                                (last_block,),
                            )
                            con.execute(
                                "UPDATE kv SET value=? WHERE key='checkpoint_time'",
                                (timestamp,),
                            )
                    except WebSocketConnectionClosedException as e:
                        # We already try reconnecting on each pass of the loop, so here just log the error and move on
                        print(e)

            scale_workers(processes, block_queue, write_queue)

            # If we have entered a new minting period, spawn a thread to fetch the power info for each node at the start of the new period
            period = Period()
            if period.offset > current_period.offset:
                start_number = client.find_block_minting(period.start)
                powers_thread = Thread(target=fetch_powers, args=[start_number])
                powers_thread.daemon = True
                powers_thread.start()
                current_period = period

            # Also make sure we keep alive our subscription thread. If there's an error in the callback, it propagates up and the thread dies
            if not sub_thread.is_alive():
                print("Subscription thread died, respawning it")
                sub_thread = spawn_subscriber(block_queue, client)

            if not writer_proc.is_alive():
                print("Writer proc died, respawning it")
                writer_proc = Process(target=db_writer, args=[write_queue])
                writer_proc.daemon = True
                writer_proc.start()
