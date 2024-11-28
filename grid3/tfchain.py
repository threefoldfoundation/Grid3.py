import substrateinterface
from substrateinterface.exceptions import SubstrateRequestException

BLOCK_TIME_SECONDS = 6


class TFChain:
    def __init__(self, network="main"):
        if network == "main":
            url = "wss://tfchain.grid.tf"
        else:
            url = url = "wss://tfchain.{}.grid.tf".format(network)

        self.sub = substrateinterface.SubstrateInterface(
            url=url, ss58_format=42, type_registry_preset="polkadot"
        )

        self.keys = None

    def create_keypair(self, mnemonic):
        """
        Creates a keypair from an existing mnemonic and saves it into the instance for signing operations. Not required for data queries.
        """
        self.keys = substrateinterface.Keypair.create_from_mnemonic(mnemonic)

    def find_block(self, timestamp):
        """
        Find the block that was created nearest to the provided timestamp. Note that this can be a block created after the specified timestamp
        """
        head = self.sub.get_block()
        head_time = self.get_timestamp(head) // 1000  # Convert to 10 digits
        head_number = head["header"]["number"]

        time_diff = head_time - timestamp
        blocks_diff = time_diff // 6  # Six second blocks
        guess_number = head_number - blocks_diff

        while blocks_diff > 0:
            guess_block = self.sub.get_block(block_number=guess_number)
            guess_block_time = self.get_timestamp(guess_block) // 1000
            time_diff = guess_block_time - timestamp
            blocks_diff = time_diff // 6  # Six second blocks
            guess_number -= blocks_diff
            last_time_diff = time_diff
            # print(last_time_diff, time_diff, guess_number)
        if time_diff < 4:
            return guess_number
        else:
            return guess_number - 1

    def find_block_minting(self, timestamp):
        """This is a port of the code found in the tfchain client that's included in the minting repo: https://github.com/threefoldtech/minting_v3/blob/c93b0c69dffec68fc5f5478db6b999112a27ad02/client/src/client.rs#L121

        It's behavior is such that it always returns a future block that was not yet created at the given timestamp, even if the timestamp matches the block timestamp exactly. I'm not sure if that was intended, but in any case, this is the one to reach for when trying to get the block that is considered the beginning or end of a period for minting purposes
        """
        latest_ts = self.get_time_at_block() // 1000
        if latest_ts < timestamp:
            raise ValueError("can't fetch block for future timestamp")
        height = 1
        last_height = 1

        while 1:
            block_hash = self.sub.get_block_hash(height)
            if block_hash is None:
                height = (height + last_height) // 2
                continue

            block_time = self.get_timestamp(self.sub.get_block(block_hash)) // 1000
            time_delta = timestamp - block_time
            block_delta = time_delta // BLOCK_TIME_SECONDS
            if block_delta == 0:
                if time_delta >= 0:
                    return height + 1
                else:
                    return height

            if (height + block_delta) < 0:
                raise RuntimeError()

            last_height = height
            height = height + block_delta

    def find_uptime_report(self, nodeid):
        head = self.sub.get_block_header()["header"]
        while 1:
            events = sub.get_events(head["hash"])
            for event in events:
                if (
                    event.value["event_id"] == "NodeUptimeReported"
                    and event.value["attributes"][0] == nodeid
                ):
                    return head["hash"], event.value["attributes"]
                    break
            else:
                head = self.sub.get_block_header(head["parentHash"])["header"]
                continue
            break

    def find_transfer(self, address):
        head = self.sub.get_block_header()["header"]
        while 1:
            events = self.sub.get_events(head["hash"])
            for event in events:
                try:
                    if event.value["event_id"] == "Transfer" and (
                        event.value["attributes"][0] == address
                        or event.value["attributes"][1] == address
                    ):
                        return head["hash"], event.value["attributes"]
                        break
                except IndexError:
                    print(event)
            else:
                head = self.sub.get_block_header(head["parentHash"])["header"]
                continue
            break

    def get_balance(self, wallet_address, block_hash=None):
        result = self.sub.query("System", "Account", [wallet_address], block_hash)
        return result.value["data"]["free"] / 1e7

    def get_next_farm_id(self):
        return self.sub.query("TfgridModule", "FarmID").value

    def get_farm(self, farm_id, block_hash=None):
        return self.sub.query("TfgridModule", "Farms", [farm_id], block_hash).value

    def get_node(self, node_id, block_hash=None):
        return self.sub.query("TfgridModule", "Nodes", [node_id], block_hash).value

    def get_node_id(self, block_hash=None):
        # Returns highest assigned node id
        return self.sub.query("TfgridModule", "NodeID", [], block_hash).value

    def get_node_by_twin(self, twin_id, block_hash=None):
        return self.sub.query(
            "TfgridModule", "NodeIdByTwinID", [twin_id], block_hash
        ).value

    def get_node_power(self, node_id, block_hash=None):
        return self.sub.query("TfgridModule", "NodePower", [node_id], block_hash).value

    def get_timestamp(self, block):
        # Timestamp should always be first extrinsic (right?)
        # Has millisecond precision, divide by 1000 to get seconds
        return block["extrinsics"][0].value["call"]["call_args"][0]["value"]

    def get_time_at_block(self, block_number=None):
        if block_number is None:
            block = self.sub.get_block()
        else:
            block = self.sub.get_block(block_number=block_number)
        return self.get_timestamp(block)

    def get_twin(self, twin_id):
        return self.sub.query("TfgridModule", "Twins", [twin_id]).value

    def get_twin_by_account(self, account_id, block_hash=None):
        return self.sub.query(
            "TfgridModule", "TwinIdByAccountID", [account_id], block_hash
        ).value

    def set_power_target(self, node_id, target):
        if self.keys is None:
            raise Exception("Please create a keypair first")

        params = {"node_id": node_id, "power_target": target}
        call = self.sub.compose_call("TfgridModule", "change_power_target", params)
        signed = self.sub.create_signed_extrinsic(call, self.keys)
        try:
            receipt = self.sub.submit_extrinsic(signed, wait_for_inclusion=True)
            return receipt

        except SubstrateRequestException as e:
            print("Failed with error: {}".format(e))
