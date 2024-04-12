import substrateinterface
from substrateinterface.exceptions import SubstrateRequestException

class TFChain:

    def __init__(self, network='main'):
        if network == 'main':
            url = 'wss://tfchain.grid.tf'
        else:
            usrl = url='wss://tfchain.{}.grid.tf'.format(network)

        self.sub = substrateinterface.SubstrateInterface(
                    url=url, 
                    ss58_format=42, 
                    type_registry_preset='polkadot')

        self.keys = None

    def create_keypair(self, mnemonic):
        """
        Creates a keypair from an existing mnemonic and saves it into the instance for signing operations. Not required for data queries.
        """
        self.keys = substrateinterface.Keypair.create_from_mnemonic(mnemonic)

    def find_block(self, timestamp):
        """
        Find the block that was created nearest to the provided timestamp
        """
        head = self.sub.get_block()
        #
        # Timestamp should always be first extrinsic (right?)
        head_time = self.get_timestamp(head) // 1000 # Convert to 10 digits
        head_number = head['header']['number']
        #
        time_diff = head_time - timestamp
        blocks_diff = time_diff // 6 # Six second blocks
        guess_number = head_number - blocks_diff
        #
        guess_block = self.sub.get_block(block_number=guess_number)
        last_time_diff = time_diff
        guess_block_time = self.get_timestamp(guess_block) // 1000
        time_diff = guess_block_time - timestamp
        #print(last_time_diff, time_diff, guess_number)
        while abs(time_diff) < abs(last_time_diff):
            blocks_diff = time_diff // 6 # Six second blocks
            guess_number -= blocks_diff
            guess_block = self.sub.get_block(block_number=guess_number)
            last_time_diff = time_diff
            guess_block_time = self.get_timestamp(guess_block) // 1000
            time_diff = guess_block_time - timestamp
            #print(last_time_diff, time_diff, guess_number)
        return guess_block


    def find_uptime_report(self, nodeid):
        head = self.sub.get_block_header()['header']
        while 1:
            events = sub.get_events(head['hash'])
            for event in events:
                if event.value['event_id'] == 'NodeUptimeReported' and event.value['attributes'][0] == nodeid:
                    return head['hash'], event.value['attributes']
                    break
            else:
                head = self.sub.get_block_header(head['parentHash'])['header']
                continue
            break

    def find_transfer(self, address):
        head = self.sub.get_block_header()['header']
        while 1:
            events = self.sub.get_events(head['hash'])
            for event in events:
                try:
                    if event.value['event_id'] == 'Transfer' and (event.value['attributes'][0] == address or event.value['attributes'][1] == address):
                        return head['hash'], event.value['attributes']
                        break
                except IndexError:
                    print(event)
            else:
                head = self.sub.get_block_header(head['parentHash'])['header']
                continue
            break

    def get_timestamp(self, block):
        return block['extrinsics'][0].value['call']['call_args'][0]['value']

    def get_next_farm_id(self):
        return self.sub.query('TfgridModule', 'FarmID').value

    def get_farm(self, farm_id):
        return self.sub.query('TfgridModule', 'Farms', [farm_id]).value

    def get_node(self, node_id):
        return self.sub.query('TfgridModule', 'Nodes', [node_id]).value

    def get_node_by_twin(self, twin_id, block_hash=None):
        return self.sub.query('TfgridModule', 'NodeIdByTwinID', [twin_id], block_hash).value

    def get_twin(self, twin_id):
        return self.sub.query('TfgridModule', 'Twins', [twin_id]).value

    def get_twin_by_account(self, account_id, block_hash=None):
        return self.sub.query('TfgridModule', 'TwinIdByAccountID', [account_id], block_hash).value

    def get_balance(self, wallet_address):
        result = self.sub.query('System', 'Account', [wallet_address])
        return result.value['data']['free'] / 1e7

    def set_power_target(self, node_id, target):
        if self.keys is None:
            raise Exception('Please create a keypair first')

        params = {'node_id': node_id, 'power_target': target}
        call = self.sub.compose_call('TfgridModule', 
                                     'change_power_target', 
                                     params)
        signed = self.sub.create_signed_extrinsic(call, self.keys)
        try:
            receipt = self.sub.submit_extrinsic(signed,          
                                                wait_for_inclusion=True)
            return receipt

        except SubstrateRequestException as e:
            print("Failed with error: {}".format(e))
