from grid3 import network, rmb

node = input('Node id? ')
timeout = int(input('Timeout in seconds? '))

mainnet = network.GridNetwork()
rmbclient = rmb.RmbClient()
node_twin = mainnet.proxy.get_node(node)['twinId']
rmbclient.send('zos.statistics.get', node_twin, '', timeout)
msg = rmbclient.receive(timeout)
if msg:
    print('Node responded before timeout')
else:
    print("Node didn't respond")
