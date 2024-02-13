import redis, json, base64, time, subprocess, socket, shutil

class RmbClient:
    """
    A simple wrapper to use the Reliable Message Bus via rmb-peer over Redis. Be sure to start up redis-server and rmb-peer before attempting to use this class.

    For more info on RMB and rmb-peer, see: https://github.com/threefoldtech/rmb-rs
    """
    
    #rmb-peer uses "msgbus.system.reply" for it's own purposes, and farmerbot uses a new uuid for each message. Maybe should use a different approach
    def __init__(self, redis_port=6379, redis_host='localhost',
                 reply_queue='msgbus.system.replies'):
        self.redis = redis.Redis(redis_host, port=redis_port, 
                                 decode_responses=True)
        self.reply_queue = reply_queue

    def push(self, ver, ref, cmd, exp, dat, tag, dst, ret, shm):
        msg = {'ver': ver, 'ref': ref, 'cmd': cmd, 'exp': exp, 'dat': dat, 'tag': tag, 'dst': dst, 'ret': ret, 'shm': shm, 'now': int(time.time())}

        # Remove optional keys
        for key in ['ref', 'tag', 'shm']:
            if not msg[key]:
                msg.pop(key)
        msg = json.dumps(msg)
        self.redis.rpush('msgbus.system.local', msg)
    
    def receive(self, timeout=0, queue=''):
        if not queue:
            queue = self.reply_queue
        reply = self.redis.blpop(queue, timeout)
        if reply:
            reply = json.loads(reply[1])
            if reply['dat']:
                reply['dat'] = base64.b64decode(reply['dat']).decode('utf-8')
        return reply

    def send(self, cmd, dst, dat='', exp_delta=60, ver=1, ref='', tag='', 
             ret='', shm=''):
        if not ret:
            ret = self.reply_queue
        exp = int(time.time()) + exp_delta
        try:
            dst[0]
        except TypeError:
            dst = [dst]

        self.push(ver, ref, cmd, exp, dat, tag, dst, ret, shm)

class RmbPeer:
    """
    A container to launch rmb-peer instances. Uses existing redis server instance or can spawn one. When spawning redis and redis_port is None, a free port number will automatically be requested from the OS. Pass None to logfile to print logs to stdout.
    """

    tfchain_urls = {'main': 'wss://tfchain.grid.tf:443', 
                    'test': 'wss://tfchain.test.grid.tf:443',
                    'qa': 'wss://tfchain.qa.grid.tf:443', 
                    'dev': 'wss://tfchain.dev.grid.tf:443'}

    relay_urls = {  'main': 'wss://relay.grid.tf:443', 
                    'test': 'wss://relay.test.grid.tf:443',
                    'qa': 'wss://relay.qa.grid.tf:443', 
                    'dev': 'wss://relay.dev.grid.tf:443'}

    def __init__(self, secret, network='main', peer_logfile='rmb-peer.log', 
                 redis_port=6379, key_type='sr25519', tfchain_url=None, 
                 relay_url=None, redis_url=None, debug=False,
                 path=None, spawn_redis=False, 
                 redis_logfile='redis.log'):

        self.redis_port = redis_port

        if spawn_redis:
            if redis_port is None:
                # We ask the OS for a free port if None is given, then close the socket immediately so we can use it
                sock = socket.socket()
                sock.bind(('', 0))
                self.redis_port = sock.getsockname()[1]
                sock.close()

            if redis_logfile is None:
                redis_stdout = None
            else:
                redis_stdout = open(redis_logfile, 'a')

            redis_call = ['redis-server', '--port', str(self.redis_port)]
            self.redis = subprocess.Popen(redis_call, stdout=redis_stdout, 
                                          stderr=redis_stdout)

        if tfchain_url is None:
            tfchain_url = self.tfchain_urls[network]

        if relay_url is None:
            relay_url = self.relay_urls[network]

        if redis_url is None:
            if self.redis_port is None:
                raise Exception('Redis port of None only valid when spawning redis')
            redis_url = 'redis://localhost:' + str(self.redis_port)

        if path is None:
            for p in ['rmb-peer', './rmb-peer']:
                if shutil.which(p):
                    path = p

            if path is None:
                raise Exception('rmb-peer binary not found')
                
        call = [path]

        if debug:
            call.append('-d')

        call.extend(['-s', tfchain_url, '--relay', relay_url, 
                     '-r', redis_url, '--mnemonic', secret, '-k', key_type])

        if peer_logfile is None:
            stdout = None
        else:
            stdout = open(peer_logfile, 'a')

        self.peer = subprocess.Popen(call, stdout=stdout, stderr=stdout)