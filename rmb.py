import redis, json, base64, time

class RmbClient:
    """
    A simple wrapper to use the Reliable Message Bus via rmb-peer over Redis. Be sure to start up redis-server and rmb-peer before attempting to use this class.

    For more info on RMB and rmb-peer, see: https://github.com/threefoldtech/rmb-rs
    """
    
    #rmb-peer uses "msgbus.system.reply" for it's own purposes, and farmerbot uses a new uuid for each message. Maybe should use a different approach
    def __init__(self, redis_host='localhost', redis_port=6379, 
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
            