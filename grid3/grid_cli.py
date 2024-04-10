"""
Wrapper for the tf-grid-cli command line application
"""

import subprocess, os, json

def execute(args):
    return subprocess.run(['tfcmd'] + args, capture_output=True)

def cancel(name):
    return execute(['cancel', name])

def login():
    execute('login')

def get_contracts():
    print(execute(['get', 'contracts']).stdout.decode())

class VM:
    def __init__(self, name, node_id=None, flist=None, entrypoint=None, ssh=None):
        self.name = name
        self.node_id = node_id
        self.flist = flist
        self.entrypoint = entrypoint
        self.ssh = ssh
        self.destroyed = False

    def deploy(self):
        args = ['deploy', 'vm', '--name', self.name]
        if self.node_id:
            args.extend(['--node', str(node_id)])
        if self.flist:
            args.extend(['--flist', flist])
            args.append('--entrypoint')
            if self.entrypoint is None:
                args.append('/sbin/zinit init')
            else:
                args.append(self.entrypoint)

        args.append('--ssh')
        if self.ssh:
            args.append(self.ssh)
        else:
            args.append(os.path.expanduser('~/.ssh/id_rsa.pub'))

        self.deploy_proc = execute(args)

        if 'yggdrasil ip' in self.deploy_proc.stderr.decode('utf-8'):
            self.ygg_ip = output.split('yggdrasil ip: ')[1].rstrip('\n')
        else:
            self.ygg_ip = None

    def destroy(self):
        self.destroy_proc = cancel(self.name)
        self.destroyed = True

    def get_info(self):
        # Store the proc in case we need it for debugging (?)
        self.get_proc = execute(['get', 'vm', self.name])
        props = json.loads(self.get_proc.stderr.decode().split('vm:')[1])
        # We could remove some fields here, like Zdbs, empty ones like Disks
        vms = props.pop('Vms')
        self.__dict__.update(props)
        self.__dict__.update(vms[0])