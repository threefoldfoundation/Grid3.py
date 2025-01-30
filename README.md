# Grid3.py

This is a collection of Python modules for working with ThreeFold Grid v3. It's designed foremostly for interactive use on the REPL and joyful scripting. We value conciseness and trying to do what you *meant*, even at the expense of a few extra CPU cycles.

The following are included:

* Client for querying data from GraphQL and Grid Proxy
* TFChain client mostly for queries but can also be used for submitting extrinsics
* Some minting related code, including minting period calculations
* RMB client based on RMB Peer (external processes required)
* Basic wrapper around `tfcmd` for creating deployments

If you're looking for a complete Grid v3 SDK, they are availalbe for [Go](https://github.com/threefoldtech/tfgrid-sdk-go) and [Typescript](https://github.com/threefoldtech/tfgrid-sdk-ts).

## Installation

Tldr: `pip install grid3`

### Requirements

Most of the functionality just requires `python3.8` or newer. 

The Reliable Message Bus functionality is Linux only and also depends on:

* [`redis-server`](https://redis.io/)
* [`rmb-peer`](https://github.com/threefoldtech/rmb-rs)

### Detailed install

Assuming that `python` is `python3` and `pip` is `pip3`, install the latest release from PyPI into a [venv](https://docs.python.org/3/library/venv.html) using `pip`:

```
python -m venv venv
source venv/bin/activate
pip install grid3
```

## Quick tour

With the `graphql` module, you can easily answer questions like, how many nodes are currently in the standby state that were online in the last 24 hours?

```
import time, grid3.network
mainnet = grid3.network.GridNetwork()
sleepers = mainnet.graphql.nodes(['nodeID'], power={'state_eq': 'Down'}, updatedAt_gt=int(time.time()) - 24 * 60 * 60)
print(len(sleepers))
```

We just executed a query against the mainnet GraphQL endpoint `nodes` without even sweating a line break. Pretty cool!
