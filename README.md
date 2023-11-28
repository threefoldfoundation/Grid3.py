# Grid3.py

This is a collection of Python modules for working with ThreeFold Grid v3. It's designed foremostly for interactive use on the REPL and joyful scripting. We value conciseness and trying to do what you *meant*, even at the expense of a few extra CPU cycles.

If you're looking for a Grid v3 SDK for writing efficient and maintainable code bases, check out [Go](https://github.com/threefoldtech/tfgrid-sdk-go). For code that must execute in the user's browser, see [Typescript](https://github.com/threefoldtech/tfgrid-sdk-ts).

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

With the `graphql` module, you can easily answer questions like, how many nodes are currently in the standby state that were online in the last 36 hours?

```
import time, grid3.network
mainnet = grid3.network.GridNetwork()
sleepers = mainnet.graphql.nodes(['nodeID'], power={'state_eq': 'Down'}, updatedAt_gt=int(time.time()) - 24 * 60 * 60)
print(len(sleepers))
```

We just executed a query against the mainnet GraphQL endpoint `nodes` without even sweating a line break. Pretty cool!
