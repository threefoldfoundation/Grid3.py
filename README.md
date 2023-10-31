# Grid3.py

This is a collection of Python modules for working with ThreeFold Grid v3. It's designed foremostly for interactive use on the REPL and joyful scripting. We value conciseness and trying to do what you *meant* even at the expense of a few extra CPU cycles.

If you're looking for a Grid v3 SDK for writing efficient and maintainable code bases, check out Go or Rust. For code that must execute in the user's browser, see Typescript.

## Installation

You can install the latest release from PyPI using `pip` (adjust the virtual environment bits to your preferences and shell as needed):

```
python -m venv venv
source venv/bin/activate
pip install grid3
```

## Quick tour

With grid3.py, you can easily answer questions like, how many nodes are currently in the standby state that were online in the last 36 hours?

```
import time, grid3.network
mainnet = grid3.network.GridNetwork()
sleepers = mainnet.graphql.nodes(['nodeID'], power={'state': 'Down'}, updatedAt_gt=int(time.time()) - 24 * 60 * 60)
len(sleepers)
```

We just executed a query against the mainnet GraphQL endpoint `nodes` without even sweating a line break. Pretty cool!
