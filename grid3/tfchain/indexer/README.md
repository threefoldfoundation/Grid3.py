This program began as a standalone complement to the node status bot, to gather data from TF Chain to be used by the bot in determining if nodes have incurred minting violations due to use of the farmerbot. It was originally called the `ingester`.

The result is a SQLite database that contains all uptime events, power state changes, and power target changes for all nodes during the scanned period. By default, all blocks for the current minting period are fetched and processed, along with all new blocks as they are created.

It turns out this data is useful for other things as well, such as simulating the node minting process and producing visualizations of high level trends related to uptime reporting, farmerbot, and minting. The [Peppermint](https://github.com/threefoldfoundation/peppermint) tool also uses the same data.

In industry standard terms, this is really a kind of limited blockchain indexer. Since we also have a demand for indexed data about tfchain transactions and node earnings from utilization, and sufficient information about this is not captured in the [main tfchain indexer](https://github.com/threefoldtech/tfchain_graphql/), a next step for this project would be to expand the collected events to enable similar to a typical blockchain explorer experience.

## Notes

So far not much of an attempt is made to catch all errors or ensure that the program continues running. Best to launch it from a process manager and ensure it's restarted on exit. All data is written in a transactional way, such that the results of processing any block, along with the fact that the block has been processed will all be written or all not be written on a given attempt.

Some apparently unavoidable errors arise from use of the Python Substrate Interface module with threads. This seems to be resolved by switching to mostly using processes instead.

Database locked issues were apparently resolved by switching to using WAL mode.
