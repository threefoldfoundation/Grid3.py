These are some test scripts and notes comparing different ways to compress raw tfchain block data in json format.

The main purpose is to create an efficient archive of the full chain that can be indexed on demand. It also gives the ability to retrieve blocks via random access, though this is a secondary use case.

With those requirements in mind, an optimal choice is zstd compression with a 1KB dictionary and 10 block batches. Moving to 100 block batches has diminishing returns and would negatively impact random access performance.
