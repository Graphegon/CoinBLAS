# coinblas

# GraphBLAS all the Cryptocurrency

CoinBLAS is a Graph Linear Algebra analysis platform for bitcoin based
on the GraphBLAS graph API and the pygraphblas Python binding.

# Full-flow Analysis

All blockchains form a totally-ordered imutable transaction graph.
Value flows from party to party, block by block, forward in time,
branching and merging from destination addresses via transactions.
coinblas replicates this graph exactly in memory using GraphBLAS
matrices.

![Two incidence matrices encode a bitcoin graph](./docs/Incidence.png)


Transactions can be multi-sender and multi-receiver.

# The Gigabyte Epoch is Over

It's now relatively cheap to get hardware with ~1TB of RAM.  Both
Google and AWS provide several different large-memory virtual machines
with up to dozens of TB of RAM with the ability to connect multiple
GPUs as well, offering hundreds of GB of on-GPU RAM for graph
analytical processing.

This opens the door for real-time, full-flow graph analysis over
cryptocurrency graphs.  Consider that as of late 2020, the entire
bitcoin blockchain is ~400GB, even less when only salient transaction
data is pivoted into GraphBLAS matrices requiring less than 256GB of
RAM to store.

# The entire blockchain in RAM

Coinblas can do full-flow exposure analysis in real-time by storing
the entire blockchain in memory using the GraphBLAS's highly
space-efficient sparse matrix data formats.
