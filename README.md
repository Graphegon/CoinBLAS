
![Logo](./docs/Logo.png)

CoinBLAS Community Edition is a Graph Linear Algebra analysis platform
for bitcoin that uses the GraphBLAS graph API via the pygraphblas
Python binding. If you have enough RAM, Google BigQuery budget, cores
and time you can load all of bitcoin history into in-memory graphs and
do full-graph, full-flow analysis using the GraphBLAS API.

![The entire blockchain in RAM](./docs/RAM.png)

Loading the full blockchain graph takes up to 512GB of memory and $500
worth of BigQuery cost, so that's probably out of most people's
budgets.  Thankfully, CoinBLAS can load a month's worth of graph data
at ta time, costing only a few dollars per month.  Current memory
requirements to load all of November 2020 is 16GB of RAM, easily done
on relatively modest laptop hardware.

Once you've loaded the data, CoinBLAS stores the graphs as SuiteSparse
binary files.  At the moment there are 3 files per block, so a full
graph load will save 1.5M files.  The directory layout is partitioned
256 ways on last two hex characters of the block's hash.

![Graph Adjacency Matrix](./docs/Adjacency.png)

The next couple of sections serve as an introduction to Graph
algorithms with Linear Algebra.  The core concept of the GraphBLAS is
that dualism that a graph can construct a matrix, and a matrix can
construct a graph.  This mathematical communion allows the power of
Linear Algebra to be used to analyze and manipulate graphs.

The core operation of graph algorithms is taking a "step" from a node
to its neighbors.  Using Linear Algebra, this translates into common
operation of [Matrix
Multiplication](https://en.wikipedia.org/wiki/Matrix_multiplication).
Repeated multiplications traverse the graph in a [Breadth First
Search](https://en.wikipedia.org/wiki/Breadth-first_search).

![Projecting Adjacency from Incidence Matrices](./docs/Incidence.png)

Adjacency matrices however can only encode simple directed and
undirected graphs between similar kinds of things.  The bitcoin graph
however is a many to many combinations of inputs and outputs to
transactions, the inputs being the outputs of previous transactions.
No worry, Linear Algebra's got you there, the concept of a
[Hypergraph](https://en.wikipedia.org/wiki/Hypergraph) can be
constructed using two [Incidence
Matrices](https://en.wikipedia.org/wiki/Incidence_matrix)

![Projecting Adjacency from Incidence Matrices](./docs/Projection.png)

![Input Output Adjacency projection](./docs/Blocktime.png)

The bitcoin blockchain is an immutable record of past transactions.
This immutability confers onto it a *total order of blocks,
transactions and outputs*.  This order is exploited by CoinBLAS.

As matrices are two dimensional, typically denoted *MxN*, each value
has an row and column index into the matrix within the "keyspace" of
*MxN*.  By convention in GraphBLAS these indexes are called `I` and
`J`.  The `I` index can be thought of as the id of the start of the
edge, and the `J` id of the end.  In SuiteSparse these values are 60
bit unsigned integers.  The maximum index is the extension constant
`GxB_INDEX_MAX` which is 2 to the 60th power (1152921504606846976).

GraphBLAS has a cool trick, because matrices are *sparse* they only
allocate enough memory to store their elements.  The MxN are just
guardrails to keep you from going "out of bounds" on your problem, but
you can makes a matrix that is effectively "unbounded" by setting M
and N to `GxB_INDEX_MAX'.  SuiteSparse won't allocate a zillion
entries, it won't allocate anything in fact until you put stuff in it.

In a sense, this turns a GraphBLAS matrix into an [Associative
Array](https://en.wikipedia.org/wiki/Associative_array) which was by
design, of course.

Now, by encoding the block, transaction index, and output index into
the index, CoinBLAS stores graphs in a linear fashion, new blocks are
always appended onto the "end" of the matrix.  Each block is a 2**32
"space" to fill with transactions and outputs, whose ids are always
between the start of the current block and the start of the next.

![Block Incidence Flow](./docs/TxFlow.png)

![Multi-party Incidence Flow](./docs/AdjacentFlow.png)

![Exposure Reduction](./docs/Reduction.png)

# The Gigabyte Epoch is Over

It's now relatively cheap to get cloud hardware with terabytes of
RAM.  Both Google and AWS provide several different large-memory
virtual machines with up to dozens of TB of RAM with the ability to
connect multiple GPUs as well, offering hundreds of GB of on-GPU RAM
for graph analytical processing using SuiteSparse:GraphBLAS.
