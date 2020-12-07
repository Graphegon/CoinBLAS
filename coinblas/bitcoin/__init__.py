import sys
from time import time
from pathlib import Path
from multiprocessing.pool import ThreadPool
from itertools import zip_longest, repeat

import psycopg2 as pg
from pygraphblas import Matrix, Vector, UINT64, semiring, unaryop, binaryop, monoid

from coinblas.util import prepared, curse, maximal_matrix, maximal_vector, grouper, btc

from .block import Block
from .tx import Tx
from .spend import Spend
from .address import Address

POOL_SIZE = 8


class Bitcoin:
    """Hodls the blockchain graphs for bitcoin analysis.

    This class contains the query state for bitcoin graph analysis
    with the GraphBLAS.

      self.Iv:  sender_id by transaction id incidence matrix
      self.Ov:  transaction_id by receiver_id incidence matrix
      self.Av:  Sender/Receiver Adjacency matrix projected with PLUS_SECOND
      self.Tv   Transaction/Transaction Adjacency projected with PLUS_FIRST
    """

    def __init__(self, dsn, block_path, start, stop):
        self.dsn = dsn
        self.conn = pg.connect(dsn)
        self.chain = self
        self.block_path = Path(block_path)
        self.pool = ThreadPool(POOL_SIZE)
        self.Iv = maximal_matrix(UINT64)
        self.Ov = maximal_matrix(UINT64)
        self.Av = maximal_matrix(UINT64)
        self.load_blocks(start, stop)

    @property
    def cursor(self):
        return self.conn.cursor()

    def get_block_number(self, id):
        return id >> 32

    def get_block_id(self, id):
        return self.get_block_number(id) << 32

    def get_tx_id(self, id):
        return (id >> 16) << 16

    @curse
    def load_blocks(self, curs, start, stop):
        curs.execute(
            "select b_hash, b_number from bitcoin.block where b_timestamp_month between %s and %s",
            (
                start,
                stop,
            ),
        )
        blocks = curs.fetchall()
        self.Iv = self.merge_all_blocks(blocks, "Iv")
        self.Ov = self.merge_all_blocks(blocks.copy(), "Ov")
        self.Av = self.merge_all_blocks(blocks.copy(), "Av_PLUS_SECOND")
        with semiring.PLUS_FIRST:
            self.Tv = self.Iv.T @ self.Ov.T

    def merge_all_blocks(self, blocks, suffix):
        blocks = list(grouper(zip(blocks, repeat(suffix)), 2, None))
        while len(blocks) > 1:
            blocks = list(
                grouper(self.pool.map(self.merge_block_pair, blocks), 2, None)
            )
        return self.merge_block_pair(blocks[0])

    def merge_block_pair(self, pair):
        left, right = pair
        if left and not isinstance(left, Matrix):
            (bhash, bn), suffix = left
            sf = self.block_path / bhash[-2] / bhash[-1] / f"{bn}_{bhash}_{suffix}.ssb"
            if not sf.exists():
                return right
            left = Matrix.from_binfile(bytes(sf))
        if right and not isinstance(right, Matrix):
            (bhash, bn), suffix = right
            rf = self.block_path / bhash[-2] / bhash[-1] / f"{bn}_{bhash}_{suffix}.ssb"
            if not rf.exists():
                return left
            right = Matrix.from_binfile(bytes(rf))
        if left is None:
            return right
        if right is None:
            return left
        return left + right

    @curse
    @prepared
    def tid_transaction(self, curs):
        """\
        Lookup transction has for tid.

            SELECT t_hash FROM bitcoin.tx WHERE t_id = $1
        """
        return curs.fetchone()[0]

    @prepared
    def transaction_tid(self, curs):
        """\
        Lookup transction has for tid.

            SELECT t_id FROM bitcoin.tx WHERE t_hash = $1
        """
        return curs.fetchone()[0]

    @curse
    @prepared
    def address_vector(self, curs, assign=0):
        """\
        Return a vector of ids for an address with values assigned, the
        default is 0.

            SELECT a_id FROM bitcoin.address WHERE a_address = $1
        """
        v = maximal_vector(UINT64)
        for (a_id,) in curs.fetchall():
            v[a_id] = assign
        return v

    @curse
    @prepared
    def vector_address(self, curs):
        """\
            select a_id, a_address from bitcoin.address where a_id = ANY($1)
        """
        return curs.fetchall()

    def block(self, number):
        return Block(self, number)

    def transaction(self, hash, id=None):
        return Tx(self, hash, id=id)

    def address(self, address):
        return Address(self, address)

    def summary(self):
        txs = self.Tv.reduce_vector().apply(unaryop.POSITIONI_INT64)
        min_tx = Tx(self, id=txs.reduce_int(monoid.MIN_MONOID))
        max_tx = Tx(self, id=txs.reduce_int(monoid.MAX_MONOID))

        min_time = min_tx.block.timestamp().ctime()
        max_time = max_tx.block.timestamp().ctime()

        in_val = self.Iv.reduce_int()
        out_val = self.Ov.reduce_int()

        print(f"Sender matrix has {self.Iv.nvals} edges.")
        print(f"Receiver matrix has {self.Iv.nvals} edges.")
        print(f"Adjacency matrix has {self.Av.nvals} edges.")
        print(f"Blocks span {min_tx.block_number} to {max_tx.block_number}")
        print(f"Earliest Transaction: {min_tx.hash}")
        print(f"Latest Transaction: {max_tx.hash}")
        print(f"Blocks time span {min_time} to {max_time}")
        print(f"Total value intput {btc(in_val)} output {btc(out_val)}")
