import sys
from time import time
from pathlib import Path
from multiprocessing.pool import ThreadPool
from itertools import zip_longest, repeat

import psycopg2 as pg
from pygraphblas import (
    Matrix,
    UINT64,
    BOOL,
    Vector,
    binaryop,
    monoid,
    semiring,
    unaryop,
    )

from coinblas.util import (
    btc,
    curse,
    grouper,
    maximal_matrix,
    maximal_vector,
    query,
    get_block_number,
    lazy_property,
    )

from .block import Block
from .tx import Tx
from .spend import Spend
from .address import Address

POOL_SIZE = 8


class Bitcoin:
    """Hodls the blockchain graphs for bitcoin analysis.
    """

    def __init__(self, dsn):
        self.dsn = dsn
        self.conn = pg.connect(dsn)
        self.chain = self
        self.pool = ThreadPool(POOL_SIZE)
        
    @lazy_property
    def BT(self):
        return self.merge_all_blocks(self.blocks.copy(), "BT")
    
    @lazy_property
    def TB(self):
        return self.merge_all_blocks(self.blocks.copy(), "TB")
    
    @lazy_property
    def IT(self):
        return self.merge_all_blocks(self.blocks.copy(), "IT")
    
    @lazy_property
    def TO(self):
        return self.merge_all_blocks(self.blocks.copy(), "TO")
        
    @lazy_property
    def IO(self):
        with semiring.PLUS_MIN:
            return self.IT @ self.TO

    @lazy_property
    def TT(self):
        with semiring.PLUS_PLUS:
            return self.IT.T @ self.TO.T

    @curse
    @query
    def date_block_range(self, curs):
        """
            SELECT b_hash, b_number FROM bitcoin.block WHERE b_timestamp <@ tstzrange(%s, %s)
        """
        return curs.fetchall()

    @curse
    @query
    def block_range(self, curs):
        """
            SELECT b_hash, b_number FROM bitcoin.block WHERE b_number <@ int4range(%s, %s)
        """
        return curs.fetchall()

    @curse
    def load_blocks(self, curs, block_path, blocks):
        self.block_path = Path(block_path)
        self.blocks = blocks

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

    def block(self, number):
        return Block(self, number)

    def tx(self, hash=None, id=None):
        return Tx(self, hash=hash, id=id)

    def address(self, address):
        return Address(self, address)

    def __iter__(self):
        b_ids = self.BT.pattern().reduce_vector(monoid.LAND_BOOL_monoid)
        for b_id, _ in b_ids:
            yield Block.from_id(self, b_id)

    def summary(self):
        txs = self.TT.reduce_vector().apply(unaryop.POSITIONI_INT64)
        min_tx = Tx(self, id=txs.reduce_int(monoid.MIN_MONOID))
        max_tx = Tx(self, id=txs.reduce_int(monoid.MAX_MONOID))

        min_time = min_tx.block.timestamp.ctime()
        max_time = max_tx.block.timestamp.ctime()

        in_val = self.IT.reduce_int()
        out_val = self.TO.reduce_int()

        print(f"Txs to Blocks: {self.TB.nvals} edges.")
        print(f"Blocks to Txs: {self.BT.nvals} edges.")
        print(f"Inputs to Tx: {self.IT.nvals} edges.")
        print(f"Tx to Outputs: {self.TO.nvals} edges.")
        print(f"Inputs to Outputs: {self.IO.nvals} edges.")
        print(f"Tx to Tx: {self.TT.nvals} edges.")
        
        print(f"Blocks span {min_tx.block_number} to {max_tx.block_number}")
        print(f"Earliest Transaction: {min_tx.hash}")
        print(f"Latest Transaction: {max_tx.hash}")
        print(f"Blocks time span {min_time} to {max_time}")
        print(f"Total value intput {btc(in_val)} output {btc(out_val)}")

