from pathlib import Path
from multiprocessing.pool import Pool, ThreadPool
from itertools import repeat, groupby
from time import time

import psycopg2 as pg
from psycopg2.extras import execute_values
from tenacity import retry, stop_after_attempt
from google.cloud import bigquery
from lazy_property import LazyWritableProperty as lazy

from pygraphblas import (
    Matrix,
    monoid,
    semiring,
    unaryop,
    UINT64,
)

from coinblas.util import (
    btc,
    curse,
    grouper,
    query,
    maximal_matrix,
)

from .block import Block
from .tx import Tx
from .spend import Spend
from .address import Address

POOL_SIZE = 8


class Bitcoin:
    def __init__(self, dsn, block_path, start, stop, POOL_SIZE=POOL_SIZE):
        self.chain = self
        self.dsn = dsn
        self.block_path = Path(block_path)
        self.POOL_SIZE = POOL_SIZE
        self.blocks = self.date_block_range(start, stop)

    @lazy
    def conn(self):
        return pg.connect(self.dsn)

    @lazy
    def BT(self):
        return self.merge_all_blocks(self.blocks.copy(), "BT")

    @lazy
    def IT(self):
        return self.merge_all_blocks(self.blocks.copy(), "IT")

    @lazy
    def TO(self):
        return self.merge_all_blocks(self.blocks.copy(), "TO")

    @lazy
    def IO(self):
        with semiring.PLUS_MIN:
            return self.IT @ self.TO

    @lazy
    def TT(self):
        with semiring.PLUS_PLUS:
            return self.IT.T @ self.TO.T

    def load_graph(self, start=None, end=None):
        with pg.connect(self.dsn) as conn:
            with conn.cursor() as curs:
                curs.execute(
                    """select i::date
                    from generate_series(%s::date, %s::date, interval '1 month') i
                    """,
                    (start, end),
                )
                months = [x[0] for x in curs.fetchall()]
        if self.POOL_SIZE == 1:
            result = list(map(self.load_graph_month, months))
        else:
            pool = Pool(self.POOL_SIZE)
            result = list(pool.map(self.load_graph_month, months, 1))

        return result

    # @retry(stop=stop_after_attempt(3))
    def load_graph_month(self, month, path="database-blocks"):
        tic = time()
        client = bigquery.Client()

        print(f"Loading {month}")
        query = f"""
        WITH TIDS AS (
            SELECT 
                block_number, 
                block_hash, 
                block_timestamp, 
                block_timestamp_month, `hash`, inputs, outputs,
                (block_number << 32) + (ROW_NUMBER() 
                    OVER(PARTITION BY block_number ORDER BY block_number, `hash`) << 16) AS t_id
        FROM `bigquery-public-data.crypto_bitcoin.transactions` 
        ORDER BY block_number, `hash`)

        SELECT
            t.t_id as t_id,
            t.block_number as b_number,
            t.block_hash as b_hash,
            t.block_timestamp as b_timestamp,
            t.block_timestamp_month as b_timestamp_month,
            t.`hash` as t_hash,
            spents.t_id as i_spent_tid,
            i.spent_output_index as i_spent_index,
            i.value as i_value,
            i.index as i_index,
            o.index as o_index,
            o.addresses as o_addresses,
            o.value as o_value
        FROM tids t LEFT JOIN UNNEST(inputs) as i,
        UNNEST(outputs) as o
        LEFT JOIN tids spents on (i.spent_transaction_hash = spents.`hash`)
        WHERE t.block_timestamp_month = '{month}'
        ORDER BY t.block_number, t.`hash`, i.index, o.index
        """
        with pg.connect(self.dsn) as conn:
            for bn, group in groupby(client.query(query), lambda r: r["b_number"]):
                with conn.cursor() as curs:
                    curs.execute(
                        "select exists (select 1 from bitcoin.block where b_number = %s)",
                        (bn,),
                    )
                    if curs.fetchone()[0]:
                        print(f"Block {bn} already done.")
                        continue
                    self.build_block_graph(curs, group, bn, path)
            print(f"Took {(time() - tic)/60.0} minutes for {month}")

    def build_block_graph(self, curs, group, bn, path):

        inputs = set()
        outputs = set()
        t_hash = None
        block = None
        tic = time()

        for t in group:

            if block is None:
                block = Block(self, t.b_number, t.b_hash)

            if t_hash != t.t_hash:
                inputs.clear()
                outputs.clear()
                tx = Tx(self, t.t_id, t.t_hash, block)
                block.add(tx)

            t_hash = t.t_hash

            if t.i_index is None:
                tx.add_input(Spend(self, block.id, 0))

            elif t.i_index not in inputs:
                spent_tx_id = t.i_spent_tid
                i_id = spent_tx_id + t.i_spent_index
                tx.add_input(Spend(self, i_id, t.i_value))
                inputs.add(t.i_index)

            if t.o_index not in outputs:
                o_id = tx.id + t.o_index
                for o_address in t.o_addresses:
                    block.add_address(o_address, o_id)
                tx.add_output(Spend(self, o_id, t.o_value))
                outputs.add(t.o_index)

        print(f"Took {time()-tic:.4f} to parse block {block.number}.")
        tic = time()
        block.insert(t.b_hash, t.b_timestamp, t.b_timestamp_month)
        block.write_block_files(path)
        self.conn.commit()
        print(f"matrix block {block.number} write took {time()-tic:.4f}")

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

    def merge_all_blocks(self, blocks, suffix):
        thread_pool = ThreadPool(POOL_SIZE)
        blocks = list(grouper(zip(blocks, repeat(suffix)), 2, None))
        while len(blocks) > 1:
            blocks = list(
                grouper(thread_pool.map(self.merge_block_pair, blocks), 2, None)
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

    def __iter__(self):
        b_ids = self.BT.pattern().reduce_vector(monoid.LAND_BOOL_monoid)
        for b_id, _ in b_ids:
            yield Block.from_id(self, b_id)

    def summary(self):
        txs = self.TT.reduce_vector().apply(unaryop.POSITIONI_INT64)
        min_tx = Tx(self, txs.reduce_int(monoid.MIN_MONOID))
        max_tx = Tx(self, txs.reduce_int(monoid.MAX_MONOID))

        min_time = min_tx.block.timestamp.ctime()
        max_time = max_tx.block.timestamp.ctime()

        in_val = self.IT.reduce_int()
        out_val = self.TO.reduce_int()

        print(f"Blocks to Txs: {self.BT.nvals} values.")
        print(f"Inputs to Tx: {self.IT.nvals} values.")
        print(f"Tx to Outputs: {self.TO.nvals} values.")
        print(f"Inputs to Outputs: {self.IO.nvals} values.")
        print(f"Tx to Tx: {self.TT.nvals} values.")

        print(f"Blocks span {min_tx.block_number} to {max_tx.block_number}")
        print(f"Earliest Transaction: {min_tx.hash}")
        print(f"Latest Transaction: {max_tx.hash}")
        print(f"Blocks time span {min_time} to {max_time}")
        print(f"Total value input {btc(in_val)} output {btc(out_val)}")
