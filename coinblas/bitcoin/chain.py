from pathlib import Path
from multiprocessing.pool import Pool, ThreadPool
from itertools import repeat, groupby
from time import time
from functools import reduce
import logging

import psycopg2 as pg
from psycopg2.extras import execute_values
from tenacity import retry, stop_after_attempt
from google.cloud import bigquery

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
    lazy,
)

from .block import Block
from .tx import Tx
from .relation import Spend
from .address import Address

POOL_SIZE = 8

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


class Chain:
    def __init__(self, dsn, block_path, pool_size=POOL_SIZE, logger=logger):
        self.chain = self
        self.dsn = dsn
        self.block_path = Path(block_path)
        self.pool_size = pool_size
        self.logger = logger

    @lazy
    def blocks(self):
        return {}

    @lazy
    def conn(self):
        return pg.connect(self.dsn)

    @lazy
    def BT(self):
        return self.merge_all_blocks("BT")

    @lazy
    def IT(self):
        return self.merge_all_blocks("IT")

    @lazy
    def TO(self):
        return self.merge_all_blocks("TO")

    @lazy
    def IO(self):
        with semiring.PLUS_MIN:
            return self.IT @ self.TO

    @lazy
    def TT(self):
        with semiring.PLUS_PLUS:
            return self.IT.T @ self.TO.T

    def addr(self, a):
        return Address(self, a)

    @curse
    def tx(self, curs, hash):
        curs.execute("select t_id from bitcoin.tx where t_hash = %s", (hash,))
        t_id = curs.fetchone()[0]
        return Tx(self, t_id)

    def initialize_blocks(self):
        tic = time()
        self.logger.info("Running BigQuery for blocks.")
        client = bigquery.Client()
        query = """
        SELECT number, `hash`, timestamp, timestamp_month 
        FROM `bigquery-public-data.crypto_bitcoin.blocks`
        ORDER BY number;
        """
        bq_blocks = list(client.query(query))
        self.logger.info(f"Initializing {len(bq_blocks)} blocks.")
        with pg.connect(self.dsn) as conn:
            with conn.cursor() as curs:
                execute_values(
                    curs,
                    """
        INSERT INTO bitcoin.base_block 
            (b_number, b_hash, b_timestamp, b_timestamp_month)
        VALUES %s
                    """,
                    bq_blocks,
                )
            conn.commit()

    @curse
    @query
    def load_blocktime(self, curs):
        """
        SELECT b_number, b_hash
        FROM bitcoin.block
        WHERE b_timestamp <@ tstzrange(%s, %s)
        """
        self.blocks = {b[0]: Block(self, b[0], b[1]) for b in curs.fetchall()}

    @curse
    @query
    def load_blockspan(self, curs):
        """
        SELECT b_number, b_hash
        FROM bitcoin.block
        WHERE b_number <@ int4range(%s, %s + 1)
        """
        self.blocks = {b[0]: Block(self, b[0], b[1]) for b in curs.fetchall()}

    def import_blocktime(self, start, end):
        with pg.connect(self.dsn) as conn:
            with conn.cursor() as curs:
                curs.execute(
                    """select i::date
                    from generate_series(%s::date, %s::date, interval '1 month') i
                    """,
                    (start, end),
                )
                months = [x[0] for x in curs.fetchall()]
        if self.pool_size == 1:
            result = list(map(self.import_month, months))
        else:
            pool = Pool(self.pool_size)
            result = list(pool.map(self.import_month, months, 1))

        return result

    @curse
    def create_month(self, curs, month):
        self.logger.debug(f"Creating partitions for {month}")
        curs.execute(f"CALL bitcoin.create_month('{month}')")

    @curse
    def index_and_attach(self, curs, month):
        self.logger.debug(f"Indexing and attaching partitions for {month}")
        curs.execute(f"CALL bitcoin.index_and_attach('{month}')")

    @curse
    @query
    def check_block_import(self, curs):
        """
        SELECT EXISTS (
            SELECT 1 FROM bitcoin.block WHERE b_number = %s
        )
        """
        return curs.fetchone() is not None

    # @retry(stop=stop_after_attempt(3))
    def import_month(self, month):
        tic = time()
        client = bigquery.Client()

        self.create_month(month)

        self.logger.info(f"Loading {month}")
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
        for bn, group in groupby(client.query(query), lambda r: r["b_number"]):
            if self.check_block_import(bn):
                self.logger.debug(f"Block {bn} already done.")
                continue
            self.build_block_graph(group, bn, month)

        self.index_and_attach(month)

        self.logger.info(f"Took {(time() - tic)/60.0} minutes for {month}")

    def build_block_graph(self, group, bn, month):

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
                block.add_tx(tx)

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

        block.finalize(month)
        self.conn.commit()
        self.logger.debug(
            f"Block {block.number}: wrote "
            f"{block.tx_vector.size} transactions in {time()-tic:.4f}")

    def merge_all_blocks(self, suffix):
        if self.pool_size == 1:
            mapper = lambda f, s: list(map(f, s))
        else:
            thread_pool = ThreadPool(self.pool_size)
            mapper = lambda f, s: thread_pool.map(f, s, 1)

        blocks = list(
            grouper(
                zip(((b.hash, b.number) for b in self.blocks.values()), repeat(suffix)),
                2,
                None,
            )
        )
        self.logger.debug(f"Merging {len(blocks)} {suffix} blocks.")
        while len(blocks) > 1:
            blocks = list(grouper(mapper(self.merge_block_pair, blocks), 2, None))
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
        return iter(self.blocks.values())

    def summary(self):
        min_tx = Tx(
            self,
            id=self.IT.T.reduce_vector()
            .apply(unaryop.POSITIONI_INT64)
            .reduce_int(monoid.MIN_MONOID),
        )
        max_tx = Tx(
            self,
            id=self.TO.reduce_vector()
            .apply(unaryop.POSITIONI_INT64)
            .reduce_int(monoid.MAX_MONOID),
        )

        min_time = min_tx.block.timestamp.ctime()
        max_time = max_tx.block.timestamp.ctime()

        in_val = self.IT.reduce_int()
        out_val = self.TO.reduce_int()

        print(
            f"""\
Blocks to Txs: {self.BT.nvals} values
Inputs to Tx: {self.IT.nvals} values.
Tx to Outputs: {self.TO.nvals} values.
Inputs to Outputs: {self.IO.nvals} values.
Tx to Tx: {self.TT.nvals} values.
Blocks span {min_tx.block_number} to {max_tx.block_number}
Earliest Transaction: {min_tx.hash}
Latest Transaction: {max_tx.hash}
Blocks time span {min_time} to {max_time}
Total value input {btc(in_val)} output {btc(out_val)}
"""
        )

    def __repr__(self):
        min_block = reduce(min, self.blocks.keys())
        max_block = reduce(max, self.blocks.keys())
        return f"<Bitcoin chain of {len(self.blocks)} blocks between {min_block} and {max_block}>"
