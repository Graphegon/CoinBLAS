from pathlib import Path
from multiprocessing.pool import Pool, ThreadPool
from itertools import repeat, groupby
from time import time
from functools import reduce
from operator import itemgetter, add
from collections import OrderedDict
import logging

import psycopg2 as pg
from psycopg2.extras import execute_values
from tenacity import retry, stop_after_attempt
from google.cloud import bigquery

from pygraphblas import (
    Matrix,
    monoid,
    semiring,
    binaryop,
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
    Object,
)

from .block import Block
from .tx import Tx
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
        return OrderedDict()

    def merge_block_graphs(self, suffix, op=binaryop.SECOND):
        self.logger.debug(f"Merging {len(self.blocks)} {suffix} blocks.")
        blocks = list(grouper(zip(self.blocks.values(), repeat(suffix)), 2, None))
        with op:
            while len(blocks) > 1:
                blocks = list(
                    grouper(self.mapper(self.merge_block_pairs, blocks), 2, None)
                )
            return self.merge_block_pairs(blocks[0])

    @lazy
    def mapper(self):
        if self.pool_size == 1:
            return lambda f, s: list(map(f, s))
        else:
            thread_pool = ThreadPool(self.pool_size)
            return lambda f, s: thread_pool.map(f, s, 1)

    def merge_block_pairs(self, pair):
        left, right = pair
        if isinstance(left, tuple):
            lblock, ls = left
            left = getattr(lblock, ls)
        if isinstance(right, tuple):
            rblock, rs = right
            right = getattr(rblock, rs)
        if left is None:
            return right
        if right is None:
            return left
        return left.eadd(right, binaryop.SECOND)

    @lazy
    def conn(self):
        return pg.connect(self.dsn)

    @lazy
    def BT(self):
        return self.merge_block_graphs("BT")

    @lazy
    def IT(self):
        return self.merge_block_graphs("IT")

    @lazy
    def TO(self):
        return self.merge_block_graphs("TO")

    @lazy
    def SI(self):
        return self.merge_block_graphs("SI")

    @lazy
    def OR(self):
        return self.merge_block_graphs("OR")

    @lazy
    def ST(self):
        return self.merge_block_graphs("ST")

    @lazy
    def TR(self):
        return self.merge_block_graphs("TR")

    @lazy
    def IO(self):
        with semiring.PLUS_MIN:
            return self.IT @ self.TO

    @lazy
    def SR(self):
        with semiring.PLUS_MIN:
            return self.ST @ self.TR

    @lazy
    def TT(self):
        with semiring.PLUS_PLUS:
            return self.IT.T @ self.TO.T

    def clear(self):
        self.blocks.clear()
        self.BT = maximal_matrix(UINT64)
        self.IT = maximal_matrix(UINT64)
        self.TO = maximal_matrix(UINT64)
        self.SO = maximal_matrix(UINT64)
        self.OR = maximal_matrix(UINT64)
        self.ST = maximal_matrix(UINT64)
        self.TR = maximal_matrix(UINT64)

    @curse
    def address(self, curs, a):
        curs.execute(
            "SELECT a_id, a_address " "FROM bitcoin.address where a_address = %s", (a,)
        )
        a_id, a_address = curs.fetchone()
        return Address(self, a_id, a_address)

    @curse
    def tx(self, curs, hash):
        curs.execute(
            "SELECT t_id, t_hash FROM " "bitcoin.tx where t_hash = %s", (hash,)
        )
        t_id, t_hash = curs.fetchone()
        return Tx(self, t_id, t_hash)

    @curse
    def initialize_blocks(self, curs):
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
        curs.executemany(
            """
            INSERT INTO bitcoin.base_block
            (b_number, b_hash, b_timestamp, b_timestamp_month)
            VALUES (%(number)s, %(hash)s, %(timestamp)s, %(timestamp_month)s)
            """,
            bq_blocks,
        )
        self.conn.commit()

    @curse
    @query
    def load_blocktime(self, curs):
        """
        SELECT b_number, b_hash
        FROM bitcoin.block
        WHERE b_timestamp <@ tstzrange(%s, %s)
        ORDER BY b_number
        """
        self.blocks.clear()
        for b in curs.fetchall():
            self.blocks[b[0]] = Block(self, b[0], b[1])

    @curse
    @query
    def load_blockspan(self, curs):
        """
        SELECT b_number, b_hash
        FROM bitcoin.block
        WHERE b_number <@ int4range(%s, %s + 1)
        ORDER BY b_number
        """
        self.blocks.clear()
        for b in curs.fetchall():
            self.blocks[b[0]] = Block(self, b[0], b[1])

    @curse
    @query
    def load_blockmonth(self, curs):
        """
        SELECT b_number, b_hash
        FROM bitcoin.block
        WHERE b_timestamp_month = %s
        ORDER BY b_number
        """
        self.blocks.clear()
        for b in curs.fetchall():
            self.blocks[b[0]] = Block(self, b[0], b[1])

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
    def check_block_imported(self, curs):
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
            i.addresses as i_addresses,
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
        for bn, group in groupby(client.query(query), itemgetter("b_number")):
            if self.check_block_imported(bn):
                self.logger.debug(f"Block {bn} already done.")
                continue
            self.build_block_graph(group, bn, month)

        self.index_and_attach(month)
        self.conn.commit()
        self.logger.info(f"Took {(time() - tic)/60.0} minutes for {month}")

    def build_block_graph(self, group, bn, month):

        t_id = None
        block = None
        tic = time()

        for t in map(Object, group):

            if block is None:
                block = Block(self, t.b_number, t.b_hash)

            if t_id != t.t_id:
                tx = Tx(self, t.t_id, t.t_hash, block)
                block.pending_txs[t.t_id] = tx

            t_id = t.t_id

            spent_tx_id = t.i_spent_tid
            if spent_tx_id is None:
                tx.pending_inputs[block.id] = 0

            elif spent_tx_id + t.i_spent_index not in tx.pending_inputs:
                i_id = spent_tx_id + t.i_spent_index
                i_value = t.i_value
                tx.pending_inputs[i_id] = i_value
                for i_address in t.i_addresses:
                    tx.pending_input_addresses[i_address].append((t_id, i_id, i_value))

            o_id = tx.id + t.o_index
            if o_id not in tx.pending_outputs:
                o_value = t.o_value
                tx.pending_outputs[o_id] = o_value
                for o_address in t.o_addresses:
                    tx.pending_output_addresses[o_address].append((t_id, o_id, o_value))

        block.finalize(month)

        self.conn.commit()
        self.logger.debug(f"Wrote block {block.number} in {time()-tic:.4f}")

    def __iter__(self):
        return iter(self.blocks.values())

    @property
    def summary(self):
        min_tx = Tx(self, id=self.min_tx_id)
        max_tx = Tx(self, id=self.max_tx_id)

        min_time = min_tx.block.timestamp.ctime()
        max_time = max_tx.block.timestamp.ctime()

        in_val = self.IT.reduce_int()
        out_val = self.TO.reduce_int()

        return f"""
Blocks:
    - min: {min_tx.block_number}
    - max: {max_tx.block_number}

Transactions:
    - earliest: {min_tx.hash}
        - time: {min_time}

    - latest: {max_tx.hash}
        - time: {max_time}

Total value:
    - in: {btc(in_val):>12} btc.
    - out: {btc(out_val):>12} btc.

Incidence Matrices:
    - BT: {self.BT.nvals:>12} Blocks to Txs.
    - IT: {self.IT.nvals:>12} Inputs to Tx.
    - TO: {self.TO.nvals:>12} Tx to Outputs.
    - SI: {self.SI.nvals:>12} Senders to Inputs.
    - OR: {self.OR.nvals:>12} Outputs to Receivers.
    - ST: {self.ST.nvals:>12} Senders to Transactions.
    - TR: {self.TR.nvals:>12} Transactions to Receivers.

Adjacencies:
    - IO: {self.IO.nvals:>12} edges Inputs to Outputs.
    - SR: {self.SR.nvals:>12} Senders to Receivers.
    - TT: {self.TT.nvals:>12} Tx to Tx.
"""

    @property
    def tx_I(self):
        return self.TO.reduce_vector().apply(unaryop.POSITIONI_INT64)

    @property
    def max_tx_id(self):
        return self.tx_I.reduce_int(monoid.MAX_MONOID)

    @property
    def min_tx_id(self):
        return self.tx_I.reduce_int(monoid.MIN_MONOID)

    def __repr__(self):
        min_block = reduce(min, self.blocks.keys())
        max_block = reduce(max, self.blocks.keys())
        return (
            f"<Bitcoin chain of {len(self.blocks)} blocks "
            f"between {min_block} and {max_block}>"
        )
