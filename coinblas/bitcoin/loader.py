import sys
from pathlib import Path
from collections import defaultdict
from pathlib import Path
from time import time
from itertools import groupby
from datetime import date, datetime
from multiprocessing import Pool, Process
import logging

import psycopg2 as pg
from psycopg2.extras import execute_values
from tenacity import retry, stop_after_attempt
from google.cloud import bigquery

from .util import *
from pygraphblas import *

POOL_SIZE = 8

logging.basicConfig(stream=sys.stderr, level=logging.DEBUG)
logger = logging.getLogger(__name__)


def before_sleep_log():
    logger.debug("Retrying")


class Blockchain:
    def __init__(self, dsn=None, verbose=False, quiet=False):
        self.dsn = dsn
        self.verbose = verbose
        self.quiet = quiet

    def insert_block(self, curs, t):
        curs.execute(
            """INSERT INTO bitcoin.block
            (b_number, b_hash, b_timestamp, b_timestamp_month)
            VALUES (%s, %s, %s, %s)
            """,
            (t.b_number, t.b_hash, t.b_timestamp, t.b_timestamp_month),
        )

    def insert_transaction_ids(self, curs, txs):
        execute_values(
            curs,
            """
            INSERT INTO bitcoin.tx
            (t_hash, t_id) VALUES %s
            """,
            txs,
        )

    def insert_addresses_ids(self, curs, oads):
        execute_values(
            curs,
            """
            INSERT INTO bitcoin.address
            (a_address, a_id) VALUES %s
            """,
            oads,
        )

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
        pool = Pool(POOL_SIZE)
        # result = list(map(self.load_graph_month, months))
        result = list(pool.map(self.load_graph_month, months, 1))
        return result

    @retry(stop=stop_after_attempt(3), before_sleep=before_sleep_log)
    def load_graph_month(self, month, path="/coinblas/blocks/bitcoin"):
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
                conn.commit()

            print(f"Took {(time() - tic)/60.0} minutes for {month}")

    def build_block_graph(self, curs, group, bn, path):
        Iv = maximal_matrix(UINT64)
        Ov = maximal_matrix(UINT64)
        inputs = set()
        outputs = set()
        addrs = []
        txs = []
        t_hash = None
        b_id = None
        t_id = None

        tic = time()
        for t in group:

            if b_id is None:
                b_id = t.b_number << 32

            if t_hash != t.t_hash:
                inputs.clear()
                outputs.clear()
                t_id = t.t_id
                txs.append((t.t_hash, t_id))

            t_hash = t.t_hash

            if t.i_index is None:
                Iv[b_id, t_id] = 0
            elif t.i_index not in inputs:
                i_id = t.i_spent_tid + t.i_spent_index
                Iv[i_id, t_id] = t.i_value
                inputs.add(t.i_index)

            if t.o_index not in outputs:
                o_id = t_id + t.o_index
                for o_address in t.o_addresses:
                    addrs.append((o_address, o_id))
                Ov[t_id, o_id] = t.o_value
                outputs.add(t.o_index)

        print(f"Took {time()-tic:.4f} to parse block {bn}.")
        self.insert_block(curs, t)
        self.insert_transaction_ids(curs, txs)
        self.insert_addresses_ids(curs, addrs)

        tic = time()
        b = Path(path) / Path(t.b_hash[-2]) / Path(t.b_hash[-1])
        b.mkdir(parents=True, exist_ok=True)
        print(f"Writing {Iv.nvals} sender vals for {bn}.")
        Ivf = b / Path(f"{bn}_{t.b_hash}_Iv.ssb")
        print(f"Writing {Ov.nvals} receiver vals for {bn}.")
        Ovf = b / Path(f"{bn}_{t.b_hash}_Ov.ssb")
        print(f"Writing {Ov.nvals} receiver vals for {bn}.")
        Avf = b / Path(f"{bn}_{t.b_hash}_Av_PLUS_SECOND.ssb")
        Iv.to_binfile(bytes(Ivf))
        Ov.to_binfile(bytes(Ovf))
        with semiring.PLUS_SECOND:
            Av = Iv @ Ov
        Av.to_binfile(bytes(Avf))
        print(f"matrix block {bn} write took {time()-tic:.4f}")


if __name__ == "__main__":
    b = Blockchain("host=db dbname=coinblas user=postgres password=postgres")
    b.load_graph(sys.argv[1], sys.argv[2])
