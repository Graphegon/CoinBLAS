from pathlib import Path
from pygraphblas import *
from collections import defaultdict
from pathlib import Path
import time
from itertools import groupby
from datetime import date, datetime
from multiprocessing import Pool, Process
import psycopg2 as pg
from psycopg2.extras import execute_values

from google.cloud import bigquery

from .util import *

GxB_INDEX_MAX = 1 << 60

POOL_SIZE=24

def maximal_matrix(T):
    return Matrix.sparse(T, GxB_INDEX_MAX, GxB_INDEX_MAX)


class BitcoinLoader:

    def __init__(self, dsn=None, verbose=False, quiet=False):
        self.dsn = dsn
        self.verbose = verbose
        self.quiet = quiet

    def get_block_id(self, number):
        return number >> 32

    def get_tx_id(self, bn, index):
        assert bn < (1<<32)
        assert index < (1<<16)
        return (bn << 32) + (index << 16)

    def get_address_id(self, tx_id, sender_index):
        assert sender_index < (1<<16)
        return tx_id + sender_index

    def query_tx_id(self, curs, t_hash):
        curs.execute(
            'select t_id from bitcoin.tx_id where t_hash = %s',
            (t_hash,))
        return curs.fetchone()[0]

    def insert_addresses_ids(self, curs, oads):
        tic = time.time()
        execute_values(curs,
            """
            INSERT INTO bitcoin.address 
            (a_address, a_id) VALUES %s
            """, oads)
        print(f'Inserting {len(oads)} addresses in {time.time()-tic}.')
    
    def load(self, start=None, end=None):
        tic = time.time()
        client = bigquery.Client()
        total_span = [x['timestamp_month'] for x in client.query(
            """SELECT 
            timestamp_month
            FROM `bigquery-public-data.crypto_bitcoin.blocks`
            GROUP BY timestamp_month ORDER BY timestamp_month ;
            """)]
        
        if start is None:
            start = total_span[0]
        elif isinstance(start, str):
            start = date.fromisoformat(start)
        if end is None:
            end = total_span[1]
        elif isinstance(end, str):
            end = date.fromisoformat(end)

        months = total_span[total_span.index(start):total_span.index(end)+1]
        pool = Pool(POOL_SIZE)
        #result = list(map(self.load_month, months))
        result = pool.map(self.load_month, months, 1)
        print(f'Took {(time.time() - tic)/60.0} minutes')
        return result

    def load_month(self, month):
        tic = time.time()
        client = bigquery.Client()
        with pg.connect(self.dsn) as conn:
            print(f'Loading {month}')
            query = f"""SELECT
            block_number as b_number,
            block_hash as b_hash,
            block_timestamp as b_timestamp,
            block_timestamp_month as b_timestamp_month,
            `hash` as t_hash,
            FROM `bigquery-public-data.crypto_bitcoin.transactions`
            WHERE block_timestamp_month = '{month}'
            ORDER BY block_number, `hash`
            """
            for bn, group in groupby(client.query(query), lambda r: r['b_number']):
                with conn.cursor() as curs:
                    self.insert_block_transactions(curs, bn, group)

            print(f'Took {(time.time() - tic)/60.0} minutes for {month}')

    def insert_block_transactions(self, curs, block_number, group):
        txns = []
        for i, t in enumerate(group):
            assert t['b_number'] == block_number
            if i == 0:
                curs.execute(
                    """INSERT INTO bitcoin.block 
                    (b_number, b_hash, b_timestamp, b_timestamp_month) 
                    VALUES (%s, %s, %s, %s)
                    """,
                    (t.b_number, t.b_hash, t.b_timestamp, t.b_timestamp_month))
            txns.append((self.get_tx_id(t.b_number, i), t.t_hash, t.b_timestamp_month))
        execute_values(curs,
            'INSERT INTO bitcoin.tx (t_id, t_hash, b_timestamp_month) VALUES %s', txns)

    def load_graph(self, start=None, end=None):
        with pg.connect(self.dsn) as conn:
            with conn.cursor() as curs:
                curs.execute(
                    """select 
                    b_timestamp_month
                    from bitcoin.block
                    group by b_timestamp_month order by b_timestamp_month
                    """)
                total_span = [x[0] for x in curs.fetchall()]

        if start is None:
            start = total_span[0]
        elif isinstance(start, str):
            start = date.fromisoformat(start)
        if end is None:
            end = total_span[1]
        elif isinstance(end, str):
            end = date.fromisoformat(end)

        months = total_span[total_span.index(start):total_span.index(end)+1]
        pool = Pool(POOL_SIZE)
        #result = list(map(self.load_graph_month, months))
        result = list(pool.map(self.load_graph_month, months, 1))
        return result

    def load_graph_month(self, month, path='/coinblas/blocks/bitcoin'):
        tic = time.time()
        client = bigquery.Client()
        with pg.connect(self.dsn) as conn:
                with conn.cursor() as curs:
                    curs.execute("""
                    select max(b_number) from bitcoin.block where b_number not in (select b_number from bitcoin.graph_log );
                    """)
                    start_block = curs.fetchone()[0]

        print(f'Loading {month}')
        query = f"""SELECT
        block_number as b_number,
        block_hash as b_hash,
        block_timestamp_month as b_timestamp_month,
        `hash` as t_hash,
        i.spent_transaction_hash as i_spent_hash,
        i.spent_output_index as i_spent_index,
        i.index as i_index,
        i.value as i_value,
        o.index as o_index,
        o.addresses as o_addresses,
        o.value as o_value
        FROM `bigquery-public-data.crypto_bitcoin.transactions`,
        UNNEST(inputs) as i,
        UNNEST(outputs) as o
        WHERE block_timestamp_month = '{month}'
        AND block_number < {start_block}
        ORDER BY block_number, `hash`, i.index, o.index
        """
        with pg.connect(self.dsn) as conn:
            for bn, group in groupby(client.query(query), lambda r: r['b_number']):
                with conn.cursor() as curs:
                    curs.execute('select exists (select 1 from bitcoin.graph_log where b_number = %s)', (bn,))
                    if curs.fetchone()[0]:
                        print(f'Block {bn} already done.')
                        continue
                    self.build_block_graph(curs, group, bn, path)
                conn.commit()

            print(f'Took {(time.time() - tic)/60.0} minutes for {month}')

    def build_block_graph(self, curs, group, bn, path):
        Sv = maximal_matrix(UINT64)
        Rv = maximal_matrix(UINT64)
        inputs = set()
        outputs = set()
        oads = []
        t_hash = None
        t_id = None
        t_index = 0
        curs.execute('select b_hash from bitcoin.block where b_number = %s', (bn,))
        bhash = curs.fetchone()[0]

        group = list(group)
        spent_hashes = list({t.i_spent_hash for t in group})
        tic = time.time()
        curs.execute('select t_id from bitcoin.tx_id where t_hash = ANY (%s)', (spent_hashes,))
        print(f'block {bhash} looking up {len(spent_hashes)} txids took {time.time() - tic}')
        ids = [r[0] for r in curs.fetchall()]
        assert len(spent_hashes) == len(ids)
        spent_hash_ids = {h: i for h, i in zip(spent_hashes, ids)}
        
        for t in group:
            if t_hash != t.t_hash:
                inputs.clear()
                outputs.clear()
                t_id = self.get_tx_id(bn, t_index)
                t_index += 1

            t_hash = t.t_hash

            if t.i_index not in inputs:
                spent_tid = spent_hash_ids[t.i_spent_hash]
                iid = self.get_address_id(spent_tid, t.i_spent_index)
                Sv[iid, t_id] = t.i_value
                inputs.add(t.i_index)
                if self.verbose:
                    print(f'Added sender {t.i_spent_index} from {t.i_spent_hash}')

            if t.o_index not in outputs:
                oid = self.get_address_id(t_id, t.o_index)
                for oa in t.o_addresses:
                    oads.append((oa, oid))
                Rv[t_id, oid] = t.o_value
                outputs.add(t.o_index)

        self.insert_addresses_ids(curs, oads)
        curs.execute("""insert into bitcoin.graph_log (b_number, b_created) VALUES
        (%s, %s)""", (bn, datetime.utcnow()))

        tic = time.time()
        b = Path(path) / Path(bhash[-2]) / Path(bhash[-1])
        b.mkdir(parents=True, exist_ok=True)
        print(f'Writing {Sv.nvals} sender vals for {bhash}.')
        Svf = b / Path(f'{bhash}_Sv.ssb')
        print(f'Writing {Rv.nvals} receiver vals for {bhash}.')
        Rvf = b / Path(f'{bhash}_Rv.ssb')
        Sv.to_binfile(bytes(Svf))
        Rv.to_binfile(bytes(Rvf))
        print(f'matrix block write took {time.time()-tic}')
