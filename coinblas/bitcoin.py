from pathlib import Path
from pygraphblas import *
from collections import defaultdict
from pathlib import Path
import time
from itertools import groupby
from datetime import date
from multiprocessing import Pool, Process
import psycopg2 as pg

from google.cloud import bigquery

from .util import *

class BitcoinLoader:

    quiet = False
    verbose = False

    def __init__(self, dsn=None):
        self.dsn = dsn

    def get_block_id(self, number):
        return number >> 32

    def get_tx_id(self, bn, index):
        assert bn < (1<<32)
        assert index < (1<<16)
        return (bn << 32) + (index << 16)

    def get_address_id(self, tx_id, sender_index):
        assert sender_index < (1<<16)
        return tx_id + sender_index
    
    def load(self, start=None, end=None):
        client = bigquery.Client()

        total_span = [x['timestamp_month'] for x in client.query(
            """select 
            timestamp_month
            from `bigquery-public-data.crypto_bitcoin.blocks`
            group by timestamp_month order by timestamp_month ;
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
        pool = Pool()
        return pool.map(self.load_month, months)

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
                    for i, t in enumerate(group):
                        assert bn == t['b_number']
                        self.insert_block_transactions(curs, bn, group)

            print(f'Took {(time.time() - tic)/60.0} minutes for {month}')

    def insert_block_transactions(self, curs, block_number, group):
        txns = []
        for i, t in enumerate(group):
            if i == 0:
                curs.execute(
                    'INSERT INTO bitcoin.block (b_number, b_hash, b_timestamp, b_timestamp_month) VALUES (%s, %s, %s, %s)',
                    (t.b_number, t.b_hash, t.b_timestamp, t.b_timestamp_month))
            txns.append((self.get_tx_id(t.b_number, i), t.t_hash, t.b_timestamp_month))
        curs.executemany(
            'INSERT INTO bitcoin.tx (t_id, t_hash, b_timestamp_month) VALUES (%s, %s, %s)', txns)

    # def build(self, block_depth=1000):
    #     for t in map(Object, client.query(query)):
    #         if t.iindex not in inputs:
    #             stid = bs.transactions.get(t.spent_hash, tid)
    #             iid = self.get_id(stid, t.spent_index)
    #             for ia in t.iaddress:
    #                 bs.senders[ia][iid] = iid
    #             ia = t.iaddress[0]
    #             bs.sender_ids[iid] = ia
    #             bs.Sv[iid, tid] = t.ivalue
    #             inputs.add(t.iindex)
    #             if self.verbose:
    #                 print(f'Added sender {ia}')

    #         if t.oindex not in outputs:
    #             oid = self.get_id(tid, t.oindex)
    #             for oa in t.oaddress:
    #                 bs.receivers[oa][oid] = iid
    #             oa = t.oaddress[0]
    #             bs.receiver_ids[oid] = oa
    #             bs.Rv[tid, oid] = t.ovalue
    #             outputs.add(t.oindex)
    #             if self.verbose:
    #                 print(f'Added receiver {oa}')
