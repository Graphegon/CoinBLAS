from pathlib import Path
from pygraphblas import *
from collections import defaultdict
from pathlib import Path
import sqlite3
import time
from itertools import groupby

from google.cloud import bigquery

from .util import *

class BitcoinLoader:

    quiet = False
    verbose = False

    def __init__(self, path=None):
        init = False
        if path is None:
            path = f"data/bitcoin/{time.strftime('%Y%m%d-%H%M%S')}.db"
        if not Path(path).exists():
            init = True
        self.path = path
        self.db = sqlite3.connect(self.path)
        if init:
            self.setup_db()

    def setup_db(self):
        e = self.db.execute

        query = f"""
        DROP TABLE IF EXISTS blocks;
        DROP TABLE IF EXISTS transactions;
        DROP TABLE IF EXISTS senders;
        DROP TABLE IF EXISTS receivers;

        CREATE TABLE block(
        b_number INTEGER PRIMARY KEY,
        b_hash TEXT NOT NULL,
        b_timestamp TEXT NOT NULL,
        b_timestamp_month TEXT NOT NULL,
        b_data BLOB
        );

        CREATE TABLE tx(
        t_id BIGINT PRIMARY KEY,
        t_hash TEXT NOT NULL,
        b_number BIGINT NOT NULL,
        FOREIGN KEY (b_number) REFERENCES block (b_number)
        );

        CREATE INDEX tx_hash ON tx (t_hash);

        CREATE TABLE sender(
        s_id BIGINT PRIMARY KEY,
        s_address TEXT
        );
        CREATE INDEX sender_address ON sender (s_address);

        CREATE TABLE sender_row(
        s_id BIGINT,
        row BIGINT
        );

        CREATE TABLE receiver(
        r_id BIGINT PRIMARY KEY,
        r_address TEXT
        );
        CREATE INDEX receiver_address ON receiver (r_address);

        CREATE TABLE receiver_col(
        r_id BIGINT,
        col BIGINT
        );

        """
        for s in query.split(';'):
            print(s)
            e(s)


    def get_block_id(self, number):
        return number >> 32

    def get_tx_id(self, bn, index):
        assert bn < (1<<32)
        assert index < (1<<16)
        return (bn << 32) + (index << 16)

    def get_sender_id(self, tx_id, sender_index):
        assert sender_index < (1<<16)
        return tx_id + sender_index
    
    def get_receiver_id(self, tx_id, index, input_count):
        assert (index + input_count) < (1<<16)
        return (tid << 16) + index + input_count

    def insert_block_transactions(self, block_number, group):
        self.db.execute("BEGIN")
        txns = []
        for i, t in enumerate(group):
            if i == 0:
                print(f"starting block {t.b_number}")
                self.db.execute(
                    'INSERT INTO block (b_number, b_hash, b_timestamp, b_timestamp_month) VALUES (?,?,?,?)',
                    (t.b_number, t.b_hash, t.b_timestamp, t.b_timestamp_month))
            txns.append((self.get_tx_id(t.b_number, i), t.t_hash, t.b_number))
        self.db.executemany(
            'INSERT INTO tx (t_id, t_hash, b_number) VALUES (?,?,?)', txns)
        self.db.execute('COMMIT')

    def load_month(self, month='2020-11-01'):
        start = time.time()
        client = bigquery.Client()

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
            for i, t in enumerate(group):
                assert bn == t['b_number']
                self.insert_block_transactions(bn, group)
        print(f'Took {time.time() - start} seconds')

    def build(self, block_depth=1000):
        for t in map(Object, client.query(query)):
            if t.iindex not in inputs:
                stid = bs.transactions.get(t.spent_hash, tid)
                iid = self.get_id(stid, t.spent_index)
                for ia in t.iaddress:
                    bs.senders[ia][iid] = iid
                ia = t.iaddress[0]
                bs.sender_ids[iid] = ia
                bs.Sv[iid, tid] = t.ivalue
                inputs.add(t.iindex)
                if self.verbose:
                    print(f'Added sender {ia}')

            if t.oindex not in outputs:
                oid = self.get_id(tid, t.oindex)
                for oa in t.oaddress:
                    bs.receivers[oa][oid] = iid
                oa = t.oaddress[0]
                bs.receiver_ids[oid] = oa
                bs.Rv[tid, oid] = t.ovalue
                outputs.add(t.oindex)
                if self.verbose:
                    print(f'Added receiver {oa}')
