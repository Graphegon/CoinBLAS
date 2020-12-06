import sys
from time import time
from pathlib import Path
from multiprocessing.pool import ThreadPool
from itertools import zip_longest, repeat

from .util import *
from .types import Flow

import psycopg2 as pg
from pygraphblas import *

POOL_SIZE=8

class Bitcoin:
    """Load graphs and blocks for bitcoin analysis.

    This class contains the query state for bitcoin graph analysis
    with the GraphBLAS.

      self.Sv:  sender_id by transaction id incidence matrix
      self.Rv:  transaction_id by receiver_id incidence matrix
      self.Av:  Adjacency matrix projected with PLUS_SECOND
    """

    def __init__(self, dsn, block_path, start, stop):
        self.dsn = dsn
        self.block_path = Path(block_path)
        self.pool = ThreadPool(POOL_SIZE)
        self.Sv = maximal_matrix(UINT64)
        self.Rv = maximal_matrix(UINT64)
        self.Av = None
        self.load_blocks(start, stop)

    def get_block_number(self, id):
        return id >> 32

    def get_block_id(self, id):
        return self.get_block_number(id) << 32

    def get_tx_id(self, id):
        return (id >> 16) << 16

    @curse
    def load_blocks(self, curs, start, stop):
        curs.execute(
            'select b_hash, b_number from bitcoin.block where b_timestamp_month between %s and %s',
            (start, stop,))
        Sblocks = curs.fetchall()
        Rblocks = Sblocks.copy()

        Sblocks = list(grouper(zip(Sblocks, repeat('Sv')), 2, None))
        while len(Sblocks) > 1:
            Sblocks = list(
                grouper(
                    self.pool.map(
                        self.merge_block_pair,
                        Sblocks),
                    2, None)
            )

        Rblocks = list(grouper(zip(Rblocks, repeat('Rv')), 2, None))
        while len(Rblocks) > 1:
            Rblocks = list(
                grouper(
                    self.pool.map(
                        self.merge_block_pair,
                        Rblocks),
                    2, None)
            )

        self.Sv += self.merge_block_pair(Sblocks[0])
        self.Rv += self.merge_block_pair(Rblocks[0])

        with semiring.PLUS_SECOND:
            self.Av = self.Sv @ self.Rv

    def merge_block_pair(self, pair):
        left, right = pair
        if left and not isinstance(left, Matrix):
            (bhash, bn), suffix = left
            sf = self.block_path / bhash[-2] / bhash[-1] / f'{bn}_{bhash}_{suffix}.ssb'
            if not sf.exists():
                return right
            left = Matrix.from_binfile(bytes(sf))
        if right and not isinstance(right, Matrix):
            (bhash, bn), suffix = right
            rf = self.block_path / bhash[-2] / bhash[-1] / f'{bn}_{bhash}_{suffix}.ssb'
            if not rf.exists():
                return left
            right = Matrix.from_binfile(bytes(rf))
        if left is None:
            return right
        if right is None:
            return left
        return left + right

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

    @prepared
    def address_vector(self, curs, assign=0):
        """\
        Return a vector of ids for an address with values assigned, the
        default is 0.

            SELECT a_id FROM bitcoin.address WHERE a_address = $1
        """
        v = maximal_vector(UINT64)
        for a_id, in curs.fetchall():
            v[a_id] = assign
        return v

    @prepared
    def vector_address(self, curs):
        """\
        Return addreses for a vector
        select a_id, a_address from bitcoin.address where a_id = ANY($1)
        """
        return curs.fetchall()

    @prepared
    def block_date_range(self, curs):
        """
        Return the min and max blocks for a date range.

            SELECT
                min(b_number),
                max(b_number) + 1
            FROM bitcoin.block WHERE b_timestamp <@ tstzrange($1, $2)
        """
        x, y = curs.fetchone()
        return x << 32, y << 32

    @prepared
    def date_block_range(self, curs):
        """
        Return the min and max blocks for a date range.

            SELECT
                min(b_timestamp),
                max(b_timestamp)
            FROM bitcoin.block WHERE b_number <@ int4range($1, $2)
        """
        return curs.fetchone()

    @curse
    def flow(self, curs, start_addr, end_addr, debug=False):
        if debug:
            print(f'Tracing {start_addr} to {end_addr}')
            tic = time()

        start = self.address_vector(curs, start_addr, assign=GxB_INDEX_MAX)
        end = self.address_vector(curs, end_addr, assign=0)
        end_nvals = end.nvals
        found = 0

        if not len(start):
            if debug:
                print('No starting address occurences.')
            return
        if not len(end):
            if debug:
                print('No ending address occurences.')
            return

        end_max = end.apply(unaryop.POSITIONI_INT64).reduce_int(monoid.MAX_MONOID)
        start_min = start.apply(unaryop.POSITIONI_INT64).reduce_int(monoid.MIN_MONOID)

        if (end_max < start_min):
            if debug:
                print(f'No {start_addr} occurences found before any {end_addr}')
                return
        if debug:
            print(f'{start.nvals} occurences of {start_addr}')
            print(f'{end.nvals} occurences of {end_addr}')

        sAv = self.Av.extract_matrix(
            slice(start_min, end_max),
            slice(start_min, end_max))

        print(f'Between blocks {self.get_block_number(start_min)} '
              f'and {self.get_block_number(end_max)} '
              f'search space is {sAv.nvals} edges')

        z_start = Vector.sparse(UINT64, sAv.nrows)
        for i, v in start:
            z_start[i-start_min] = v

        z_end = Vector.sparse(UINT64, sAv.nrows)
        for i, v in end:
            z_end[i-start_min] = v

        for level in range(sAv.nvals):
            w = z_start[z_end.pattern()]
            with semiring.PLUS_MIN, Accum(binaryop.MIN):
                z_start @= sAv
            z_send = z_start[z_end.pattern()]
            if debug:
                print(f'After round {level} searched {z_start.nvals} addresses.')
                if z_send.nvals > found:
                    print(f'Found {found+1} of {end_nvals} after {time()-tic:.4f}')
                    found = z_send.nvals
            if z_send.nvals == end_nvals and w.iseq(z_send):
                break
        print(f'Flow search took {time()-tic:.4f}')
        s_ids, s_vals = z_send.to_lists()

        return list(zip(self.vector_address(curs, s_ids), map(btc, s_vals)))

    @curse
    def transaction_summary(self, curs, t_hash):
        if isinstance(t_hash, int):
            t_hash = self.tid_transaction(curs, t_hash)
        print(f'Summary for {t_hash}')
        t_id = self.transaction_tid(curs, t_hash)
        b_id = self.get_block_id(t_id)
        bn = self.get_block_number(b_id)
        print(f'Block: {bn}')

        sv = self.Sv[:,t_id].to_lists()
        rv = self.Rv[t_id,:].to_lists()

        if len(sv[0]) == 1 and sv[0][0] == b_id:
            print('Coinbase')
        else:
            print('Senders')
            saddrs = self.vector_address(curs, sv[0])
            for sa, sv in zip(saddrs, sv[1]):
                print(f'    {sa[1]}: {btc(sv):.8f}')
                spent = self.Rv[:,sa[0]]
                if spent.nvals:
                    t_id = spent.to_lists()[0][0]
                    bn = self.get_block_number(t_id)
                    spent_hash = self.tid_transaction(curs, t_id)
                    print(f'        from {spent_hash} in block {bn}')
                else:
                    print(f'        from unknown')

        print('Receivers')
        raddrs = self.vector_address(curs, rv[0])
        for ra, rv in zip(raddrs, rv[1]):
            print(f'    {ra[1]}: {btc(rv)}')
            spent = self.Sv[ra[0],:]
            if spent.nvals:
                t_id = spent.to_lists()[0][0]
                bn = self.get_block_number(t_id)
                spent_hash = self.tid_transaction(curs, t_id)
                print(f'        to {spent_hash} in block {bn}')
            else:
                print(f'        to unknown')

    @curse
    def address_summary(self, curs, a_address):
        occurences = self.address_vector(curs, a_address)
        print(f'{len(occurences)} occurences of {a_address} found:')
        for a_id, _ in occurences:
            t_hash = self.tid_transaction(curs, self.get_tx_id(a_id))
            bn = self.get_block_number(a_id)
            print(f'Transaction {t_hash} in block {bn}')

    @curse
    def summary(self, curs):
        occurences = g.Av.reduce_vector().apply(unaryop.POSITIONI_INT64)
        min_occurence = self.get_block_number(occurences.reduce_int(monoid.MIN_MONOID))
        max_occurence = self.get_block_number(occurences.reduce_int(monoid.MAX_MONOID))
        min_time, max_time = self.date_block_range(curs, min_occurence, max_occurence)
        t_count = g.Sv.reduce_vector().pattern().reduce_int()
        in_val = g.Sv.reduce_int()
        out_val = g.Rv.reduce_int()
        print(f'Sender matrix has {self.Sv.nvals} edges.')
        print(f'Receiver matrix has {self.Sv.nvals} edges.')
        print(f'Adjacency matrix has {self.Av.nvals} edges.')
        print(f'Blocks span {min_occurence} to {max_occurence}')
        print(f'Blocks time span {min_time} to {max_time}')
        print(f'There are {t_count} transactions.')
        print(f'Total value intput {btc(in_val)} output {btc(out_val)}')

if __name__ == '__main__':
    g = Bitcoin('host=db user=postgres dbname=coinblas password=postgres',
                '/coinblas/blocks/bitcoin', sys.argv[1], sys.argv[2])
