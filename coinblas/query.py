from pathlib import Path

from .util import *

import psycopg2 as pg
from pygraphblas import *

GxB_INDEX_MAX = 1 << 60

POOL_SIZE=24

def maximal_matrix(T):
    return Matrix.sparse(T, GxB_INDEX_MAX, GxB_INDEX_MAX)


class Graphs:

    def __init__(self, dsn, block_path):
        self.dsn = dsn
        self.block_path = Path(block_path)
        self.Sv = maximal_matrix(UINT64)
        self.Rv = maximal_matrix(UINT64)

    def load_blocks(self, start, stop):
        for i in range(start, stop):
            with pg.connect(self.dsn) as conn:
                with conn.cursor() as curs:
                    curs.execute(
                        'select b_hash from bitcoin.block where b_number between %s and %s',
                        (start, stop,))
                    block_hashes = curs.fetchall()
                    for bhash, in block_hashes:
                        sf = self.block_path / bhash[-2] / bhash[-1] / f'{bhash}_Sv.ssb'
                        rf = self.block_path / bhash[-2] / bhash[-1] / f'{bhash}_Rv.ssb'
                        if sf.exists():
                            self.Sv += Matrix.from_binfile(bytes(sf))
                        if sf.exists():
                            self.Rv += Matrix.from_binfile(bytes(rf))
                        
    def transaction_summary(self, t):
        print(f'Summary for {t}')
        tid = self.state.transactions[t]
        sv = self.Sv[:,tid]
        rv = self.Rv[tid,:]
        print('Senders')
        for sid, value in sv:
            print(f'    Input address {sid}: {btc(value)}')
        print('Receivers')
        for rid, value in rv:
            print(f'    Output address {rid}: {btc(value)}')

    def flow(self, start, debug=False):
        if debug:
            print(f'Tracing {start}')
            
        with semiring.PLUS_SECOND:
            Av = self.state.Sv @ self.state.Rv

        v = self.state.senders[start].dup()
        v.assign_scalar(0, mask=v)
        
        for level in range(Av.nrows):
            w = v.dup()
            with semiring.MIN_PLUS, Accum(binaryop.MIN):
                v @= Av
            
            if w.iseq(v):
                break
            if debug:
                ids, vals = v.extract(w, desc=descriptor.C).to_lists()
                for di, dv in zip(ids, vals):
                    diblock = self.get_id_block(di)
                    rid = self.state.receiver_ids.get(di)
                    dt = self.state.Sv[di].reduce_int()
                    thash = self.state.tids[self.state.Rv[:,di].to_lists()[0][0]]
                    if rid:
                        print(f"{'  '*level}{btc(dv):.8f} to {rid} via {thash} block {diblock}")
        return v

    def exposure(self, A, B, debug=False):
        return self.flow(A, debug=debug)[self.state.receivers[B]]

g = Graphs('host=db user=postgres password=postgres', '/coinblas/blocks/bitcoin')
g.load_blocks(10000, 11000)
