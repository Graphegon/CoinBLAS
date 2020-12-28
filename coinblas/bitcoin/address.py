from time import time

from coinblas.util import (
    curse,
    get_block_number,
    maximal_vector,
    query,
    lazy,
)

from pygraphblas import (
    Accum,
    INT64,
    BOOL,
    Vector,
    binaryop,
    lib,
    monoid,
    semiring,
    unaryop,
    descriptor,
)

from .tx import Tx
from .spend import Spend


class Address:
    def __init__(self, chain, id, address=None):
        self.chain = chain
        self.id = id
        if address:
            self.address = address

    @lazy
    @curse
    @query
    def address(self, curs):
        """
        SELECT a_address FROM bitcoin.address WHERE a_id = {self.id}
        """
        r = curs.fetchone()
        if r is None:
            return
        return r[0]

    @property
    def sent_vector(self):
        return self.chain.SI[self.id, :]

    @property
    def received_vector(self):
        return self.chain.OR[:, self.id]

    @property
    def sent(self):
        for r_id, v in self.sent_v:
            yield Spend(self.chain, r_id, v)

    @property
    def received(self):
        for r_id, v in self.received_v:
            yield Spend(self.chain, r_id, v)

    @property
    def txs_as_sender_vector(self):
        return self.chain.ST[self.id, :]

    @property
    def txs_as_receiver_vector(self):
        return self.chain.TR[:, self.id]

    @property
    def txs_as_sender(self):
        for a_id, t_id in self.txs_as_sender_vector:
            yield Tx(self.chain, id=t_id)

    @property
    def txs_as_receiver(self):
        for t_id, a_id in self.txs_as_receiver_vector:
            yield Tx(self.chain, id=t_id)

    def bfs_level(self, depth=lib.GxB_INDEX_MAX):
        SR = self.chain.SR
        q = maximal_vector(INT64)
        pi = q.dup()
        q[self.id] = 0
        for level in range(min(depth + 1, SR.nvals)):
            with semiring.ANY_PAIR_INT64:
                q.vxm(SR, out=q, mask=pi, desc=descriptor.RSC)
            if not q:
                break
            pi.assign_scalar(level + 1, mask=q, desc=descriptor.S)
        return pi

    def bfs_parent(self, depth=lib.GxB_INDEX_MAX):
        SR = self.chain.SR
        q = maximal_vector(INT64)
        pi = q.dup()
        q[self.id] = self.id
        for level in range(min(depth + 1, SR.nvals)):
            with semiring.ANY_SECONDI_INT64:
                q.vxm(SR, out=q, mask=pi, desc=descriptor.RSC)
            if not q:
                break
            pi.assign(q, mask=q, desc=descriptor.S)
        return pi

    def bfs_exposure(self, depth=lib.GxB_INDEX_MAX):
        SR = self.chain.SR
        q = maximal_vector(INT64)
        pi = q.dup()
        q[self.id] = lib.GxB_INDEX_MAX
        for level in range(min(depth + 1, SR.nvals)):
            with semiring.MIN_MIN_INT64:
                q.vxm(SR, out=q, mask=pi, desc=descriptor.RSC)
            if not q:
                break
            pi.assign(q, mask=q, desc=descriptor.S)
        return pi

    def __repr__(self):
        return f"<Address: {self.address}>"
