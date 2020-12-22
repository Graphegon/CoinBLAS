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
        return curs.fetchone()[0]

    @property
    def sent(self):
        return self.chain.SI[self.id, :]

    @property
    def received(self):
        return self.chain.OR[:, self.id]

    def bfs_parent(self, depth=lib.GxB_INDEX_MAX):
        SR = self.chain.SR
        q = maximal_vector(INT64)
        q[self.id] = self.id
        pi = q.dup()
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
            with semiring.PLUS_MIN_INT64:
                q.vxm(SR, out=q, mask=pi, desc=descriptor.RSC)
            if not q:
                break
            pi.assign(q, mask=q, desc=descriptor.S)
        return pi

    def __repr__(self):
        return f"<Address: {self.address}>"
