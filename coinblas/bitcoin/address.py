from time import time

from coinblas.util import (
    curse,
    get_block_number,
    maximal_vector,
    query,
)

from pygraphblas import (
    Accum,
    UINT64,
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
    def __init__(self, chain, address):
        self.chain = chain
        self.address = address

    @curse
    @query
    def spend_ids(self, curs):
        """
        SELECT a_id FROM bitcoin.address WHERE a_address = '{self.address}'
        """
        return [i[0] for i in curs.fetchall()]

    @property
    @curse
    def spends(self, curs):
        from .spend import Spend

        for i in self.spend_ids():
            yield Spend(self.chain, i, self.chain.IT[i, :].reduce_int())

    @curse
    def id_vector(self, curs, T=UINT64, assign=0):
        v = maximal_vector(T)
        for a_id in self.spend_ids():
            v[a_id] = assign
        return v

    def bfs(self, depth=lib.GxB_INDEX_MAX):
        v = self.id_vector(UINT64, 0)
        q = self.id_vector(BOOL, True)
        IO = self.chain.IO
        for level in range(min(depth, IO.nvals)):
            v.assign_scalar(level, mask=q, desc=descriptor.S)
            if not q:
                break
            with BOOL.ANY_PAIR:
                q = q.vxm(IO, mask=v, desc=descriptor.RC)
        return v

    @curse
    def exposure(self, curs, end_addr, max_iters=lib.GxB_INDEX_MAX):
        from .spend import Exposure
        from .bitcoin import logger

        if isinstance(end_addr, str):
            end_addr = Address(self.chain, end_addr)

        logger.debug(f"Tracing {self.address} to {end_addr.address}")
        tic = time()

        start = self.id_vector(assign=lib.GxB_INDEX_MAX)
        end = end_addr.id_vector(assign=0)

        end_nvals = end.nvals
        found = 0

        if not len(start):
            logger.debug("No starting address spends.")
            return
        if not len(end):
            logger.debug("No ending address spends.")
            return

        IO = self.chain.IO

        end_max = end.apply(unaryop.POSITIONI_INT64).reduce_int(monoid.MAX_MONOID)
        start_min = start.apply(unaryop.POSITIONI_INT64).reduce_int(monoid.MIN_MONOID)

        if end_max < start_min:
            logger.debug(
                f"No {self.address} spends found before any {end_addr.address}"
            )
            return
        logger.debug(
            f"{start.nvals} occurences of {self.address} to {end.nvals} occurences of {end_addr.address}"
        )

        logger.debug(
            f"Search is between blocks {get_block_number(start_min)} "
            f"and {get_block_number(end_max)} "
        )
        send = start[end.pattern()]
        for level in range(min(max_iters, IO.nvals)):
            w = start[end.pattern()]
            with semiring.PLUS_MIN, Accum(binaryop.MIN):
                start @= IO
            send = start[end.pattern()]
            if send.nvals > found:
                logger.debug(
                    f"After {level} rounds searched {start.nvals} "
                    f"addresses found {found+1} of {end_nvals} "
                    f"after {time()-tic:.4f} seconds"
                )
                found = send.nvals
            if send.nvals == end_nvals and w.iseq(send):
                break
        logger.debug(f"Flow search took {time()-tic:.4f}")
        return [Exposure(self.chain, i, v) for i, v in send]

    def __repr__(self):
        return f"<Address: {self.address}>"
