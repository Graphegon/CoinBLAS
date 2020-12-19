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

    @lazy
    @curse
    @query
    def spend_ids(self, curs):
        """
        SELECT o_id FROM bitcoin.output 
        JOIN bitcoin.address ON a_id 
        WHERE a_address '{self.address}')
        """
        return [i[0] for i in curs.fetchall()]

    @property
    def spends(self):
        from .relation import Spend

        for i in self.spend_ids:
            yield Spend(self.chain, i, self.chain.IT[i, :].reduce_int())

    def id_vector(self, assign=0, T=UINT64):
        v = maximal_vector(T)
        for o_id in self.spend_ids:
            v[o_id] = assign
        return v

    def bfs_parent(self, depth=lib.GxB_INDEX_MAX, sring=semiring.ANY_SECONDI1_INT64):
        IO = self.chain.IO
        q = self.id_vector().apply(unaryop.POSITIONI1_INT64)
        pi = q.dup()
        for level in range(min(depth + 1, IO.nvals)):
            q.vxm(IO, out=q, mask=pi, semiring=sring, desc=descriptor.RC)
            if not q:
                break
            pi.assign(q, mask=q, desc=descriptor.S)
        return pi

    @curse
    def exposure(self, curs, end_addr, depth=lib.GxB_INDEX_MAX):
        from .relation import Exposure

        if isinstance(end_addr, str):
            end_addr = Address(self.chain, end_addr)

        debug = self.chain.logger.debug
        debug(f"Tracing {self.address} to {end_addr.address}")
        tic = time()

        start = self.id_vector(lib.GxB_INDEX_MAX)
        end = end_addr.id_vector()

        end_nvals = end.nvals
        found = 0

        if not len(start):
            self.chain.logger.warning("No starting address spends.")
            return
        if not len(end):
            self.chain.logger.warning("No ending address spends.")
            return

        IO = self.chain.IO

        end_max = end.apply(unaryop.POSITIONI_INT64).reduce_int(monoid.MAX_MONOID)
        start_min = start.apply(unaryop.POSITIONI_INT64).reduce_int(monoid.MIN_MONOID)

        if end_max < start_min:
            self.chain.logger.warning(
                f"No {self.address} spends found before any {end_addr.address}"
            )
            return
        debug(
            f"{start.nvals} occurences of {self.address} to {end.nvals} occurences of {end_addr.address}"
        )

        debug(
            f"Search is between blocks {get_block_number(start_min)} "
            f"and {get_block_number(end_max)} "
        )
        send = start[end.pattern()]
        for level in range(min(depth + 1, IO.nvals)):
            w = start[end.pattern()]
            with semiring.PLUS_MIN, Accum(binaryop.MIN):
                start @= IO
            send = start[end.pattern()]
            if send.nvals > found:
                toc = time() - tic
                debug(
                    f"After {level} rounds searched {start.nvals} "
                    f"addresses found {found+1} of {end_nvals} "
                    f"after {toc:.4f} seconds ({start.nvals/toc:.4f} edges/second)"
                )
                found = send.nvals
            if send.nvals == end_nvals and w.iseq(send):
                break
        debug(f"Flow search took {time()-tic:.4f}")
        max_block = self.chain.blocks[
            get_block_number(
                send.apply(unaryop.POSITIONI_INT64).reduce_int(monoid.MAX_MONOID)
            )
        ]
        debug(f"Got as far as block {max_block}")
        return send

    def __repr__(self):
        return f"<Address: {self.address}>"
