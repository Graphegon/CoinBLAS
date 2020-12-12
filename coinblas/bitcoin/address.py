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
    Vector,
    binaryop,
    lib,
    monoid,
    semiring,
    unaryop,
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
    def id_vector(self, curs, assign=0):
        v = maximal_vector(UINT64)
        for a_id in self.spend_ids():
            v[a_id] = assign
        return v

    @curse
    def exposure(self, curs, end_addr, debug=False):
        from .spend import Exposure

        if isinstance(end_addr, str):
            end_addr = Address(self.chain, end_addr)

        if debug:
            print(f"Tracing {self.address} to {end_addr.address}")
            tic = time()

        start = self.id_vector(assign=lib.GxB_INDEX_MAX)
        end = end_addr.id_vector(assign=0)

        end_nvals = end.nvals
        found = 0

        if not len(start):
            if debug:
                print("No starting address spends.")
            return
        if not len(end):
            if debug:
                print("No ending address spends.")
            return

        IO = self.chain.IO

        end_max = end.apply(unaryop.POSITIONI_INT64).reduce_int(monoid.MAX_MONOID)
        start_min = start.apply(unaryop.POSITIONI_INT64).reduce_int(monoid.MIN_MONOID)

        if end_max < start_min:
            if debug:
                print(f"No {self.address} spends found before any {end_addr.address}")
                return
        if debug:
            print(f"{start.nvals} occurences of {self.address}")
            print(f"{end.nvals} occurences of {end_addr.address}")

        if debug:
            print(
                f"Search is between blocks {get_block_number(start_min)} "
                f"and {get_block_number(end_max)} "
            )
        send = start[end.pattern()]
        for level in range(IO.nvals):
            w = start[end.pattern()]
            with semiring.PLUS_MIN, Accum(binaryop.MIN):
                start @= IO
            send = start[end.pattern()]
            if debug:
                if send.nvals > found:
                    print(
                        f"After {level} rounds searched {start.nvals} "
                        f"addresses found {found+1} of {end_nvals} "
                        f"after {time()-tic:.4f} seconds"
                    )
                    found = send.nvals
            if send.nvals == end_nvals and w.iseq(send):
                break
        if debug:
            print(f"Flow search took {time()-tic:.4f}")
        return [Exposure(self.chain, i, v) for i, v in send]

    def __repr__(self):
        return f"<Address: {self.address}>"
