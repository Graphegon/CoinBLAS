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
    def flow(self, curs, end, debug=False):
        from .spend import Exposure

        if isinstance(end, str):
            end = Address(self.chain, end)

        if debug:
            print(f"Tracing {self.address} to {end.address}")
            tic = time()

        start_v = self.id_vector(assign=lib.GxB_INDEX_MAX)
        end_v = end.id_vector(assign=0)

        end_nvals = end_v.nvals
        found = 0

        if not len(start_v):
            if debug:
                print("No starting address spends.")
            return
        if not len(end_v):
            if debug:
                print("No ending address spends.")
            return

        end_max = end_v.apply(unaryop.POSITIONI_INT64).reduce_int(monoid.MAX_MONOID)
        start_min = start_v.apply(unaryop.POSITIONI_INT64).reduce_int(monoid.MIN_MONOID)

        if end_max < start_min:
            if debug:
                print(f"No {self.address} spends found before any {end.address}")
                return
        if debug:
            print(f"{start_v.nvals} occurences of {self.address}")
            print(f"{end_v.nvals} occurences of {end.address}")

        IO = self.chain.IO.extract_matrix(
            slice(start_min, end_max), slice(start_min, end_max)
        )

        if debug:
            print(
                f"Between blocks {get_block_number(start_min)} "
                f"and {get_block_number(end_max)} "
                f"search space is {IO.nvals} edges"
            )

        z_start = Vector.sparse(UINT64, IO.nrows)
        for i, v in start_v:
            z_start[i - start_min] = v

        z_end = Vector.sparse(UINT64, IO.nrows)
        for i, v in end_v:
            z_end[i - start_min] = v

        z_send = z_start[z_end.pattern()]
        for level in range(IO.nvals):
            w = z_start[z_end.pattern()]
            with semiring.PLUS_MIN, Accum(binaryop.MIN):
                z_start @= IO
            z_send = z_start[z_end.pattern()]
            if debug:
                if z_send.nvals > found:
                    print(
                        f"After {level} rounds searched {z_start.nvals} "
                        f"addresses found {found+1} of {end_nvals} "
                        f"after {time()-tic:.4f} seconds"
                    )
                    found = z_send.nvals
            if z_send.nvals == end_nvals and w.iseq(z_send):
                break
        if debug:
            print(f"Flow search took {time()-tic:.4f}")
        return [Exposure(self.chain, i + start_min, v) for i, v in z_send]

    def __repr__(self):
        return f"<Address: {self.address}>"
