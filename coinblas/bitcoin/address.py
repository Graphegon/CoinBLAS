from coinblas.util import prepared, curse


class Address:
    def __init__(self, chain, address):
        self.chain = chain
        self.address = address

    @curse
    @prepared
    def spend_ids(self, curs):
        """
        SELECT a_id FROM bitcoin.address WHERE a_address =  %(address)s
        """
        return [i[0] for i in curs.fetchall()]

    @curse
    def spends(self, curs):
        from .spend import Spend

        return [
            Spend(self.chain, i, self.chain.Iv[i, :].reduce_int())
            for i in self.spend_ids()
        ]

    @curse
    def id_vector(self, curs, assign=0):
        v = maximal_vector(UINT64)
        for a_id in self.spend_ids():
            v[a_id] = assign
        return v

    @curse
    def flow(self, curs, start_addr, end_addr, debug=False):
        if debug:
            print(f"Tracing {start_addr} to {end_addr}")
            tic = time()

        start = start.id_vector(start_addr, assign=GxB_INDEX_MAX)
        end = end.id_vector(end_addr, assign=0)
        end_nvals = end.nvals
        found = 0

        if not len(start):
            if debug:
                print("No starting address occurences.")
            return
        if not len(end):
            if debug:
                print("No ending address occurences.")
            return

        end_max = end.apply(unaryop.POSITIONI_INT64).reduce_int(monoid.MAX_MONOID)
        start_min = start.apply(unaryop.POSITIONI_INT64).reduce_int(monoid.MIN_MONOID)

        if end_max < start_min:
            if debug:
                print(f"No {start_addr} occurences found before any {end_addr}")
                return
        if debug:
            print(f"{start.nvals} occurences of {start_addr}")
            print(f"{end.nvals} occurences of {end_addr}")

        sAv = self.Av.extract_matrix(
            slice(start_min, end_max), slice(start_min, end_max)
        )

        print(
            f"Between blocks {self.get_block_number(start_min)} "
            f"and {self.get_block_number(end_max)} "
            f"search space is {sAv.nvals} edges"
        )

        z_start = Vector.sparse(UINT64, sAv.nrows)
        for i, v in start:
            z_start[i - start_min] = v

        z_end = Vector.sparse(UINT64, sAv.nrows)
        for i, v in end:
            z_end[i - start_min] = v

        for level in range(sAv.nvals):
            w = z_start[z_end.pattern()]
            with semiring.PLUS_MIN, Accum(binaryop.MIN):
                z_start @= sAv
            z_send = z_start[z_end.pattern()]
            if debug:
                print(f"After round {level} searched {z_start.nvals} addresses.")
                if z_send.nvals > found:
                    print(f"Found {found+1} of {end_nvals} after {time()-tic:.4f}")
                    found = z_send.nvals
            if z_send.nvals == end_nvals and w.iseq(z_send):
                break
        print(f"Flow search took {time()-tic:.4f}")
        s_ids, s_vals = z_send.to_lists()

        return list(zip(self.vector_address(curs, s_ids), map(btc, s_vals)))
