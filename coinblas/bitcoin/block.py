from collections import defaultdict
from itertools import chain
from pathlib import Path
from psycopg2.extras import execute_values

from pygraphblas import UINT64, Matrix

from coinblas.util import (
    curse,
    get_block_number,
    get_tx_id,
    query,
    maximal_matrix,
    lazy,
)


class Block:
    def __init__(self, chain, number, hash=None):
        self.chain = chain
        self.id = number << 32
        self.number = number
        if hash is not None:
            self.hash = hash
        self.pending_txs = {}

    def load_block_graph(self, suffix):
        bhash = self.hash
        prefix = self.chain.block_path / bhash[-2] / bhash[-1]
        datafile = prefix / f"{self.number}_{bhash}_{suffix}.ssb"
        if not datafile.exists():
            return maximal_matrix(UINT64)
        return Matrix.from_binfile(bytes(datafile))

    @lazy
    def BT(self):
        """ Incidence Matrix from Block id rows to Transaction id columns. """
        return self.load_block_graph("BT")

    @lazy
    def IT(self):
        """ Incidence Matrix from Input id rows to Transaction id columns. """
        return self.load_block_graph("IT")

    @lazy
    def TO(self):
        """ Incidence Matrix from Transaction id rows to Output id columns. """
        return self.load_block_graph("TO")

    @lazy
    def SI(self):
        """ Incidence Matrix from Sender Address id rows to Input id columns. """
        return self.load_block_graph("SI")

    @lazy
    def OR(self):
        """ Incidence Matrix from Output id rows to Receiver Address id columns. """
        return self.load_block_graph("OR")

    @lazy
    def ST(self):
        """ Incidence Matrix from Sender Address id rows to Input id columns. """
        return self.load_block_graph("ST")

    @lazy
    def TR(self):
        """ Incidence Matrix from Output id rows to Receiver Address id columns. """
        return self.load_block_graph("TR")

    @lazy
    @curse
    @query
    def hash(self, curs):
        """
        SELECT b_hash FROM bitcoin.base_block WHERE b_number = {self.number}
        """
        return curs.fetchone()[0]

    @lazy
    @curse
    @query
    def timestamp(self, curs):
        """
        SELECT b_timestamp FROM bitcoin.base_block WHERE b_number  = {self.number}
        """
        return curs.fetchone()[0]

    @lazy
    def tx_vector(self):
        return self.chain.BT[self.id, :]

    @curse
    def finalize(self, curs, month):
        month = str(month).replace("-", "_")
        i_addrs = defaultdict(list)
        o_addrs = defaultdict(list)

        for t_id, tx in self.pending_txs.items():
            for i, v in tx.pending_inputs.items():
                self.IT[i, t_id] = v

            for o, v in tx.pending_outputs.items():
                self.TO[t_id, o] = v
                blockout = self.BT.get(self.id, t_id, 0)
                self.BT[self.id, t_id] = blockout + v

            for a, i in tx.pending_input_addresses.items():
                i_addrs[a] += i
            for a, o in tx.pending_output_addresses.items():
                o_addrs[a] += o

        to_insert = [(i,) for i in chain(i_addrs.keys(), o_addrs.keys())]

        curs.connection.commit()
        execute_values(
            curs,
            f"""
            INSERT INTO bitcoin.address (a_address)
            VALUES %s ORDER BY 1
            ON CONFLICT DO NOTHING
            """,
            to_insert,
            page_size=10000,
        )
        curs.connection.commit()

        curs.execute(
            """
            SELECT a_address, a_id from bitcoin.address where a_address = any(%s)
            """,
            (list(i_addrs.keys()),),
        )
        i_ids = dict(curs.fetchall())

        assert len(i_ids) == len(i_addrs)

        sas = defaultdict(int)
        for a_address, inputs in i_addrs.items():
            a_id = i_ids[a_address]
            for (t_id, i_id, i_value) in inputs:
                self.SI[a_id, i_id] = i_value
                sas[(a_id, t_id)] += i_value

        for (a_id, t_id), i_value in sas.items():
            self.ST[a_id, t_id] = i_value

        curs.execute(
            """
            SELECT a_address, a_id from bitcoin.address where a_address = any(%s)
            """,
            (list(o_addrs.keys()),),
        )
        o_ids = dict(curs.fetchall())

        assert len(o_ids) == len(o_addrs)

        ras = defaultdict(int)
        for a_address, outputs in o_addrs.items():
            a_id = o_ids[a_address]
            for (t_id, o_id, o_value) in outputs:
                self.OR[o_id, a_id] = o_value
                ras[(a_id, t_id)] += o_value

        for (a_id, t_id), o_value in ras.items():
            self.TR[t_id, a_id] = o_value

        execute_values(
            curs,
            f"""
            INSERT INTO bitcoin."base_tx_{month}" (t_id, t_hash) VALUES %s ORDER BY 1
            """,
            [(t.id, t.hash) for t in self.pending_txs.values()],
            page_size=10000,
        )
        curs.connection.commit()

        execute_values(
            curs,
            f"""
        UPDATE bitcoin.base_block
            SET b_addresses = s.agg,
                b_imported_at = now()
            FROM (SELECT hll_add_agg(hll_hash_bigint(v.id)) as agg
                  FROM (VALUES %s) v(id)) s
        WHERE b_number = {self.number}
            """,
            [(a,) for a in o_ids.values()],
            page_size=10000,
        )
        self.write_block_files(self.chain.block_path)

        self.pending_txs.clear()

    def write_block_files(self, path):
        b = Path(path) / Path(self.hash[-2]) / Path(self.hash[-1])
        b.mkdir(parents=True, exist_ok=True)
        BTf = b / Path(f"{self.number}_{self.hash}_BT.ssb")
        ITf = b / Path(f"{self.number}_{self.hash}_IT.ssb")
        TOf = b / Path(f"{self.number}_{self.hash}_TO.ssb")
        SIf = b / Path(f"{self.number}_{self.hash}_SI.ssb")
        ORf = b / Path(f"{self.number}_{self.hash}_OR.ssb")
        STf = b / Path(f"{self.number}_{self.hash}_ST.ssb")
        TRf = b / Path(f"{self.number}_{self.hash}_TR.ssb")

        self.BT.to_binfile(bytes(BTf))
        self.IT.to_binfile(bytes(ITf))
        self.TO.to_binfile(bytes(TOf))
        self.SI.to_binfile(bytes(SIf))
        self.OR.to_binfile(bytes(ORf))
        self.ST.to_binfile(bytes(STf))
        self.TR.to_binfile(bytes(TRf))

    def __iter__(self):
        from .tx import Tx

        for t_id, _ in self.tx_vector:
            yield Tx(self.chain, id=t_id)

    def __repr__(self):
        return f"<Block: {self.number}>"
