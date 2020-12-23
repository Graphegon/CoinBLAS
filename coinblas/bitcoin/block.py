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
    def insert(self, curs, bq):
        curs.execute(
            """
        INSERT INTO bitcoin.base_block
            (b_number, b_hash, b_timestamp, b_timestamp_month)
        VALUES
            (%s, %s, %s, %s)
        """,
            (self.number, self.hash, bq.timestamp, bq.timestamp_month),
        )

    @curse
    def finalize(self, curs, month):
        month = str(month).replace("-", "_")
        i_addrs = []
        o_addrs = []

        for t_id, tx in self.pending_txs.items():
            for i, v in tx.pending_inputs.items():
                self.IT[i, t_id] = v

            for o, v in tx.pending_outputs.items():
                self.TO[t_id, o] = v
                blockout = self.BT.get(self.id, t_id, 0)
                self.BT[self.id, t_id] = blockout + v

            i_addrs += tx.pending_input_addresses
            o_addrs += tx.pending_output_addresses

        senders = execute_values(
            curs,
            f"""
            WITH
                i_addrs (a_address, t_id, i_id, value) AS (VALUES %s ORDER BY 3),
                a_ids AS (
                    INSERT INTO bitcoin.address (a_address)
                    SELECT a_address FROM i_addrs
                    ON CONFLICT DO NOTHING
                    RETURNING *
                )
            SELECT a.a_id, i.t_id, i.i_id, i.value
            FROM i_addrs i JOIN
                (SELECT * FROM a_ids UNION SELECT * FROM bitcoin.address) a
            USING(a_address)
            """,
            i_addrs,
            fetch=True,
            page_size=10000,
        )
        assert len(senders) == len(i_addrs)
        sas = defaultdict(int)
        for a_id, t_id, i_id, i_value in senders:
            self.SI[a_id, i_id] = i_value
            sas[(a_id, t_id)] += i_value

        for (a_id, t_id), i_value in sas.items():
            self.ST[a_id, t_id] = i_value

        receivers = execute_values(
            curs,
            f"""
            WITH
                o_addrs (a_address, t_id, o_id, value) AS (VALUES %s ORDER BY 3),
                a_ids AS (
                    INSERT INTO bitcoin.address (a_address)
                    SELECT a_address FROM o_addrs
                    ON CONFLICT DO NOTHING
                    RETURNING *
                )
            SELECT a.a_id, o.t_id, o.o_id, o.value
            FROM o_addrs o JOIN
                (SELECT * FROM a_ids UNION select * from bitcoin.address) a
            USING(a_address)
            """,
            o_addrs,
            fetch=True,
            page_size=10000,
        )
        assert len(receivers) == len(o_addrs)
        ras = defaultdict(int)
        for a_id, t_id, o_id, o_value in receivers:
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
            [(a[1],) for a in o_addrs],
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
