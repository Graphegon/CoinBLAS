from itertools import chain
from pathlib import Path
from psycopg2.extras import execute_values

from pygraphblas import UINT64

from coinblas.util import (
    curse,
    get_block_number,
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

    @classmethod
    def from_id(cls, chain, id):
        return Block(chain, get_block_number(id))

    @classmethod
    def bq_insert(cls, chain, bq):
        b = Block(chain, bq.number, bq.hash)
        b.insert(bq)
        return b

    @lazy
    def BT(self):
        """ Incidence Matrix from Block id rows to Transaction id columns. """
        return maximal_matrix(UINT64)

    @lazy
    def IT(self):
        """ Incidence Matrix from Input id rows to Transaction id columns. """
        return maximal_matrix(UINT64)

    @lazy
    def TO(self):
        """ Incidence Matrix from Transaction id rows to Output id columns. """
        return maximal_matrix(UINT64)

    @lazy
    def SI(self):
        """ Incidence Matrix from Sender Address id rows to Input id columns. """
        return maximal_matrix(UINT64)

    @lazy
    def OR(self):
        """ Incidence Matrix from Output id rows to Receiver Address id columns. """
        return maximal_matrix(UINT64)

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

        for tx in self.pending_txs.values():
            for i, v in tx.pending_inputs.items():
                self.IT[i, tx.id] = v

            for o, v in tx.pending_outputs.items():
                t_id = tx.id
                self.TO[t_id, o] = v
                blockout = self.BT.get(self.id, t_id, 0)
                self.BT[self.id, t_id] = blockout + v

            i_addrs += [(a, i, v) for a, (i, v) in tx.pending_input_addresses.items()]
            o_addrs += [(a, o, v) for a, (o, v) in tx.pending_output_addresses.items()]

        r = execute_values(
            curs,
            f"""
            WITH
                a_addrs (a_address, i_id, value) AS (VALUES %s),
                a_ids AS (
                    INSERT INTO bitcoin.base_address (a_address)
                    SELECT a_address FROM a_addrs
                    ON CONFLICT DO NOTHING
                    RETURNING a_id, a_address
                )
            SELECT a.a_id, i.i_id, i.value 
            FROM a_addrs i JOIN a_ids a USING(a_address)
            """,
            i_addrs,
            fetch=True,
        )
        for a_id, i_id, i_value in r:
            self.SI[a_id, i_id] = i_value

        r = execute_values(
            curs,
            f"""
            WITH
                a_addrs (a_address, o_id, value) AS (VALUES %s),
                a_ids AS (
                    INSERT INTO bitcoin.base_address (a_address)
                    SELECT a_address FROM a_addrs
                    ON CONFLICT DO NOTHING
                    RETURNING a_id, a_address
                ),
                write_out as (
                    INSERT INTO bitcoin."base_output_{month}" (o_id, a_id)
                    SELECT o.o_id, a.a_id
                    FROM a_addrs o JOIN a_ids a USING(a_address)
                )
            SELECT a.a_id, o.o_id, o.value 
            FROM a_addrs o JOIN a_ids a USING(a_address)
            """,
            o_addrs,
            fetch=True,
        )
        for a_id, o_id, o_value in r:
            self.OR[o_id, a_id] = o_value

        execute_values(
            curs,
            f"""
            INSERT INTO bitcoin."base_tx_{month}" (t_id, t_hash) VALUES %s
            """,
            [(t.id, t.hash) for t in self.pending_txs.values()],
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

        self.BT.to_binfile(bytes(BTf))
        self.IT.to_binfile(bytes(ITf))
        self.TO.to_binfile(bytes(TOf))
        self.IT.to_binfile(bytes(SIf))
        self.TO.to_binfile(bytes(ORf))

    def __iter__(self):
        from .tx import Tx

        for t_id, _ in self.tx_vector:
            yield Tx(self.chain, id=t_id)

    def __repr__(self):
        return f"<Block: {self.number}>"
