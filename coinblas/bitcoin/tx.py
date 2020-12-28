from collections import defaultdict

from coinblas.util import (
    btc,
    curse,
    get_block_number,
    query,
    lazy,
)
from .spend import Spend


class Tx:
    def __init__(self, chain, id, hash=None, block=None):
        assert id is not None or hash is not None
        self.chain = chain
        self.id = id
        if hash is not None:
            self.hash = hash
        if block is not None:
            self.block = block

        self.pending_inputs = {}
        self.pending_outputs = {}
        self.pending_input_addresses = defaultdict(list)
        self.pending_output_addresses = defaultdict(list)

    @lazy
    @curse
    @query
    def hash(self, curs):
        """
        SELECT t_hash FROM bitcoin.tx WHERE t_id = {self.id}
        """
        h = curs.fetchone()
        if h is None:
            return None
        return h[0]

    @property
    def block_number(self):
        return get_block_number(self.id)

    @property
    def block(self):
        self.chain.blocks[self.block_number]

    @lazy
    def input_vector(self):
        return self.chain.IT[:, self.id]

    @lazy
    def output_vector(self):
        return self.chain.TO[self.id, :]

    @property
    def inputs(self):
        for i, v in self.input_vector:
            yield Spend(self.chain, i, v)

    @property
    def outputs(self):
        for i, v in self.output_vector:
            yield Spend(self.chain, i, v)

    @property
    def summary(self):
        r = f"Tx: {self.hash}\n"
        r += f"Block: {self.block_number}\n"

        inputs = list(self.inputs)
        outputs = self.outputs

        if len(inputs) == 1 and inputs[0].coinbase:
            r += "Coinbase Transaction\n"
        else:
            r += "  Inputs:\n"
            for i in inputs:
                r += f"    {i}\n"
                r += f"      \\ from: {i.tx.hash}\n"
        r += "  Outputs:\n"
        for o in outputs:
            r += f"    {o}\n"
            if o.spent_tx:
                r += f"      \\ spent: {o.spent_tx.hash}\n"
        return r

    def __repr__(self):
        return f"<Tx: {self.hash}>"
