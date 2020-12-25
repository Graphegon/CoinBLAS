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

    @lazy
    def block(self):
        from .block import Block

        return Block(self.chain, self.block_number)

    @property
    def input_vector(self):
        return self.chain.IT[:, self.id]

    @property
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

    def summary(self):
        print(f"Summary for {self.hash}")
        print(f"Block: {self.block_number}")

        inputs = list(self.inputs)
        outputs = self.outputs

        if len(inputs) == 1 and inputs[0].coinbase:
            print("Coinbase Transaction")
        else:
            print("  Inputs")
            for i in inputs:
                print(f"    ", i)
                print(f"      > from: {i.tx.hash}")
        print("  Outputs")
        for o in outputs:
            print(f"    ", o)
            if o.spent_tx:
                print(f"      > spent: {o.spent_tx.hash}")

    def __repr__(self):
        return f"<Tx: {self.hash}>"
