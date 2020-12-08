from coinblas.util import (
    btc,
    curse,
    get_block_id,
    get_block_number,
    lazy_property,
    query,
)
from .spend import Spend


class Tx:
    def __init__(self, chain, id):
        self.chain = chain
        self.id = id

    @lazy_property
    @curse
    @query
    def hash(self, curs):
        """
        SELECT t_hash FROM bitcoin.tx WHERE t_id = {self.id}
        """
        return curs.fetchone()[0]

    @lazy_property
    def block_number(self):
        return get_block_number(self.id)

    @lazy_property
    def block_id(self):
        return get_block_id(self.id)

    @lazy_property
    def block(self):
        from .block import Block

        return Block(self.chain, self.block_number)

    @lazy_property
    def input_vector(self):
        return self.chain.IT[:, self.id]

    @lazy_property
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

    @curse
    def summary(self, curs):
        print(f"Summary for {self.hash}")
        print(f"Block: {self.block_number}")

        inputs = list(self.inputs)
        outputs = self.outputs

        if len(inputs) == 1 and inputs[0].coinbase:
            print("Coinbase Transaction")
        else:
            print("  Inputs")
            for i in inputs:
                if i.address is None:
                    print(f"Unknown input {i.id}")
                    continue
                print("    ", i)
                if i.spent_vector:
                    print(f"        from {i.tx.hash} in block {i.tx.block_number}")
                else:
                    print(f"        from unknown")
        print("  Outputs")
        for o in outputs:
            if o.address is None:
                print(f"Unknown output {i.id}")
                continue
            print("    ", o)
            if o.spent_vector:
                print(f"        to {o.spent.hash} in block {o.spent.block_number}")
            else:
                print(f"        to unknown")

    def __repr__(self):
        return f"<Tx: {self.hash}>"
