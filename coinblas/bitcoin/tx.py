from coinblas.util import prepared, curse
from .spend import Spend


class Tx:
    def __init__(self, chain, hash=None, id=None):
        self.chain = chain
        with chain.cursor as curs:
            if hash:
                self.hash = hash
                self.id = self.hash_id()
            elif id:
                self.id = id
                self.hash = self.id_hash()

    @curse
    @prepared
    def insert(self, curs):
        """
        INSERT INTO bitcoin.tx (t_hash, t_id) VALUES %s
        """

    @curse
    @prepared
    def hash_id(self, curs):
        """
        SELECT t_id FROM bitcoin.tx WHERE t_hash = %(hash)s
        """
        return curs.fetchone()[0]

    @curse
    @prepared
    def id_hash(self, curs):
        """
        SELECT t_hash FROM bitcoin.tx WHERE t_id = %(id)s
        """
        return curs.fetchone()[0]

    @property
    def block_number(self):
        return self.id >> 32

    @property
    def block_id(self):
        return (self.id >> 32) << 32

    @property
    def block(self):
        from .block import Block

        return Block(self.chain, self.block_number)

    @property
    def input_vector(self):
        return self.chain.Iv[:, self.id]

    @property
    def output_vector(self):
        return self.chain.Ov[self.id, :]

    @property
    def inputs(self):
        return [Spend(self.chain, i, v) for i, v in self.input_vector]

    @property
    def outputs(self):
        return [Spend(self.chain, i, v) for i, v in self.output_vector]

    @curse
    def summary(self, curs):
        print(f"Summary for {self.hash}")
        print(f"Block: {self.block_number}")

        inputs = self.inputs
        outputs = self.outputs

        if len(inputs) == 1 and inputs[0].coinbase:
            print("Coinbase")
        else:
            print("Senders")
            for i in inputs:
                addr = i.address(curs)
                print(f"    {addr.address}: {btc(i.value):.8f}")
                if i.spent_vector:
                    print(f"        from {i.tx.hash} in block {i.tx.block_number}")
                else:
                    print(f"        from unknown")
        print("Receivers")
        for o in outputs:
            addr = o.address(curs)
            print(f"    {addr.address}: {btc(o.value):.8f}")
            if o.spent_vector:
                print(f"        to {o.spent.hash} in block {o.spent.block_number}")
            else:
                print(f"        to unknown")

    def __repr__(self):
        return f"<Transaction {self.hash}>"
