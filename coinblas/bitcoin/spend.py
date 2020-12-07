from coinblas.util import prepared, curse


class Spend:
    def __init__(self, chain, id, value):
        self.chain = chain
        self.id = id
        self.t_id = self.chain.get_tx_id(self.id)
        self.value = value

    @property
    def coinbase(self):
        return self.id == self.chain.get_block_id(self.id)

    @curse
    @prepared
    def address(self, curs):
        """
        SELECT a_address from bitcoin.address where a_id = %(id)s
        """
        from .address import Address

        return Address(self.chain, curs.fetchone()[0])

    @property
    def tx(self):
        from .tx import Tx

        return Tx(self.chain, id=self.t_id)

    @property
    def spent_vector(self):
        return self.chain.Iv[self.id, :]

    @property
    def spent(self):
        return Transaction(self.chain, id=self.spent_vector.to_lists()[0][0])

    def __repr__(self):
        return f"<Spend {self.id}>"
