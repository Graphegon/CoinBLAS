from coinblas.util import query, curse, get_tx_id, get_block_id, lazy_property, btc


class Spend:
    def __init__(self, chain, id, value):
        self.chain = chain
        self.id = id
        self.t_id = get_tx_id(self.id)
        self.value = value

    @lazy_property
    def coinbase(self):
        return self.id == get_block_id(self.id)

    @lazy_property
    @curse
    @query
    def address(self, curs):
        """
        SELECT a_address from bitcoin.address where a_id = {self.id}
        """
        from .address import Address

        r = curs.fetchone()
        if r is None:
            return
        return Address(self.chain, r[0])

    @lazy_property
    def tx(self):
        from .tx import Tx

        return Tx(self.chain, id=self.t_id)

    @lazy_property
    def spent_vector(self):
        return self.chain.IT[self.id, :]

    @lazy_property
    def spent(self):
        from .tx import Tx

        if self.spent_vector:
            return Tx(self.chain, id=self.spent_vector.to_lists()[0][0])

    def __repr__(self):
        return f"<{self.__class__.__name__}: {self.address.address} value: {btc(self.value)}>"


class Exposure(Spend):
    pass
