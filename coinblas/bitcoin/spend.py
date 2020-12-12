from lazy_property import LazyWritableProperty as lazy

from coinblas.util import (
    btc,
    curse,
    get_block_id,
    get_tx_id,
    query,
)


class Spend:
    def __init__(self, chain, id, value):
        self.chain = chain
        self.id = id
        self.t_id = get_tx_id(self.id)
        self.value = value

    @lazy
    def coinbase(self):
        return self.id == get_block_id(self.id)

    @lazy
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

    @lazy
    def tx(self):
        from .tx import Tx

        return Tx(self.chain, id=self.t_id)

    @lazy
    def spent_vector(self):
        return self.chain.IT[self.id, :]

    @lazy
    def spent(self):
        from .tx import Tx

        if self.spent_vector:
            return Tx(self.chain, id=self.spent_vector.to_lists()[0][0])

    def __repr__(self):
        return f"<{self.__class__.__name__}: {self.address} value: {btc(self.value)}>"


class Exposure(Spend):
    pass
