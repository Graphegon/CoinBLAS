from coinblas.util import (
    btc,
    curse,
    get_block_id,
    get_tx_id,
    query,
    lazy,
)


class Relation:
    def __init__(self, chain, id, value):
        self.chain = chain
        self.id = id
        self.value = value

    @lazy
    def coinbase(self):
        return self.id == get_block_id(self.id)

    @property
    def address(self):
        from .address import Address

        r = self.chain.ST[self.id]
        if not r.nvals:
            r = self.chain.OR[:, self.id]
            if not r.nvals:
                return chain.UNKNOWN_ADDRESS
        a_id = r.to_lists()[0][0]
        return Address(self.chain, a_id)

    @property
    def tx(self):
        from .tx import Tx

        return Tx(self.chain, id=get_tx_id(self.t_id))

    @property
    def spent_tx(self):
        from .tx import Tx

        if self.input_tx.nvals:
            return Tx(self.chain, id=self.input_tx.to_lists()[0][0])

    def __repr__(self):
        return f"<{self.__class__.__name__}: {self.address} value: {btc(self.value)}>"


class Input(Relation):
    pass


class Output(Relation):
    pass
