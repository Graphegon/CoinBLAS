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
        self.value = value

    @property
    def addresses(self):
        from .address import Address

        r = self.chain.OR[self.id, :]
        if not r.nvals:
            r = self.chain.SI[:, self.id]
            if not r.nvals:
                return []
        return [Address(self.chain, a_id) for a_id, _ in r]

    @property
    def coinbase(self):
        return self.id == get_block_id(self.id)

    @property
    def tx(self):
        from .tx import Tx

        return Tx(self.chain, id=get_tx_id(self.id))

    @property
    def spent_tx(self):
        from .tx import Tx

        spent = self.chain.IT[self.id]
        if spent.nvals:
            return Tx(self.chain, id=spent.to_lists()[0][0])

    def __repr__(self):
        return f"<{self.__class__.__name__}: {self.addresses} value: {btc(self.value)}>"
