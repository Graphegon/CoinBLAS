from coinblas.util import query, curse, get_block_number, lazy_property


class Block:

    def __init__(self, chain, number):
        self.chain = chain
        self.number = number
        self.id = number << 32

    @classmethod
    def from_id(cls, chain, id):
        return Block(chain, get_block_number(id))

    @lazy_property
    @curse
    @query
    def timestamp(self, curs):
        """
        SELECT b_timestamp FROM bitcoin.block WHERE b_number  = {self.number}
        """
        return curs.fetchone()[0]

    @lazy_property
    def tx_vector(self):
        return self.chain.BT[self.id,:]

    def __iter__(self):
        from .tx import Tx
        for t_id, _ in self.tx_vector:
            yield Tx(self.chain, id=t_id)

    def __repr__(self):
        return f"<Block number: {self.number} txs: {self.tx_vector.nvals}>"
