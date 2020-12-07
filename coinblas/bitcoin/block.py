from coinblas.util import prepared, curse


class Block:
    def __init__(self, chain, number):
        self.chain = chain
        self.number = number
        self.id = number << 32

    @curse
    @prepared
    def insert(self, curs):
        """
        INSERT INTO bitcoin.block
        (b_number, b_hash, b_timestamp, b_timestamp_month)
        VALUES ($1, $2, $3, $4)
        """

    @curse
    @prepared
    def timestamp(self, curs):
        """
        SELECT b_timestamp FROM bitcoin.block WHERE b_number  = %(number)s
        """
        return curs.fetchone()[0]

    def __str__(self):
        return f"Block: {self.number} : {self.id}"
