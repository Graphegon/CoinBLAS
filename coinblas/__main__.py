import sys
import pickle

from pygraphblas import *


if __name__ == '__main__':
    from .bitcoin import Bitcoin
    b = Bitcoin()
    b.build()

# exposure(Aa, '18Crvj4VCgZNN7eZywoPcak7C5ZwwiCSsw', '1JHEi5gVN6eT5FeLk4D5oNiMeSUnD3ntis')
