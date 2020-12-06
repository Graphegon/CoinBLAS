import sys
from .bitcoin import *

if __name__ == '__main__':
    b = BitcoinLoader('host=db dbname=coinblas user=postgres password=postgres')
    b.load_graph(sys.argv[1], sys.argv[2])
