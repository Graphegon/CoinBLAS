import sys
from .bitcoin import *

if __name__ == '__main__':
    b = BitcoinLoader('host=db user=postgres password=postgres')
    b.load('2009-01-01', '2020-11-01')

