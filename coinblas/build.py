import sys
from .bitcoin import *

if __name__ == '__main__':
    path = None
    if len(sys.argv) > 1:
        path = sys.argv[1]
    b = BitcoinLoader(path)

