from itertools import islice, chain
from pygraphblas import *


class Object:
    def __init__(self, d):
        self.__dict__ = dict(d)

def btc(value):
    return float(value / 100000000)

        # state.Sv.to_binfile(bytes(b / Path(f'{bhash}_Sv.ssb')))
        # state.Rv.to_binfile(bytes(b / Path(f'{bhash}_Rv.ssb')))
        # state.senders = {k: v.to_lists() for k, v in state.senders.items()}
        # state.receivers = {k: v.to_lists() for k, v in state.receivers.items()}

        # s = {}
        # while len(state.senders):
        #     si = state.senders.popitem()
        #     s[si[0]] = Vector.from_lists(*si[1], GxB_INDEX_MAX, UINT64)
        # state.senders = s

        # r = {}
        # while len(state.receivers):
        #     ri = state.receivers.popitem()
        #     r[ri[0]] = Vector.from_lists(*ri[1], GxB_INDEX_MAX, UINT64)
        # state.senders = r
        
        # state.Sv = Matrix.from_binfile(bytes(path / Path(f'all_Sv.ssb')))
        # state.Rv = Matrix.from_binfile(bytes(path / Path(f'all_Sv.ssb')))

        
def batch(iterable, size):
    sourceiter = iter(iterable)
    while True:
        batchiter = islice(sourceiter, size)
        yield chain([batchiter.next()], batchiter)
        
