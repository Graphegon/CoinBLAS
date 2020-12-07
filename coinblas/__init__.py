def get_tid(bn, index):
    assert index < 131072
    return (bn << 17) + index


def get_tid_block(id):
    return id >> 17


def get_id(tid, index):
    assert index < 65536
    return (tid << 16) + index


def get_id_block(id):
    return get_tid_block(id) >> 16

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
