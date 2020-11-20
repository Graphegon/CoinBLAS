from pygraphblas import *
from collections import defaultdict
from google.cloud import bigquery


GxB_INDEX_MAX = 1 << 60


def maximal_matrix(T):
    return Matrix.sparse(T, GxB_INDEX_MAX, GxB_INDEX_MAX)


class Object:
    def __init__(self, d):
        self.__dict__ = dict(d)
        

class Bitcoin:

    def __init__(self):
        self.blocks = defaultdict(int)
        self.transactions = {}
        self.tids = {}
        self.senders = defaultdict(lambda: Vector.sparse(BOOL, GxB_INDEX_MAX))
        self.receivers = defaultdict(lambda: Vector.sparse(BOOL, GxB_INDEX_MAX))
        self.sender_ids = {}
        self.receiver_ids = {}
        self.sender_value = maximal_matrix(UINT64)
        self.receiver_value = maximal_matrix(UINT64)

    def get_tid(self, bn, index):
        assert index < 131072
        return (bn << 17) + index

    def get_tid_block(self, id):
        return id >> 17

    def get_id(self, tid, index):
        assert index < 65536
        return (tid << 16) + index

    def get_id_block(self, id):
        return self.get_tid_block(id) >> 16

    def write_block_files(self, bn, S, R):
        S.to_binfile(f'blocks/Sv_block_{bn}.ssb'.encode('utf8'))
        R.to_binfile(f'blocks/Rv_block_{bn}.ssb'.encode('utf8'))
    
    def transaction_summary(self, t):
        tid = self.transactions[t]
        sv = self.sender_value[:,tid]
        rv = self.receiver_value[:,tid]
        print('Senders')
        for sid, value in sv:
            print(f'    Input address {self.sender_ids[sid]}: {value}')
        print('Receivers')
        for rid, value in rv:
            print(f'    Output address {self.receiver_ids[rid]}: {value}')

    def adjacency(self):
        return self.sender_value @ self.receiver_value

    def flow(self, A, debug=False, sring=semiring.PLUS_PLUS, accum=binaryop.PLUS):
        M = self.sender_value @ self.receiver_value
        v = Vector.sparse(UINT64, M.nrows)
        sids = self.senders[A]
        v.assign_scalar(0, mask=sids)
        with sring, Accum(accum):
            for level in range(M.nrows):
                w = v.dup()
                if debug:
                    ids, vals = v.to_lists()
                    for di, dv in zip(ids, vals):
                        diblock = self.get_id_block(di)
                        rid = self.receiver_ids[di]
                        print(f"{'  '*level}{diblock}: {rid}: {dv}")
                v @= M
                if w.iseq(v):
                    break
        return v

    def exposure(M, A, B):
        f = flow(M, A)
        rids = receivers[B]
        return f[rids]

    def build(self, block_depth=100):
        bn = 0
        Sv = maximal_matrix(UINT64)
        Rv = maximal_matrix(UINT64)
        client = bigquery.Client()

        bn = None
        th = None
        ii = None
        oi = None
        tid = None

        inputs = set()
        outputs = set()

        query = f"""\
        SELECT `hash`,
            block_timestamp_month,
            block_number,
            block_hash,
            input_count,
            i.spent_transaction_hash as spent_hash,
            i.spent_output_index as spent_index,
            i.index as iindex,
            i.addresses as iaddress,
            i.value as ivalue,
            o.index as oindex,
            o.addresses as oaddress,
            o.value as ovalue
        FROM `bigquery-public-data.crypto_bitcoin.transactions`,
        UNNEST (inputs) as i,
        UNNEST (outputs) as o
        WHERE block_timestamp_month = '2020-11-01'
        AND block_number >= (
            SELECT max(number) - {block_depth} 
            FROM `bigquery-public-data.crypto_bitcoin.blocks`
        )
        ORDER BY block_number, `hash`, i.index, o.index
        """

        for t in map(Object, client.query(query)):

            if t.block_number != bn:
                if bn is not None:
                    print(f'Writing block files for block {bn}')
                    self.write_block_files(bn, Sv, Rv)
                    self.sender_value += Sv
                    self.receiver_value += Rv
                    Sv.clear()
                    Rv.clear()
                print(f'Starting block {t.block_number}')

            bn = t.block_number

            if t.hash != th:
                self.blocks[bn] += 1
                tid = self.get_tid(bn, self.blocks[bn])
                self.transactions[t.hash] = tid
                self.tids[tid] = t.hash
                print(f'Starting transaction {t.hash}')
                inputs.clear()
                outputs.clear()

            th = t.hash

            if t.iindex not in inputs:
                stid = self.transactions.get(t.spent_hash, tid)
                iid = self.get_id(stid, t.spent_index)
                for ia in t.iaddress:
                    self.senders[ia][iid] = True
                ia = t.iaddress[0]
                self.sender_ids[iid] = ia
                Sv[iid, tid] = t.ivalue
                inputs.add(t.iindex)
                print(f'Added sender {ia}')

            if t.oindex not in outputs:
                oid = self.get_id(tid, t.oindex)
                for oa in t.oaddress:
                    self.receivers[oa][oid] = True
                oa = t.oaddress[0]
                self.receiver_ids[oid] = oa
                Rv[tid, oid] = t.ovalue
                outputs.add(t.oindex)
                print(f'Added receiver {oa}')

        self.write_block_files(bn, Sv, Rv)
        self.sender_value += Sv
        self.receiver_value += Rv

    def __getstate__(self):
        return dict(
            blocks=self.blocks,
            transactions=self.transactions,
            tids=self.tids,
            senders={k: v.to_lists() for k, v in senders.items()},
            receivers={k: v.to_lists() for k, v in receivers.items()},
            sender_ids=sender_ids,
            receiver_ids=receiver_ids
        )
    
