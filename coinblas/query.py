    def transaction_summary(self, t):
        print(f'Summary for {t}')
        tid = self.state.transactions[t]
        sv = self.state.Sv[:,tid]
        rv = self.state.Rv[tid,:]
        print('Senders')
        for sid, value in sv:
            print(f'    Input address {self.state.sender_ids[sid]}: {btc(value)}')
        print('Receivers')
        for rid, value in rv:
            print(f'    Output address {self.state.receiver_ids[rid]}: {btc(value)}')

    def flow(self, start, debug=False):
        if debug:
            print(f'Tracing {start}')
            
        with semiring.PLUS_SECOND:
            Av = self.state.Sv @ self.state.Rv

        v = self.state.senders[start].dup()
        v.assign_scalar(0, mask=v)
        
        for level in range(Av.nrows):
            w = v.dup()
            with semiring.MIN_PLUS, Accum(binaryop.MIN):
                v @= Av
            
            if w.iseq(v):
                break
            if debug:
                ids, vals = v.extract(w, desc=descriptor.C).to_lists()
                for di, dv in zip(ids, vals):
                    diblock = self.get_id_block(di)
                    rid = self.state.receiver_ids.get(di)
                    dt = self.state.Sv[di].reduce_int()
                    thash = self.state.tids[self.state.Rv[:,di].to_lists()[0][0]]
                    if rid:
                        print(f"{'  '*level}{btc(dv):.8f} to {rid} via {thash} block {diblock}")
        return v

    def exposure(self, A, B, debug=False):
        return self.flow(A, debug=debug)[self.state.receivers[B]]

