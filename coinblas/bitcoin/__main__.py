import os

if __name__ == "__main__":
    import argparse

    parser = argparse.ArgumentParser(description="CoinBLAS")
    parser.add_argument("mode", default="query", help="query|init")
    parser.add_argument("--start", help="Start block number")
    parser.add_argument("--end", help="End block number")
    parser.add_argument("--start-date", help="Start block number")
    parser.add_argument("--end-date", help="End block number")
    parser.add_argument("--pool-size", default="1", type=int, help="Pool Size")
    parser.add_argument("--db", default=os.getenv("COINBLAS_DB"), help="Postgres DB")
    parser.add_argument(
        "--block-path", default=os.getenv("COINBLAS_PATH"), help="Block file Path"
    )
    args = parser.parse_args()

    from coinblas.bitcoin import Bitcoin

    g = Bitcoin(args.db, args.block_path, args.pool_size)
    if args.mode == "init":
        g.initialize_blocks()
        g.import_blocktime(args.start, args.end)
    else:
        if args.start and args.end:
            g.load_blockspan(args.start, args.end)
        elif args.start_date and args.end_date:
            g.load_blocktime(args.start_date, args.end_date)
