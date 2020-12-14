import os

if __name__ == "__main__":
    import argparse

    parser = argparse.ArgumentParser(description="CoinBLAS")
    parser.add_argument("mode", default="query", help="query|init|summary")
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

    chain = Bitcoin(args.db, args.block_path, args.pool_size)
    if args.mode == "init":
        chain.initialize_blocks()
        chain.import_blocktime(args.start_date, args.end_date)
    else:
        if args.start and args.end:
            chain.load_blockspan(args.start, args.end)
        elif args.start_date and args.end_date:
            chain.load_blocktime(args.start_date, args.end_date)
    if args.mode == "summary":
        chain.summary()
