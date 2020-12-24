import os, logging

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
    parser.add_argument(
        "--log-level",
        default=os.getenv("COINBLAS_LOG_LEVEL", "INFO"),
        help="Log level.",
    )
    args = parser.parse_args()

    from coinblas.bitcoin import Chain, logger
    from coinblas.util import *

    logger.setLevel(getattr(logging, args.log_level.upper()))

    btc = Chain(args.db, args.block_path, args.pool_size)

    if args.mode == "init":
        btc.initialize_blocks()
        if args.start_date and args.end_date:
            btc.import_blocktime(args.start_date, args.end_date)

    elif args.mode == "import":
        btc.import_blocktime(args.start_date, args.end_date)

    elif args.mode == "query":
        import IPython

        if args.start and args.end:
            btc.load_blockspan(args.start, args.end)
        elif args.start_date and args.end_date:
            btc.load_blocktime(args.start_date, args.end_date)
        IPython.embed()

    elif args.mode == "summary":
        print(btc.summary)
