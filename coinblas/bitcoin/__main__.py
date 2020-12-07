import sys

if __name__ == "__main__":
    from . import Bitcoin

    g = Bitcoin(
        "host=db user=postgres dbname=coinblas password=postgres",
        "/coinblas/blocks/bitcoin",
        sys.argv[1],
        sys.argv[2],
    )
