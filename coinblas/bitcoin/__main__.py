import sys

if __name__ == "__main__":
    from . import Bitcoin

    g = Bitcoin(
        "host=db dbname=coinblas user=postgres password=postgres",
        "/home/jovyan/coinblas/database-blocks",
        sys.argv[-3],
        sys.argv[-2],
        int(sys.argv[-1]),
    )

    g.load_graph(sys.argv[-3], sys.argv[-2])
