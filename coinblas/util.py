from itertools import zip_longest
from functools import wraps
from textwrap import dedent
from pygraphblas import Matrix, Vector
from lazy_property import LazyWritableProperty as lazy

GxB_INDEX_MAX = 1 << 60


def get_tx_id(id):
    return (id >> 16) << 16


def get_block_number(id):
    return id >> 32


def get_block_id(id):
    return get_block_number(id) << 32


def grouper(iterable, n, fillvalue=None):
    "Collect data into fixed-length chunks or blocks"
    args = [iter(iterable)] * n
    return zip_longest(*args, fillvalue=fillvalue)


def maximal_matrix(T):
    return Matrix.sparse(T, GxB_INDEX_MAX, GxB_INDEX_MAX)


def maximal_vector(T):
    return Vector.sparse(T, GxB_INDEX_MAX)


def btc(value):
    return float(value / 100000000)


def query(f):
    d = dedent(f.__doc__.split("\n", 1)[1])
    doc_query = (
        "\n".join(line for line in d.split("\n") if line.startswith("    ")) or d
    )
    arg_count = doc_query.count("%s")

    @wraps(f)
    def wrapper(self, cursor, *args, **kwargs):
        params = args[:arg_count]
        kw2 = kwargs.copy()
        kw2["self"] = self
        query = eval("""f'''""" + doc_query + """'''""", kw2)
        cursor.execute(query, params or None)
        return f(self, cursor, *args[arg_count:], **kwargs)

    return wrapper


def curse(func):
    def _decorator(self, *args, **kwargs):
        with self.chain.conn.cursor() as curs:
            return func(self, curs, *args, **kwargs)

    return _decorator
