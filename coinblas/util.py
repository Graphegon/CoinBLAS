import re
from itertools import islice, chain, zip_longest
from functools import update_wrapper
from weakref import WeakKeyDictionary
from functools import wraps
from textwrap import dedent
from hashlib import sha256
from base64 import b32encode
from pygraphblas import Matrix, Vector
import psycopg2 as pg

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


_re_format = re.compile(r"%\(([a-z]+)\)[a-z]")
_re_identifier = re.compile(r"^[_a-z][_a-z0-9]*$", re.IGNORECASE)
_statements = WeakKeyDictionary()


def query(f):
    """Decorator that prepares an SQL statement.

    The statement must be present in the function's docstring,
    indented with four spaces, i.e.

        SELECT * FROM %(name)s WHERE value > $1

    The formatting variables are pulled from the function bind's
    instance dictionary.

    The placeholders (with dollar sign) are positional arguments to
    the decorated function. These are automatically applied.
    """

    d = dedent(f.__doc__.split("\n", 1)[1])
    doc_query = "\n".join(line for line in d.split("\n") if line.startswith("    ")) or d
    arg_count = doc_query.count("%s")
    @wraps(f)
    def wrapper(self, cursor, *args, **kwargs):
        conn = cursor.connection
        params = args[:arg_count]
        query = eval("""f'''"""+doc_query+"""'''""", dict(self=self))
        cursor.execute(query, params or None)
        return f(self, cursor, *args[arg_count:], **kwargs)

    return wrapper


def unique_identifier(name, prefix="pq_"):
    """Create a unique identifier from a name and a (non-empty) prefix.
    ValueError is raised if prefix is not a valid postgresql identifier of maximum length 11 characters.
    """

    if len(prefix) > 11 or not _re_identifier.match(prefix):
        raise ValueError(prefix)

    # b32encode of 256 bits (32 bytes) is 56 bytes, but the final 4 are always padding ('====')
    b32_digest = b32encode(sha256(name.encode()).digest())[:52]

    return prefix + b32_digest.decode()


def curse(func):
    def _decorator(self, *args, **kwargs):
        with self.chain.conn.cursor() as curs:
            return func(self, curs, *args, **kwargs)

    return _decorator


class lazy_property:
    name = None

    @staticmethod
    def func(instance):
        raise TypeError(
            'Cannot use lazy_property instance without calling '
            '__set_name__() on it.'
        )

    def __init__(self, func, name=None):
        self.real_func = func
        self.__doc__ = getattr(func, '__doc__')

    def __set_name__(self, owner, name):
        if self.name is None:
            self.name = name
            self.func = self.real_func
        elif name != self.name:
            raise TypeError(
                "Cannot assign the same lazy_property to two different names "
                "(%r and %r)." % (self.name, name)
            )

    def __get__(self, instance, cls=None):
        if instance is None:
            return self
        res = instance.__dict__[self.name] = self.func(instance)
        return res
