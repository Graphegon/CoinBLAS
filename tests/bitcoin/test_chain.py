import os
import pytest
import pickle
from google.cloud import bigquery
from pytest_postgresql import factories

from coinblas.bitcoin import Chain, Block, Tx, Address
from pygraphblas import *

postgresql_my_proc = factories.postgresql_noproc(
    host="db", user="postgres", password="postgres"
)

postgresql = factories.postgresql(
    "postgresql_my_proc", db_name="test", load=["/docker-entrypoint-initdb.d/01.sql"]
)

import pytest


@pytest.fixture
def btc(postgresql, mocker, datadir, tmp_path):
    q = mocker.patch("google.cloud.bigquery.Client.query", autospec=True)
    q.return_value = pickle.load(open(datadir / "initialize_blocks.pickle", "rb"))
    c = Chain(
        "host=db dbname=test user=postgres password=postgres",
        tmp_path / "blocks",
        pool_size=1,
    )
    c.initialize_blocks()
    q.return_value = pickle.load(open(datadir / "import_month.pickle", "rb"))
    c.import_month("2012-01-01")
    c.load_blockmonth("2012-01-01")
    return c

def test_pyshell(btc):
    print(btc.summary)
    if os.getenv("COINBLAS_SHELL") == "1":
        import IPython
        IPython.embed()
