#!/usr/bin/env bash

cat /docker-entrypoint-initdb.d/00_dev_postgresql.conf >> $PGDATA/postgresql.conf
# cat /docker-entrypoint-initdb.d/00_pg_hba.conf >> $PGDATA/pg_hba.conf

pg_ctl -D $PGDATA restart

