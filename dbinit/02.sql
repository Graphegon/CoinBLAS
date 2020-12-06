CREATE SCHEMA bitcoin;

CREATE TABLE bitcoin.block(
       b_number INTEGER PRIMARY KEY,
       b_hash TEXT NOT NULL,
       b_timestamp timestamptz NOT NULL,
       b_timestamp_month date NOT NULL);
       
CREATE INDEX ON bitcoin.block (b_hash);
CREATE INDEX ON bitcoin.block (b_timestamp);

CREATE TABLE bitcoin.tx(
       t_hash TEXT NOT NULL,
       t_id BIGINT NOT NULL
       ) PARTITION BY LIST (left(t_hash, 1));
       
CREATE INDEX ON bitcoin.tx (t_hash) INCLUDE (t_id);
CREATE INDEX ON bitcoin.tx (t_id) INCLUDE (t_hash);

ALTER TABLE bitcoin.tx ALTER COLUMN t_hash SET STORAGE plain;

DO $$
DECLARE
i text;
BEGIN
    FOREACH i IN ARRAY '{0,1,2,3,4,5,6,7,8,9,a,b,c,d,e,f}'::text[] LOOP
        EXECUTE format($i$
	CREATE TABLE bitcoin."tx_%s"
	PARTITION OF bitcoin.tx
	FOR VALUES IN (%L)
	$i$, i, i);
    END LOOP;
END
$$;

CREATE TABLE bitcoin.address(
       a_address TEXT NOT NULL,
       a_id bigint NOT NULL
       ) PARTITION BY HASH (a_address);
ALTER TABLE bitcoin.address ALTER COLUMN a_address SET STORAGE plain;

CREATE INDEX ON bitcoin.address (a_address) INCLUDE (a_id);
CREATE INDEX ON bitcoin.address (a_id) INCLUDE (a_address);

DO $$
DECLARE
i integer;
BEGIN
    FOR i IN (SELECT * FROM generate_series(0, 15)) LOOP
        EXECUTE format($i$
	CREATE TABLE bitcoin."address_%s"
	PARTITION OF bitcoin.address
	FOR VALUES WITH (modulus 16, remainder %s)
	$i$, i, i);
    END LOOP;
END
$$;

