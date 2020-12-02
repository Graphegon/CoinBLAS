SELECT metagration.new_script(
$up$
CREATE SCHEMA bitcoin;

CREATE TABLE bitcoin.block(
       b_number INTEGER,
       b_hash TEXT NOT NULL,
       b_timestamp timestamptz NOT NULL,
       b_timestamp_month date NOT NULL,
       PRIMARY KEY (b_number, b_timestamp_month)
       ) PARTITION BY LIST (b_timestamp_month)
       ;

SELECT metagration.new_script(
$up$
    FOR i IN (SELECT * FROM generate_series((args->>'start')::date, (args->>'end')::date, interval '1 month')) LOOP
        EXECUTE format($i$
	CREATE UNLOGGED TABLE bitcoin."block_%s"
	PARTITION OF bitcoin.block
	FOR VALUES IN (%L)
	WITH (fillfactor=100)
	$i$, i, i);
    END LOOP
$up$,
    up_declare:='i date'
    );

SELECT metagration.new_script(
$up$
CREATE TABLE bitcoin.tx(
       t_hash TEXT NOT NULL,
       t_id BIGINT NOT NULL
       ) PARTITION BY LIST (left(t_hash, 1))
       ;
CREATE INDEX ON bitcoin.tx (t_hash) INCLUDE (t_id) WITH (fillfactor=100);
CREATE INDEX ON bitcoin.tx (t_id) INCLUDE (t_hash) WITH (fillfactor=100);
ALTER TABLE bitcoin.tx alter column t_hash set storage plain;

$up$,
$down$
DROP SCHEMA bitcoin CASCADE
$down$);

DO $$
DECLARE
i text;
BEGIN
    FOREACH i IN ARRAY '{0,1,2,3,4,5,6,7,8,9,a,b,c,d,e,f}'::text[] LOOP
        EXECUTE format($i$
	CREATE TABLE bitcoin."tx_%s"
	PARTITION OF bitcoin.tx
	FOR VALUES IN (%L)
	WITH (fillfactor=100)
	$i$, i, i);
    END LOOP;
END
$$;

$up$,
    up_declare:='i text'
    );

SELECT metagration.new_script(
$up$

CREATE TABLE bitcoin.address(
       a_address TEXT NOT NULL,
       a_id bigint NOT NULL
       ) PARTITION BY HASH (a_address);
ALTER TABLE bitcoin.address alter column a_hash set storage plain;

$up$
);

SELECT metagration.new_script(
$up$
    FOR i IN (SELECT * FROM generate_series(0, 15)) LOOP
        EXECUTE format($i$
	CREATE TABLE bitcoin."address_%s"
	PARTITION OF bitcoin.address
	FOR VALUES with (modulus 16, remainder %s) WITH (fillfactor=100)
	$i$, i, i);
    END LOOP
$up$,
    up_declare:='i date'
    );

DO $$
DECLARE
i integer;
BEGIN
    FOR i IN (SELECT * FROM generate_series(0, 15)) LOOP
        EXECUTE format($i$
	CREATE TABLE bitcoin."address_%s"
	PARTITION OF bitcoin.address
	FOR VALUES with (modulus 16, remainder %s) WITH (fillfactor=100)
	$i$, i, i);
    END LOOP
END
$$;


SELECT metagration.new_script(
$up$

CREATE TABLE bitcoin.graph_log(
       b_number INTEGER,
       b_created timestamptz NOT NULL,
       PRIMARY KEY (b_number)
       )
$up$
);

CALL metagration.run(args:=jsonb_build_object('start', '2009-01-01', 'end', '2020-11-01'));
