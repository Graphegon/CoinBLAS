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

CREATE TABLE bitcoin.tx(
       t_id BIGINT,
       t_hash TEXT NOT NULL,
       b_timestamp_month date NOT NULL,
       PRIMARY KEY (t_hash, b_timestamp_month)
       ) PARTITION BY LIST (b_timestamp_month)
       ;

CREATE INDEX ON bitcoin.tx (t_hash) WITH (fillfactor=100)
$up$,
$down$
DROP SCHEMA bitcoin CASCADE
$down$);


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
    FOR i IN (SELECT * FROM generate_series((args->>'start')::date, (args->>'end')::date, interval '1 month')) LOOP
        EXECUTE format($i$
	CREATE UNLOGGED TABLE bitcoin."tx_%s"
	PARTITION OF bitcoin.tx
	FOR VALUES IN (%L)
	WITH (fillfactor=100)
	$i$, i, i);
    END LOOP
$up$,
    up_declare:='i date'
    );

CALL metagration.run(args:=jsonb_build_object('start', '2009-01-01', 'end', '2020-11-01'));
