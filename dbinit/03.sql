SELECT metagration.new_script(
$up$

CREATE TABLE bitcoin.address(
       a_id BIGINT,
       a_address TEXT NOT NULL,
       b_timestamp_month date NOT NULL,
       PRIMARY KEY (a_id, b_timestamp_month)
       ) PARTITION BY LIST (b_timestamp_month);
CREATE INDEX ON bitcoin.address (a_address) WITH (fillfactor=100)
$up$
);

SELECT metagration.new_script(
$up$
    FOR i IN (SELECT * FROM generate_series((args->>'start')::date, (args->>'end')::date, interval '1 month')) LOOP
        EXECUTE format($i$
	CREATE UNLOGGED TABLE bitcoin."address_%s"
	PARTITION OF bitcoin.address
	FOR VALUES IN (%L) WITH (fillfactor=100)
	$i$, i, i);
    END LOOP
$up$,
    up_declare:='i date'
    );

CALL metagration.run(args:=jsonb_build_object('start', '2009-01-01', 'end', '2020-11-01'));
