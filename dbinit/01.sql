CREATE DATABASE coinblas;
\c coinblas

BEGIN;

CREATE EXTENSION hll;

CREATE SCHEMA bitcoin;

-- Block

CREATE TABLE bitcoin.base_block(
    b_number INTEGER PRIMARY KEY,
    b_hash TEXT NOT NULL,
    b_timestamp timestamptz NOT NULL,
    b_timestamp_month date NOT NULL,
    b_imported_at timestamptz,
    b_addresses hll
    );

CREATE INDEX ON bitcoin.base_block (b_hash);

CREATE INDEX ON bitcoin.base_block (b_timestamp);

CREATE INDEX ON bitcoin.base_block (b_imported_at);

-- Tx

CREATE VIEW bitcoin.block AS
    SELECT * FROM bitcoin.base_block
    WHERE b_imported_at IS NOT null;

CREATE TABLE bitcoin.base_tx(
       t_id BIGINT,
       t_hash TEXT NOT NULL
       ) PARTITION BY RANGE (t_id);

ALTER TABLE ONLY bitcoin.base_tx
        ADD PRIMARY KEY (t_id);

ALTER TABLE bitcoin.base_tx
        ALTER COLUMN t_id SET STATISTICS 1000;

ALTER TABLE bitcoin.base_tx
        ALTER COLUMN t_hash SET STORAGE plain;

CREATE VIEW bitcoin.tx AS
    SELECT
        t_hash,
        t_id,
        (t_id >> 32)::integer AS b_number
    FROM bitcoin.base_tx;

CREATE INDEX base_tx_t_hash
    ON ONLY bitcoin.base_tx (t_hash);

CREATE INDEX base_tx_b_number
    ON ONLY bitcoin.base_tx
    USING brin(((t_id >> 32)::integer))
    WITH (pages_per_range = 32, autosummarize = on);

-- Output

CREATE TABLE bitcoin.base_output(
       o_id bigint,
       o_address TEXT NOT NULL
       ) PARTITION BY RANGE (o_id);

ALTER TABLE bitcoin.base_output
        ALTER COLUMN o_id SET STATISTICS 1000;

ALTER TABLE bitcoin.base_output
    ALTER COLUMN o_address SET STORAGE plain;

CREATE INDEX base_output_o_id
    ON ONLY bitcoin.base_output
    USING brin(o_id)
    WITH (pages_per_range = 32, autosummarize = on);

CREATE INDEX base_output_o_address
    ON ONLY bitcoin.base_output (o_address);

CREATE INDEX base_output_b_number
    ON ONLY bitcoin.base_output
    USING brin(((o_id >> 32)::integer))
    WITH (pages_per_range = 32, autosummarize = on);

CREATE INDEX base_output_t_id
    ON ONLY bitcoin.base_output
    USING brin(((o_id >> 16) << 16))
    WITH (pages_per_range = 32, autosummarize = on);

CREATE VIEW bitcoin.output AS
    SELECT
        o_address,
        o_id,
        (o_id >> 16) << 16 AS t_id,
        (o_id >> 32)::integer AS b_number
    FROM bitcoin.base_output;

-- Partitioning

CREATE OR REPLACE PROCEDURE bitcoin.create_month(timestamp_month date)
    LANGUAGE plpgsql AS
$$
DECLARE
    min_id bigint;
    max_id bigint;
    name text = replace(timestamp_month::text, '-', '_');

BEGIN
    SELECT min(b_number), max(b_number)
    INTO min_id, max_id
    FROM bitcoin.base_block
    WHERE b_timestamp_month = timestamp_month;

    min_id = min_id << 32;
    max_id = ((max_id + 1) << 32) - 1;

    EXECUTE format($i$
        CREATE TABLE IF NOT EXISTS bitcoin."base_tx_%1$s"
        (LIKE bitcoin.base_tx INCLUDING DEFAULTS INCLUDING CONSTRAINTS);

        ALTER TABLE bitcoin."base_tx_%1$s"
            ADD CONSTRAINT "base_tx_%1$s_t_id_check"
            CHECK (t_id >= %2$L AND t_id < %3$L );

        CREATE TABLE IF NOT EXISTS bitcoin."base_output_%1$s"
        (LIKE bitcoin.base_output INCLUDING DEFAULTS INCLUDING CONSTRAINTS);

        ALTER TABLE bitcoin."base_output_%1$s"
            ADD CONSTRAINT "base_output_%1$s_o_id_check"
            CHECK (o_id >= %2$L AND o_id < %3$L );

        $i$, name, min_id, max_id);
END;
$$;

CREATE OR REPLACE PROCEDURE bitcoin.index_and_attach(timestamp_month date)
    LANGUAGE plpgsql AS
$$
DECLARE
    min_id bigint;
    max_id bigint;
    name text = replace(timestamp_month::text, '-', '_');

BEGIN
    SELECT min(b_number), max(b_number)
    INTO min_id, max_id
    FROM bitcoin.base_block
    WHERE b_timestamp_month = timestamp_month;

    min_id = min_id << 32;
    max_id = ((max_id + 1) << 32) - 1;

    EXECUTE format($i$

        -- Tx indexing

        ALTER TABLE bitcoin."base_tx_%1$s"
            ADD PRIMARY KEY (t_id);

        CREATE INDEX "base_tx_%1$s_t_hash"
            ON bitcoin."base_tx_%1$s"
            USING btree(t_hash);

        CREATE INDEX "base_tx_%1$s_b_number"
            ON bitcoin."base_tx_%1$s"
            USING brin(((t_id >> 32)::integer))
            WITH (pages_per_range = 32, autosummarize = on);

        -- Output indexing

        CREATE INDEX "base_output_%1$s_o_address"
            ON bitcoin."base_output_%1$s"
            USING btree(o_address);

        CREATE INDEX "base_output_%1$s_o_id"
            ON bitcoin."base_output_%1$s"
            USING brin(o_id)
            WITH (pages_per_range = 32, autosummarize = on);

        CREATE INDEX "base_output_%1$s_b_number"
            ON bitcoin."base_output_%1$s"
            USING brin(((o_id >> 32)::integer))
            WITH (pages_per_range = 32, autosummarize = on);

        CREATE INDEX "base_output_%1$s_t_id"
             ON bitcoin."base_output_%1$s"
            USING brin(((o_id >> 16) << 16))
            WITH (pages_per_range = 32, autosummarize = on);

        -- Attach partitions

        ALTER TABLE bitcoin.base_tx
            ATTACH PARTITION bitcoin."base_tx_%1$s"
        FOR VALUES FROM (%2$L) TO (%3$L);

        ALTER TABLE bitcoin.base_output
            ATTACH PARTITION bitcoin."base_output_%1$s"
        FOR VALUES FROM (%2$L) TO (%3$L);

        ALTER INDEX bitcoin.base_tx_t_hash
            ATTACH PARTITION bitcoin."base_tx_%1$s_t_hash";

        ALTER INDEX bitcoin.base_tx_b_number
            ATTACH PARTITION bitcoin."base_tx_%1$s_b_number";

        ALTER INDEX bitcoin.base_output_o_id
            ATTACH PARTITION bitcoin."base_output_%1$s_o_id";

        ALTER INDEX bitcoin.base_output_o_address
            ATTACH PARTITION bitcoin."base_output_%1$s_o_address";

        ALTER INDEX bitcoin.base_output_b_number
            ATTACH PARTITION bitcoin."base_output_%1$s_b_number";

        ALTER INDEX bitcoin.base_output_t_id
            ATTACH PARTITION bitcoin."base_output_%1$s_t_id";

    $i$, name, min_id, max_id);
END;
$$;

COMMIT;
