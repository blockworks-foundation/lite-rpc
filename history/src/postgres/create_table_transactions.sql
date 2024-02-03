-- lookup table; maps signatures to generated int8 transaction ids
-- no updates or deletes, only INSERTs
CREATE TABLE {schema}.transaction_ids(
    transaction_id bigserial PRIMARY KEY WITH (FILLFACTOR=90),
    -- never put sig on TOAST
    signature text STORAGE PLAIN NOT NULL,
    UNIQUE(signature)
) WITH (FILLFACTOR=100);

-- parameter 'schema' is something like 'rpc2a_epoch_592'
CREATE TABLE IF NOT EXISTS {schema}.transaction_blockdata(
    -- transaction_id must exist in the transaction_ids table
    transaction_id bigint PRIMARY KEY WITH (FILLFACTOR=90),
    slot bigint NOT NULL,
    cu_requested bigint,
    prioritization_fees bigint,
    cu_consumed bigint,
    recent_blockhash text NOT NULL,
    err text,
    message text NOT NULL
    -- model_transaction_blockdata
) WITH (FILLFACTOR=90,TOAST_TUPLE_TARGET=128);
CREATE INDEX idx_slot ON {schema}.transaction_blockdata USING btree (slot) WITH (FILLFACTOR=90);
