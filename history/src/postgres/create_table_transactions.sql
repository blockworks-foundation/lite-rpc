-- parameter 'schema' is something like 'rpc2a_epoch_592'
CREATE TABLE IF NOT EXISTS {schema}.transactions(
    signature VARCHAR(88) NOT NULL,
    slot BIGINT NOT NULL,
    err TEXT,
    cu_requested BIGINT,
    prioritization_fees BIGINT,
    cu_consumed BIGINT,
    recent_blockhash TEXT NOT NULL,
    message TEXT NOT NULL,
    CONSTRAINT pk_transaction_sig PRIMARY KEY(signature)
    ) WITH (FILLFACTOR=90);
CREATE INDEX idx_slot ON {schema}.transactions USING btree (slot) WITH (FILLFACTOR=90);
CLUSTER {schema}.transactions USING idx_slot;
