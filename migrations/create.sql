CREATE TABLE lite_rpc.Txs (
  id SERIAL NOT NULL PRIMARY KEY,
  signature BINARY(64) NOT NULL,
  recent_slot BIGINT NOT NULL,
  forwarded_slot BIGINT NOT NULL,
  processed_slot BIGINT,
  cu_consumed BIGINT,
  cu_requested BIGINT,
  quic_response CHAR
);


CREATE TABLE lite_rpc.Blocks (
  slot BIGINT NOT NULL PRIMARY KEY,
  leader_id BIGINT NOT NULL,
  parent_slot BIGINT NOT NULL
);

CREATE TABLE lite_rpc.AccountAddrs (
  id SERIAL PRIMARY KEY,
  addr VARCHAR(45) NOT NULL
);
