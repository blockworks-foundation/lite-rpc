CREATE SCHEMA lite_rpc;

CREATE TABLE lite_rpc.Txs (
  id SERIAL NOT NULL PRIMARY KEY,
  signature CHAR(88) NOT NULL,
  recent_slot BIGINT NOT NULL,
  forwarded_slot BIGINT NOT NULL,
  forwarded_local_time TIMESTAMP WITH TIME ZONE NOT NULL,
  processed_slot BIGINT,
  processed_cluster_time TIMESTAMP WITH TIME ZONE,
  processed_local_time TIMESTAMP WITH TIME ZONE,
  cu_consumed BIGINT,
  cu_requested BIGINT,
  quic_response SMALLINT
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
