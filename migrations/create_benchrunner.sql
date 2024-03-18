
CREATE SCHEMA benchrunner;

CREATE TABLE benchrunner.bench_metrics (
   tenant text NOT NULL,
   ts timestamp NOT NULL,
   prio_fees int8 NOT NULL,
   txs_sent int8 NOT NULL,
   txs_confirmed int8 NOT NULL,
   txs_un_confirmed int8 NOT NULL,
   average_confirmation_time_ms real NOT NULL,
   metric_json jsonb NOT NULL,
   PRIMARY KEY (tenant, ts)
);

CREATE TABLE benchrunner.bench_runs (
    tenant text NOT NULL,
    ts timestamp NOT NULL,
    status text NOT NULL,
    PRIMARY KEY (tenant, ts)
);

GRANT USAGE ON SCHEMA benchrunner TO r_benchrunner;
GRANT SELECT, INSERT, UPDATE ON ALL TABLES IN SCHEMA benchrunner TO r_benchrunner;
