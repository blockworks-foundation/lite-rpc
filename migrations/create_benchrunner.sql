
CREATE SCHEMA benchrunner;

CREATE TABLE benchrunner.bench_metrics (
   ts timestamp NOT NULL PRIMARY KEY,
   txs_sent int8 NOT NULL,
   txs_confirmed int8 NOT NULL,
   txs_un_confirmed int8 NOT NULL,
   average_confirmation_time_ms real NOT NULL,
   metric_json jsonb NOT NULL
);

GRANT USAGE ON SCHEMA benchrunner TO r_benchrunner;
GRANT SELECT, INSERT ON ALL TABLES IN SCHEMA benchrunner TO r_benchrunner;
