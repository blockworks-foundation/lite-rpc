
CREATE SCHEMA benchrunner;


CREATE TABLE benchrunner.bench_runs (
                                        tenant text NOT NULL,
                                        ts timestamp NOT NULL,
                                        status text NOT NULL,
                                        PRIMARY KEY (tenant, ts)
);


CREATE TABLE benchrunner.bench_metrics_bench1 (
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


CREATE TABLE benchrunner.bench_metrics_confirmation_rate (
   tenant text NOT NULL,
   ts timestamp NOT NULL,
   prio_fees int8 NOT NULL,
   txs_sent int8 NOT NULL,
   txs_confirmed int8 NOT NULL,
   txs_un_confirmed int8 NOT NULL,
   average_confirmation_time_ms real NOT NULL,
   average_slot_confirmation_time_ms real NOT NULL,
   metric_json jsonb NOT NULL,
   PRIMARY KEY (tenant, ts)
);


GRANT USAGE ON SCHEMA benchrunner TO r_benchrunner;
GRANT USAGE ON SCHEMA benchrunner TO ro_benchrunner;

GRANT SELECT, INSERT, UPDATE ON ALL TABLES IN SCHEMA benchrunner TO r_benchrunner;
ALTER DEFAULT PRIVILEGES IN SCHEMA benchrunner GRANT SELECT, INSERT, UPDATE ON TABLES TO r_benchrunner;

GRANT SELECT ON ALL TABLES IN SCHEMA benchrunner TO ro_benchrunner;
ALTER DEFAULT PRIVILEGES IN SCHEMA benchrunner GRANT SELECT ON TABLES TO ro_benchrunner;

ALTER TABLE benchrunner.bench_metrics RENAME TO bench_metrics_bench1;

ALTER TABLE benchrunner.bench_metrics_confirmation_rate ADD COLUMN average_slot_confirmation_time real;

ALTER TABLE benchrunner.bench_metrics_confirmation_rate DROP COLUMN average_slot_confirmation_time_ms;
