-- postgresql permission schema for Lite RPC persistence

-- YOU NEED TO adjust the script

-- create role and user; role is defined in code as LITERPC_ROLE constant
CREATE ROLE r_literpc;
CREATE USER literpc_app IN GROUP r_literpc;
-- ALTER USER literpc_app PASSWORD 'secret'; -- TODO provide your authentication

-- required for postgres_logger
GRANT USAGE ON SCHEMA lite_rpc TO r_literpc;
GRANT ALL ON ALL TABLES IN SCHEMA lite_rpc TO r_literpc;
GRANT ALL ON ALL SEQUENCES IN SCHEMA lite_rpc TO r_literpc;
ALTER DEFAULT PRIVILEGES IN SCHEMA lite_rpc GRANT SELECT ON TABLES TO r_literpc;

-- required for block persistence (dynamic schemata - one per epoch)
GRANT CONNECT, CREATE ON DATABASE my_literpc_database TO r_literpc; -- TODO adjust database name

-- query path
CREATE ROLE ro_literpc;
GRANT ro_literpc TO literpc_app;

GRANT CONNECT ON DATABASE literpc_integrationtest TO ro_literpc; -- TODO adjust database name

-- required for benchrunner-service
CREATE ROLE r_benchrunner;
GRANT r_benchrunner TO literpc_app;
