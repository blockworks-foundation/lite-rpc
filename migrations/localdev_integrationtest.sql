
CREATE DATABASE literpc_integrationtest_localdev;
CREATE USER literpc_integrationtest;
ALTER DATABASE literpc_integrationtest_localdev OWNER TO literpc_integrationtest;
ALTER USER literpc_integrationtest PASSWORD 'youknowme';

-- now apply the permissions.sql
