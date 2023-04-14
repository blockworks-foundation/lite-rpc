#!/bin/sh

# kill background jobs on exit/failure
trap 'kill $(jobs -pr)' SIGINT SIGTERM EXIT

# env variables
export PGPASSWORD="password"
export PG_CONFIG="host=localhost dbname=postgres user=postgres password=password sslmode=disable" 

# functions
pg_run() {
    psql -h localhost -U postgres -d postgres -a "$@"
}

# create and start docker
docker create --name test-postgres -e POSTGRES_PASSWORD=password -p 5432:5432 postgres:latest || true
docker start test-postgres

echo "Clearing database"
pg_run -f ../migrations/rm.sql
pg_run -f ../migrations/create.sql

echo "Starting the test validator"
solana-test-validator > /dev/null &

echo "Waiting 8 seconds for solana-test-validator to start"
sleep 8
 
echo "Starting lite-rpc"
cargo run --release -- -p &

echo "Waiting 5 seconds for lite-rpc to start"
sleep 5

echo "Sending 10 txs"
cd ../bench && cargo run --release -- -t 10

echo "Killing lite-rpc"
kill "$(jobs -p)"

echo "Fetching database values"
pg_run -c "SELECT * FROM lite_rpc.txs;"
pg_run -c "SELECT * FROM lite_rpc.blocks;"

