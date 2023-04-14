#!/bin/bash

# kill background jobs on exit/failure
trap 'kill $(jobs -pr)' SIGINT SIGTERM EXIT

echo "Doing an early build"
cargo build --workspace --tests 
yarn

echo "Switching to local lite-rpc rpc config"
solana config set --url "http://0.0.0.0:8899"

echo "Starting the test validator"
solana-test-validator &

echo "Air Dropping 10000 sol" 
sleep 20 && solana airdrop 10000

echo "Starting LiteRpc"
cargo run &

echo "Running cargo tests in 20s"
sleep 20 && cargo test

echo "Running yarn tests"
yarn test

echo "Done. Killing background jobs"
kill "$(jobs -p)" || true 
