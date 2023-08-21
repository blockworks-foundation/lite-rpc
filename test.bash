#!/bin/bash

# kill background jobs on exit/failure
trap 'kill $(jobs -pr)' SIGINT SIGTERM EXIT

echo "Switching to local lite-rpc rpc config"
solana config set --url "http://0.0.0.0:8899"

echo "Starting the test validator"
solana-test-validator &

echo "Air Dropping 10000 sol" 
sleep 20 && solana airdrop 10000

echo "Starting LiteRpc"
cargo run &

echo "Waiting 20s for startup"
sleep 20

echo "Running yarn tests"
yarn test

echo "Done. Killing background jobs"
kill "$(jobs -p)" || true 
