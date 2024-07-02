

# Setup
### Hardware
Hardware: recommend 1024MB RAM, 2 vCPUs, small disk


### Environment Variables
| Environment Variable | Purpose                                               | Required?    | Default Value                              |
|----------------------|-------------------------------------------------------|--------------|--------------------------------------------|
| `PG_ENABLED`         | Enable writing to PostgreSQL                          | No           | false                                      |
| `PG_CONFIG`          | PostgreSQL connection string                          | if PG_ENABLED |                                            |
| `TENANT1_ID`         | Technical ID for the tenant                           | Yes          |                                            |
| `TENANT1_RPC_ADDR`   | RPC address for the target RPC node                   | Yes          |                                            |
| `TENANT1_TX_STATUS_WS_ADDR`   | Websocket source for tx status               |No            | RPC Url<br/>(replacing schema with ws/wss) |
| `TENANT2_..          | more tenants can be added using TENANT2, TENANT3, ... |              |                                            |

### Command-line Arguments
```
Options:
  -p, --payer-path <PAYER_PATH>
          
  -r, --rpc-url <RPC_URL>
          
  -w, --tx-status-websocket-addr <TX_STATUS_WEBSOCKET_ADDR>
          Set websocket source (blockSubscribe method) for transaction status updates. You might want to send tx to one RPC and listen to another (reliable) RPC for status updates. Not all RPC nodes support this method. If not provided, the RPC URL is used to derive the websocket URL
  -s, --size-tx <SIZE_TX>
          [possible values: small, large]
  -m, --max-timeout-ms <MAX_TIMEOUT_MS>
          Maximum confirmation time in milliseconds. After this, the txn is considered unconfirmed [default: 15000]
  -t, --txs-per-run <TXS_PER_RUN>
          
  -n, --num-of-runs <NUM_OF_RUNS>
          
  -f, --cu-price <CU_PRICE>
          The CU price in micro lamports [default: 300]
```

```bash
solana-lite-rpc-benchrunner-service \
  --bench-interval 600000 \
  --tx-count 100 \
  --prio-fees 0 --prio-fees 1000 --prio-fees 100000
```
