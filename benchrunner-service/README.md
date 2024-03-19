

# Setup
### Hardware
Hardware: recommend 1024MB RAM, 2 vCPUs, small disk


### Environment Variables
| Environment Variable | Purpose                                               | Required?     | Default Value |
|----------------------|-------------------------------------------------------|---------------|---------------|
| `PG_ENABLED`         | Enable writing to PostgreSQL                          | No            | false         |
| `PG_CONFIG`          | PostgreSQL connection string                          | if PG_ENABLED |               |
| `TENANT1_ID`         | Technical ID for the tenant                           | Yes           |               |
| `TENANT1_RPC_ADDR`   | RPC address for the target RPC node                   | Yes           |               |
| `TENANT2_..          | more tenants can be added using TENANT2, TENANT3, ... |            |               |

### Command-line Arguments
```
Options:
  -b, --bench-interval <BENCH_INTERVAL>
          interval in milliseconds to run the benchmark [default: 60000]
  -n, --tx-count <TX_COUNT>
          [default: 10]
  -s, --size-tx <SIZE_TX>
          [default: small] [possible values: small, large]
  -p, --prio-fees <PRIO_FEES>
          [default: 0]
```

```bash
solana-lite-rpc-benchrunner-service \
  --bench-interval 600000 \
  --tx-count 100 \
  --prio-fees 0 --prio-fees 1000 --prio-fees 100000
```
