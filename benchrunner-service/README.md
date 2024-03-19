

# Setup
### Hardware
Hardware: recommend 512MB RAM, 1 CPU, small disk


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
Usage: solana-lite-rpc-benchrunner-service [OPTIONS]

Options:
  -b, --bench-interval <BENCH_INTERVAL>
          interval in milliseconds to run the benchmark [default: 60000]
  -n, --tx-count <TX_COUNT>
          [default: 10]
  -s, --size-tx <SIZE_TX>
          [default: small] [possible values: small, large]
  -h, --help
          Print help
  -V, --version
          Print version
```

```bash
--prio-fees 0 --prio-fees 1000 --prio-fees 100000
```
