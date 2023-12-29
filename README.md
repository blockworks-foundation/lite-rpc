# Lite RPC For Solana Blockchain 

Submitting a [transaction](https://docs.solana.com/terminology#transaction) to
be executed on the solana blockchain, requires the client to identify the next
few leaders based on the
[leader schedule](https://docs.solana.com/terminology#leader-schedule), look up
their peering information in gossip and connect to them via the
[quic protocol](https://en.wikipedia.org/wiki/QUIC). In order to simplify the
process so it can be triggered from a web browser, most applications run full
[validators](https://docs.solana.com/terminology#validator) that forward the
transactions according to the protocol on behalf of the web browser. Running
full solana [validators](https://docs.solana.com/terminology#validator) is
incredibly resource intensive `(>256GB RAM)`, the goal of this project would be
to create a specialized micro-service that allows to deploy this logic quickly
and allows [horizontal scalability](https://en.wikipedia.org/wiki/Scalability)
with commodity vms. Optionally the Lite RCP micro-services can be configured to
send the transactions to a complementary __QUIC forward proxy__ instead of the
solana tpu ([details](quic-forward-proxy/README.md)).

### Confirmation strategies

1) Subscribe to new blocks using websockets (deprecated)
2) Polling blocks over RPC.(Current) 
3) Subscribe blocks over gRPC.
(Current) 
4) Listening to gossip protocol. (Future roadmap)

## Executing

*run using*
```bash
$ cargo run --release
```

*to know about command line options*
```bash
$ cargo run --release -- --help
```

## Test and Bench

*Make sure both `solana-validator` and `lite-rpc` is running*

*test*
```bash
$ cargo test
```

*bench*
```bash
$ cd bench and cargo run --release
```

Find a new file named `metrics.csv` in the project root.

## Deployment

### Environment Variables

Thank you for providing the default values. Here's the updated table with the default values for the environment variables based on the additional information:

| Environment Variable                                                       | Purpose                                                  | Required?           | Default Value                                  |
|----------------------------------------------------------------------------|----------------------------------------------------------|---------------------|------------------------------------------------|
| `RPC_ADDR`                                                                 | Address for the RPC node                                 | Replaces default if set | `http://0.0.0.0:8899` (from `DEFAULT_RPC_ADDR`) |
| `WS_ADDR`                                                                  | WebSocket address for the RPC node                       | Replaces default if set | `ws://0.0.0.0:8900` (from `DEFAULT_WS_ADDR`)   |
| `LITE_RPC_HTTP_ADDR`                                                       | HTTP address for the lite RPC node                       | Replaces default if set | `http://0.0.0.0:8890` (from `DEFAULT_LITE_RPC_ADDR`) |
| `LITE_RPC_WS_ADDR`                                                         | WebSocket address for the lite RPC node                  | Replaces default if set | `[::]:8891` (from `Config::default_lite_rpc_ws_addr`) |
| `FANOUT_SIZE`                                                              | Configuration for the fanout size                        | Replaces default if set | `18` (from `DEFAULT_FANOUT_SIZE`)             |
| `IDENTITY`                                                                 | Identity keypair                                         | Optional, replaces default if set | None |
| `PROMETHEUS_ADDR`                                                          | Address for Prometheus monitoring                        | Replaces default if set | None specified in provided defaults |
| `MAX_RETRIES`                                                              | Maximum number of retries per transaction                | Replaces default if set | `40` (from `MAX_RETRIES`)                     |
| `RETRY_TIMEOUT`                                                            | Timeout for transaction retries in seconds               | Replaces default if set | `3` (from `DEFAULT_RETRY_TIMEOUT`)            |
| `QUIC_PROXY_ADDR`                                                          | Address for QUIC proxy                                   | Optional | None |
| `USE_GRPC`                                                                 | Flag to enable or disable gRPC                           | Enables gRPC if set | `false` |
| `GRPC_ADDR`<br/>`GRPC_ADDR2`<br/>`GRPC_ADDR3`<br/>`GRPC_ADDR4`             | gRPC address(es); will be multiplexed                    | Replaces default if set | `http://127.0.0.0:10000` (from `DEFAULT_GRPC_ADDR`) |
| `GRPC_X_TOKEN`<br/>`GRPC_X_TOKEN2`<br/>`GRPC_X_TOKEN3`<br/>`GRPC_X_TOKEN4` | Token for gRPC authentication                            | Optional | None |
| `PG_*`                                                                     | Various environment variables for Postgres configuration | Depends on Postgres usage | Based on `PostgresSessionConfig::new_from_env()` |

### Postgres
lite-rpc implements an optional postgres service that can write to postgres
database tables as defined in `./migrations`. This can be enabled by either
setting the environment variable `PG_ENABLED` to `true` or by passing the `-p`
option when launching the executable. If postgres is enabled then the optional
environment variables shown above must be set.

### Metrics
Various Prometheus metrics are exposed on `localhost:9091/metrics` which can be
used to monitor the health of the application in production.

### Deployment on fly.io
While lite-rpc can be deployed on any cloud infrastructure, it has been tested
extensively on https://fly.io. An example configuration has been provided in
`fly.toml`. We recommend a `dedicated-cpu-2x` VM with at least 4GB RAM.

The app listens by default on ports 8890 and 8891 for HTTP and Websockets
respectively. Since only a subset of RPC methods are implemented, we recommend
serving unimplemented methods from a full RPC node using a reverse proxy such as
HAProxy or Kong. Alternatively, you can connect directly to lite-rpc using a
web3.js Connection object that is _only_ used for sending and confirming
transactions.

Troubleshooting: if you encounter issues with QUIC _sendmsg_ check
[this](https://github.com/blockworks-foundation/lite-rpc/issues/199) - you might
need to explicitly disable GSO (Generic Segmenatin Offload) see
```DISABLE_GSO=true```

#### Example
```bash
fly apps create my-lite-rpc
fly secrets set -a my-lite-rpc RPC_URL=... WS_URL=...   # See above table for env options
fly scale vm dedicated-cpu-2x --memory 4096 -a my-lite-rpc
fly deploy -c cd/lite-rpc.toml -a my-lite-rpc --remote-only # To just launch lite-rpc
fly deploy -c cd/lite-rpc.toml -a my-lite-rpc --remote-only # To launch lite-rpc with proxy mode
```

## License & Copyright

Copyright (c) 2022 Blockworks Foundation

Licensed under the **[AGPL-3.0 license](LICENSE)**

