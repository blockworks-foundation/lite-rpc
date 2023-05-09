# Lite RPC For Solana Blockchain 

Submitting a [transaction](https://docs.solana.com/terminology#transaction) to be executed on the solana blockchain,
requires the client to identify the next few leaders based on the
[leader schedule](https://docs.solana.com/terminology#leader-schedule), look up their peering information in gossip and
connect to them via the [quic protocol](https://en.wikipedia.org/wiki/QUIC). In order to simplify the
process so it can be triggered from a web browser, most applications
run full [validators](https://docs.solana.com/terminology#validator) that forward the transactions according to the
protocol on behalf of the web browser. Running full solana [validators](https://docs.solana.com/terminology#validator)
is incredibly resource intensive `(>256GB RAM)`, the goal of this
project would be to create a specialized micro-service that allows
to deploy this logic quickly and allows [horizontal scalability](https://en.wikipedia.org/wiki/Scalability) with
commodity vms.

### Confirmation strategies

1) Subscribe to new blocks using [blockSubscribe](https://docs.solana.com/developing/clients/jsonrpc-api#blocksubscribe---unstable-disabled-by-default)
2) Subscribing to signatures with pool of rpc servers. (Under development)
3) Listening to gossip protocol. (Future roadmap)

## Executing

*make sure `solana-validator` is running in the background with `--rpc-pubsub-enable-block-subscription`*

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

| env               | purpose                           | required?       |
| ---------         | ------                            | ----------      |
| `RPC_URL`         | HTTP URL for a full RPC node      | yes, for docker |
| `WS_URL`          | WS URL for a full RPC node        | yes, for docker |
| `IDENTITY`        | Staked validator identity keypair | no              |
| `CA_PEM_B64`      | Base64 encoded `ca.pem`           | if `-p` passed  |
| `CLIENT_PKS_B64`  | Base64 encoded `client.pks`       | if `-p` passed  |
| `CLIENT_PKS_PASS` | Password to `client.pks`          | if `-p` passed  |
| `PG_CONFIG`       | Postgres Connection Config        | if `-p` passed  |

### Postgres
LiteRpc implements an optional postgres service that can write to a postgres database tables as defined
in `./migrations`. This can be enabled by passing the `-p` option when launching the executable and defining the below environment variables

### Metrics
Various Prometheus metrics are exposed on `localhost:9091/metrics` which can be used to monitor the health of the application in production. 
Grafana dashboard coming soon!

### Deployment on fly.io
While lite-rpc can be deployed on any cloud infrastructure, it has been tested extensively on https://fly.io.
An example configuration has been provided in `fly.toml`. We recommend a `dedicated-cpu-2x` VM with at least 4GB RAM.

The app listens by default on ports 8890 and 8891 for HTTP and Websockets respectively. Since only a subset of RPC methods are implemented, we recommend serving unimplemented methods from a full RPC node using a reverse proxy such as HAProxy or Kong. Alternatively, you can connect directly to lite-rpc using a web3.js Connection object that is _only_ used for sending and confirming transactions.

#### Example
```
fly apps create my-lite-rpc
fly secrets set ...                           # See above table for env options
fly scale vm dedicated-cpu-2x --memory 4096
fly deploy --remote-only
```

## License & Copyright

Copyright (c) 2022 Blockworks Foundation

Licensed under the **[AGPL-3.0 license](LICENSE)**

