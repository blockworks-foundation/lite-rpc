# Lite RPC

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
3) Listining to gossip protocol. (Future roadmap)

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

## License & Copyright

Copyright (c) 2022 Blockworks Foundation

Licensed under the **[MIT LICENSE](LICENSE)**

