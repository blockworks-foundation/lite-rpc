# Light RPC

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

## Test

*make sure `solana-test-validator` is running in the background*
```bash
$ cd ~ && solana-test-validator 
```

*run `light-rpc` test*
```bash
$ cargo test
```

## Bench

*make sure `solana-test-validator` is running in the background*
```bash
$ cd ~ && solana-test-validator 
```

*run `light-rpc` bench*
```bash
$ cargo bench
```

Find a new file named `metrics.csv` in the project root.
