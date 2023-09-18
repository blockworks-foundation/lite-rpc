



```
 +------------+          +------------+          +------------+          +------------+
 |            |          |            |          |            |          |            |
 |    bench   | ---1---> |  lite-rpc  | ---2---> |   proxy    | ---3---> |  validator |
 |            |          |            |          |            |          |            |
 +------------+          +------------+          +------------+          +------------+
 
 1. rpc request
 2. tpu forward proxy request (QUIC): transactions, tpu address and tpu identity
 3. tpu call (QUIC), transactions:
 
```

Local Development / Testing
---------------------------

1. run test-validator (tested with 1.16.1)
```bash
RUST_LOG="error,solana_streamer::nonblocking::quic=debug" solana-test-validator --log
```
3. run quic proxy
```bash
RUST_LOG=debug cargo run --bin solana-lite-rpc-quic-forward-proxy -- --proxy-listen-addr 0.0.0.0:11111 --identity-keypair /pathto-test-ledger/validator-keypair.json
```
2. run lite-rpc
```bash
RUST_LOG=debug cargo run --bin lite-rpc -- --quic-proxy-addr 127.0.0.1:11111
```
3. run rust bench tool in _lite-rpc_
```bash
cd bench; cargo run -- --tx-count=10
```


### Example Output from _Solana Validator_:
(note: the peer type is __Staked__)
```
[2023-06-26T15:16:18.430602000Z INFO  solana_streamer::nonblocking::quic] Got a connection 127.0.0.1:8058
[2023-06-26T15:16:18.430633000Z DEBUG solana_streamer::nonblocking::quic] Peer public key is EPLzGRhibYmZ7qysF9BiPmSTRaL8GiLhrQdFTfL8h2fy
[2023-06-26T15:16:18.430839000Z DEBUG solana_streamer::nonblocking::quic] Peer type: Staked, stake 999999997717120, total stake 999999997717120, max streams 2048 receive_window Ok(12320) from peer 127.0.0.1:8058
[2023-06-26T15:16:18.430850000Z DEBUG solana_streamer::nonblocking::quic] quic new connection 127.0.0.1:8058 streams: 0 connections: 1
[2023-06-26T15:16:18.430854000Z DEBUG solana_streamer::nonblocking::quic] stream error: ApplicationClosed(ApplicationClose { error_code: 0, reason: b"done" })
```


QUIC/QUINN Endpoint and Connection specifics
---------------------------
* keep-alive and idle timeout: both values must be aligned AND they must be configured on both endpoints (see [docs](https://docs.rs/quinn/latest/quinn/struct.TransportConfig.html#method.keep_alive_interval))
* tune or disable __max_concurrent_uni_streams__ respectively


Monitoring
---------------------------
The Quic Proxy exposes prometheus metrics on address configured using _prometheus_addr_.

