
# Welcome to the Lite RPC QUIC Forward Proxy Sub-Project!


Project Info
----------------
* Status: __experimental__ - Feedback welcome (Solana-Discord: _grooviegermanikus_)
* [Lite RPC QUIC Forward Proxy](https://github.com/blockworks-foundation/lite-rpc/tree/main/quic-forward-proxy)
* [Lite RPC](https://github.com/blockworks-foundation/lite-rpc/)



Purpose
-------
This component (__quic-forward-proxy__) can be optionally used along with one or multiple Lite RPC micro-service instances to optimized and simplify sending transactions to Solana TPUs.

Benefits:
* the __quic-forward-proxy__ can batch transactions from multiple source and send them _efficiently_ to the Solana validator
* the __quic-forward-proxy__ can be optionally configured with a validator identity keypair:
  * connections to TPU will be privileged (__staked connections__)
  * keypair can be kept in one place while the Lite RPC instances can benefit from the staked connection


Configuration
---------------------
Prepare: choose a proxy port (e.g. 11111) and bind address of appropriate (minimal) network interface (e.g. 127.0.0.1)
1. run quic proxy
    ```
    # unstaked
    solana-lite-rpc-quic-forward-proxy --proxy-listen-addr 127.0.0.1:11111
    # staked
    solana-lite-rpc-quic-forward-proxy --proxy-listen-addr 127.0.0.1:11111 --identity-keypair /pathto/validator-keypair.json
    ```
2. run lite-rpc
    ```bash
    lite-rpc --experimental-quic-proxy-addr 127.0.0.1:11111
    ```

Architecture Overview
---------------------
```
 +------------+          +------------+          +------------+          +------------+
 |            |          |            |          |            |          |            |
 |   client   | ---1---> |  lite-rpc  | ---2---> |   proxy    | ---3---> |  validator |
 |            |          |            |          |            |          |            |
 +------------+          +------------+          +------------+          +------------+
 
 1. rpc request
 2. tpu forward proxy request (QUIC): transactions, tpu address and tpu identity
 3. tpu call (QUIC), transactions:
 
 * client: RPC client to lite-rpc
 * proxy: QUIC forward proxy service (one instance)
 * lite-rpc: N lite-rpc services 
 * validator: solana validator (TPU) according to the leader schedule
 
```

Local Development / Testing
---------------------------
### Rust Integration Test

Use integrated testing in __quic_proxy_tpu_integrationtest.rs__ for fast feedback.

### Local Setup With Solana Test Validator
1. run test-validator (tested with 1.16.1)
    ```bash
    RUST_LOG="error,solana_streamer::nonblocking::quic=debug" solana-test-validator --log
    ```
2. run quic proxy
    ```bash
    # unstaked
    RUST_LOG=debug cargo run --bin solana-lite-rpc-quic-forward-proxy -- --proxy-listen-addr 0.0.0.0:11111
    # staked
    RUST_LOG=debug cargo run --bin solana-lite-rpc-quic-forward-proxy -- --proxy-listen-addr 0.0.0.0:11111 --identity-keypair /pathto-test-ledger/validator-keypair.json
    ```
3. run lite-rpc
    ```bash
    RUST_LOG=debug cargo run --bin lite-rpc -- --quic-proxy-addr 127.0.0.1:11111
    ```
4. run rust bench tool in _lite-rpc_
    ```bash
    cd bench; cargo run -- --tx-count=10
    ```

Implementation Details
----------------------
* _proxy_ expects clients to send transactions via QUIC using the __proxy request format__:
  * array of transactions (signature and raw bytes)
  * array of tpu target nodes (address:port and identity public key)
* the _proxy_ is designed to be light-weight and stateless (no persistence)
* note: only one instance of the _proxy_ should talk to the TPU nodes at a time to be able to correctly comply with the validator quic policy
* outbound connections (to TPU):
  * _proxy_ tries to maintain active quic connections to recent TPU nodes using transparent reconnect
  * _proxy_ will maintain multiple quic connection per TPU according to the Solana validator quic policy
  * _proxy_ will use lightweight quic streams to send the transactions
* inbound traffic (from Lite RPC)
  * client-proxy-communcation is done via QUIC using a custom wire format
  * _proxy_ supports only quic ATM but that could be extended to support other protocols
  * _proxy_ should perform client authentication by TLS (see [issue](https://github.com/blockworks-foundation/lite-rpc/issues/167))
* _proxy_ uses a single queue (channel) for buffering the transactions from any inbound connection
* TPU selection / Leader Schedule
  * the _proxy_ will not perform any TPU selection; the TPU target nodes __MUST__ be selected by the __client__ (Lite RPC) and not by the _proxy_
  * pitfall: the TPU target node list might become stale if the transactions are not sent out fast enough

### Example Output from _Solana Validator_:
(note: the peer type is __Staked__)
```
[2023-06-26T15:16:18.430602000Z INFO  solana_streamer::nonblocking::quic] Got a connection 127.0.0.1:8058
[2023-06-26T15:16:18.430633000Z DEBUG solana_streamer::nonblocking::quic] Peer public key is EPLzGRhibYmZ7qysF9BiPmSTRaL8GiLhrQdFTfL8h2fy
[2023-06-26T15:16:18.430839000Z DEBUG solana_streamer::nonblocking::quic] Peer type: Staked, stake 999999997717120, total stake 999999997717120, max streams 2048 receive_window Ok(12320) from peer 127.0.0.1:8058
[2023-06-26T15:16:18.430850000Z DEBUG solana_streamer::nonblocking::quic] quic new connection 127.0.0.1:8058 streams: 0 connections: 1
[2023-06-26T15:16:18.430854000Z DEBUG solana_streamer::nonblocking::quic] stream error: ApplicationClosed(ApplicationClose { error_code: 0, reason: b"done" })
```

Solana Validator QUIC Policy
----------------------------
TPU has complex logic to assign connection capacity to TPU clients (see Solana quic.rs)
* it purges connections
* it limits number of parallel quic streams
* it considers stake vs unstaked connections (validator identity signature in QUIC TLS certificate, see Solana get_remote_pubkey+get_pubkey_from_tls_certificate)
* it keeps connections on a per peer address / peer identity basis
* ...

## License & Copyright

Copyright (c) 2022 Blockworks Foundation

Licensed under the **[AGPL-3.0 license](/LICENSE)**

