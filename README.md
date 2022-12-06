# Solana Lite RPC

This project aims to create a lite rpc server which is responsible only for sending and confirming the transactions. 
The lite-rpc server will not have any ledger or banks.
While sending transaction the lite rpc server will send the transaction to next few leader (FANOUT) and then use different strategies to confirm the transaction. 
The rpc server will also have a websocket port which is reponsible for just subscribing to slots and signatures. 
The lite rpc server will be optimized to confirm transactions which are forwarded to leader using the same server.
This project is currently based on an unstable feature of block subscription of solana which can be enabled using `--rpc-pubsub-enable-block-subscription` while running rpc node.

### Confirmation strategies
1) Subscribing to blocks changes and checking the confirmations. (Under development)
2) Subscribing to signatures with pool of rpc servers. (Under development)
3) Listining to gossip protocol. (Future roadmap)

## Build 
`cargo build`

## Run
* For RPC node : `http://localhost:8899`,
* Websocket : `http://localhost:8900` (Optional),
* Port : `9000` Listening port for LiteRpc server,
* Subscription Port : `9001` Listening port of websocket subscriptions for LiteRpc server,


```
cargo run --bin lite-rpc -- --port 9000 --subscription-port 9001 --rpc-url http://localhost:8899
```