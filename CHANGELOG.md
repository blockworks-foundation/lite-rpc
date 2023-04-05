# Changelog

All notable changes to this project will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.0.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

**Note:** Version 0 of Semantic Versioning is handled differently from version 1 and above.
The minor version will be incremented upon a breaking change and the patch version will be incremented for features.

## [Unreleased]

### Features : 
- rpc: Creating a custom TPU Client instead of using solana TPU client more suitable for lite-rpc loads [PR](https://github.com/blockworks-foundation/lite-rpc/pull/105)
- metrics: Adding metrics related to custom tpu client [bf5841f](https://github.com/blockworks-foundation/lite-rpc/pull/105/commits/bf5841f43841d2bebd612abb714c53fbc920f090)

## [0.1.0] - 2023-04-01

commit: dc75e0e57386ce272bc22aa8fcfe35a0d8ce0eb0

Initial release.

### Includes

- rpc: Json rpc implementation using jsonrpsee crate to provide frontend to send and confirm transactions
- pubsub: A websocket implementation to subscribe to signature updates
- block-listening: A mechanism to get blocks from the RPC and read them to extract transaction data
- tpu-client: Mechanisms related to sending transaction to the cluster leaders
- postgres: Saving transaction and block data into postgres
- metrics: Updates related to metrics used for graphana and prometheus