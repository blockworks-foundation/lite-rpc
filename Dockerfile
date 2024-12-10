# syntax = docker/dockerfile:1.2
FROM rust:1.81.0 as base
RUN cargo install cargo-chef@0.1.62 --locked
RUN rustup component add rustfmt
RUN apt-get update && apt-get install -y clang cmake ssh
WORKDIR /app

FROM base AS plan
COPY . .
WORKDIR /app
RUN cargo chef prepare --recipe-path recipe.json

FROM base as build
COPY --from=plan /app/recipe.json recipe.json
RUN cargo chef cook --release --recipe-path recipe.json
COPY . .
ENV RUSTFLAGS="--cfg tokio_unstable"
RUN cargo build --release --bin lite-rpc --bin solana-lite-rpc-quic-forward-proxy

FROM debian:bookworm-slim as run
RUN apt-get update && apt-get -y install ca-certificates libc6 libssl3 libssl-dev openssl

COPY --from=build /app/target/release/solana-lite-rpc-quic-forward-proxy /usr/local/bin/
COPY --from=build /app/target/release/lite-rpc /usr/local/bin/
COPY openssl-legacy.cnf /etc/ssl/openssl-legacy.cnf

ENV OPENSSL_CONF=/etc/ssl/openssl-legacy.cnf
CMD lite-rpc
