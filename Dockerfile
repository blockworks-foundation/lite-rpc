# syntax = docker/dockerfile:1.2
FROM lukemathwalker/cargo-chef:0.1.62-rust-1.73-bookworm as base
WORKDIR /app

FROM base AS plan
COPY . .
WORKDIR /app
RUN cargo chef prepare --recipe-path recipe.json

FROM chef as build
COPY --from=plan /app/recipe.json recipe.json
RUN cargo chef cook --release --recipe-path recipe.json
COPY . .
RUN cargo build --release --bin lite-rpc --bin solana-lite-rpc-quic-forward-proxy

FROM debian:bookworm as run
RUN apt update && apt-get -y install libssl3 openssl
COPY --from=build /app/target/release/solana-lite-rpc-quic-forward-proxy /usr/local/bin/
COPY --from=build /app/target/release/lite-rpc /usr/local/bin/

CMD lite-rpc