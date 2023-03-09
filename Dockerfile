# syntax = docker/dockerfile:1.2
FROM rust:1.67.0 as base
RUN cargo install cargo-chef
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
RUN cargo build --release --bin lite-rpc

FROM debian:bullseye-slim as run
RUN apt-get update && apt-get -y install ca-certificates libc6
COPY --from=build /app/target/release/lite-rpc /usr/local/bin/

CMD lite-rpc --rpc-addr "$RPC_URL" --ws-addr "$WS_URL"
