use std::sync::Arc;

use jsonrpc_core::MetaIoHandler;
use jsonrpc_http_server::{hyper, AccessControlAllowOrigin, DomainsValidation, ServerBuilder};
use solana_perf::thread::renice_this_thread;

use crate::rpc::{
    lite_rpc::{self, Lite},
    LightRpcRequestProcessor,
};
mod cli;
mod context;
mod rpc;

pub fn main() {
    let matches = cli::build_args(solana_version::version!()).get_matches();
    let cli_config = cli::extract_args(&matches);

    let cli::Config {
        json_rpc_url,
        websocket_url,
        rpc_addr,
        ..
    } = &cli_config;

    let mut io = MetaIoHandler::default();
    let lite_rpc = lite_rpc::LightRpc;
    io.extend_with(lite_rpc.to_delegate());

    let request_processor = LightRpcRequestProcessor::new(json_rpc_url, websocket_url);

    let runtime = Arc::new(
        tokio::runtime::Builder::new_multi_thread()
            .worker_threads(1)
            .on_thread_start(move || renice_this_thread(0).unwrap())
            .thread_name("solLiteRpcProcessor")
            .enable_all()
            .build()
            .expect("Runtime"),
    );
    let max_request_body_size: usize = 50 * (1 << 10);
    let socket_addr = *rpc_addr;

    let server =
        ServerBuilder::with_meta_extractor(io, move |_req: &hyper::Request<hyper::Body>| {
            request_processor.clone()
        })
        .event_loop_executor(runtime.handle().clone())
        .threads(1)
        .cors(DomainsValidation::AllowOnly(vec![
            AccessControlAllowOrigin::Any,
        ]))
        .cors_max_age(86400)
        .max_request_body_size(max_request_body_size)
        .start_http(&socket_addr);
    println!("Starting Lite RPC node");
    server.unwrap().wait();
}
