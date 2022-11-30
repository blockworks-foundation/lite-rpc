use std::sync::Arc;

use context::LiteRpcSubsrciptionControl;
use jsonrpc_core::MetaIoHandler;
use jsonrpc_http_server::{hyper, AccessControlAllowOrigin, DomainsValidation, ServerBuilder};
use pubsub::LitePubSubService;
use solana_perf::thread::renice_this_thread;
use tokio::sync::broadcast;

use crate::rpc::{
    lite_rpc::{self, Lite},
    LightRpcRequestProcessor,
};
mod cli;
mod context;
mod pubsub;
mod rpc;

pub fn main() {
    let matches = cli::build_args(solana_version::version!()).get_matches();
    let cli_config = cli::extract_args(&matches);

    let cli::Config {
        json_rpc_url,
        websocket_url,
        rpc_addr,
        subscription_port,
        ..
    } = &cli_config;

    let (broadcast_sender, _broadcast_receiver) = broadcast::channel(128);
    let (notification_sender, notification_reciever) = crossbeam_channel::unbounded();

    let pubsub_control = Arc::new(LiteRpcSubsrciptionControl::new(
        broadcast_sender,
        notification_reciever,
    ));

    // start websocket server
    let (_trigger, websocket_service) =
        LitePubSubService::new(pubsub_control.clone(), *subscription_port);

    // start recieving notifications and broadcast them
    {
        let pubsub_control = pubsub_control.clone();
        std::thread::Builder::new()
            .name("broadcasting thread".to_string())
            .spawn(move || {
                pubsub_control.start_broadcasting();
            })
            .unwrap();
    }
    let mut io = MetaIoHandler::default();
    let lite_rpc = lite_rpc::LightRpc;
    io.extend_with(lite_rpc.to_delegate());

    let mut request_processor =
        LightRpcRequestProcessor::new(json_rpc_url, websocket_url, notification_sender);

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

    {
        let request_processor = request_processor.clone();
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
    request_processor.free();
    websocket_service.close().unwrap();
}
