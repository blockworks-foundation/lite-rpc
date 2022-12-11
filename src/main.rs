use std::{
    net::{IpAddr, Ipv4Addr, SocketAddr},
    sync::Arc,
    thread::sleep,
};

use clap::Parser;
use context::LiteRpcSubsrciptionControl;
use jsonrpc_core::MetaIoHandler;
use jsonrpc_http_server::{hyper, AccessControlAllowOrigin, DomainsValidation, ServerBuilder};
use pubsub::LitePubSubService;
use solana_cli_config::ConfigInput;
use solana_perf::thread::renice_this_thread;
use tokio::sync::broadcast;

use crate::{
    context::{launch_performance_updating_thread, PerformanceCounter},
    rpc::{
        lite_rpc::{self, Lite},
        LightRpcRequestProcessor,
    },
};
mod cli;
mod client;
mod context;
mod pubsub;
mod rpc;

use cli::Args;

fn run(port: u16, subscription_port: u16, rpc_url: String, websocket_url: String) {
    let rpc_url = if rpc_url.is_empty() {
        let (_, rpc_url) = ConfigInput::compute_json_rpc_url_setting(
            rpc_url.as_str(),
            &ConfigInput::default().json_rpc_url,
        );
        rpc_url
    } else {
        rpc_url
    };
    let websocket_url = if websocket_url.is_empty() {
        let (_, ws_url) = ConfigInput::compute_websocket_url_setting(
            &websocket_url.as_str(),
            "",
            rpc_url.as_str(),
            "",
        );
        ws_url
    } else {
        websocket_url
    };
    println!(
        "Using rpc server {} and ws server {}",
        rpc_url, websocket_url
    );
    let performance_counter = PerformanceCounter::new();
    launch_performance_updating_thread(performance_counter.clone());

    let (broadcast_sender, _broadcast_receiver) = broadcast::channel(10000);
    let (notification_sender, notification_reciever) = crossbeam_channel::unbounded();

    let pubsub_control = Arc::new(LiteRpcSubsrciptionControl::new(
        broadcast_sender,
        notification_reciever,
    ));

    let subscription_port =
        SocketAddr::new(IpAddr::V4(Ipv4Addr::new(127, 0, 0, 1)), subscription_port);

    // start websocket server
    let (_trigger, websocket_service) = LitePubSubService::new(
        pubsub_control.clone(),
        subscription_port,
        performance_counter.clone(),
    );
    let broadcast_thread = {
        // build broadcasting thread
        let pubsub_control = pubsub_control.clone();
        std::thread::Builder::new()
            .name("broadcasting thread".to_string())
            .spawn(move || {
                pubsub_control.start_broadcasting();
            })
            .unwrap()
    };
    let mut io = MetaIoHandler::default();
    let lite_rpc = lite_rpc::LightRpc;
    io.extend_with(lite_rpc.to_delegate());

    let mut request_processor = LightRpcRequestProcessor::new(
        rpc_url.as_str(),
        &websocket_url,
        notification_sender,
        performance_counter.clone(),
    );
    let cleaning_thread = {
        // build cleaning thread
        let context = request_processor.context.clone();
        std::thread::Builder::new()
            .name("cleaning thread".to_string())
            .spawn(move || {
                context.remove_stale_data(60 * 10);
                sleep(std::time::Duration::from_secs(60 * 5))
            })
            .unwrap()
    };

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

    let socket_addr = SocketAddr::new(IpAddr::V4(Ipv4Addr::new(127, 0, 0, 1)), port);
    {
        let request_processor = request_processor.clone();
        let server =
            ServerBuilder::with_meta_extractor(io, move |_req: &hyper::Request<hyper::Body>| {
                request_processor.clone()
            })
            .event_loop_executor(runtime.handle().clone())
            .threads(4)
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
    broadcast_thread.join().unwrap();
    cleaning_thread.join().unwrap();
}

pub async fn main() {
    let mut cli = Args::parse();
    cli.resolve_address();
    let Args {
        port,
        subscription_port,
        rpc_url,
        websocket_url,
    } = cli;

    run(port, subscription_port, rpc_url, websocket_url)
}
