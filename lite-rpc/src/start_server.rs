use crate::{
    bridge::LiteBridge, bridge_pubsub::LitePubSubBridge, rpc::LiteRpcServer,
    rpc_pubsub::LiteRpcPubSubServer,
};

use jsonrpsee::server::middleware::{Authority, AuthorityError};
use jsonrpsee::server::ServerBuilder;
use solana_lite_rpc_core::AnyhowJoinHandle;
use hyper::{Body, Method, Request, Response};
use std::error::Error as StdError;
use std::sync::Arc;
use std::time::Duration;
use tower::{Layer, Service};
use futures_util::{Future, FutureExt, TryFutureExt};
use std::task::{Context, Poll};
use std::pin::Pin;
use tower_http::cors::{Any, CorsLayer};

// /// Middleware to enable host filtering.
// #[derive(Debug, Clone)]
// pub struct LogLayer;

// impl LogLayer {
// 	/// Enables host filtering and allow only the specified hosts.
// 	pub fn new<T: IntoIterator<Item = U>, U: TryInto<Authority>>(allow_only: T) -> Result<Self, AuthorityError>
// 	where
// 		T: IntoIterator<Item = U>,
// 		U: TryInto<Authority, Error = AuthorityError>,
// 	{
// 		let allow_only: Result<Vec<_>, _> = allow_only.into_iter().map(|a| a.try_into()).collect();
// 		Ok(Self)
// 	}

// 	pub fn disable() -> Self {
// 		Self
// 	}
// }

// impl<S> Layer<S> for LogLayer {
// 	type Service = LLLayer<S>;

// 	fn layer(&self, inner: S) -> Self::Service {
// 		LLLayer { inner }
// 	}
// }


/// Middleware to enable host filtering.
// #[derive(Debug)]
// pub struct LLLayer<S> {
// 	inner: S,
// }

// impl<S> Service<Request<Body>> for LLLayer<S>
// where
// 	S: Service<Request<Body>, Response = Response<Body>>,
// 	S::Response: 'static,
// 	S::Error: Into<Box<dyn StdError + Send + Sync>> + 'static,
// 	S::Future: Send + 'static,
// {
// 	type Response = S::Response;
// 	type Error = Box<dyn StdError + Send + Sync + 'static>;
// 	type Future = Pin<Box<dyn Future<Output = Result<Self::Response, Self::Error>> + Send + 'static>>;

// 	fn poll_ready(&mut self, cx: &mut Context<'_>) -> Poll<Result<(), Self::Error>> {
// 		self.inner.poll_ready(cx).map_err(Into::into)
// 	}

// 	fn call(&mut self, request: Request<Body>) -> Self::Future {
//         log::info!("got method {:?}", request.method() );
// 		Box::pin(self.inner.call(request).map_err(Into::into))
// 	}
// }


pub async fn start_servers(
    rpc: LiteBridge,
    pubsub: LitePubSubBridge,
    ws_addr: String,
    http_addr: String,
) -> anyhow::Result<()> {
    let rpc = rpc.into_rpc();
    let pubsub = pubsub.into_rpc();
    

    let ws_server_handle = ServerBuilder::default()
        .ws_only()
        .build(ws_addr.clone())
        .await?
        .start(pubsub);

    let cors = CorsLayer::new()
        .max_age(Duration::from_secs(60))
		// Allow `POST` when accessing the resource
		.allow_methods([Method::POST, Method::GET, Method::OPTIONS])
		// Allow requests from any origin
		.allow_origin(Any)
		.allow_headers(Any);

	let middleware = tower::ServiceBuilder::new().layer(cors);

    let http_server_handle = ServerBuilder::default()
        .set_middleware(middleware)
        .http_only()
        .build(http_addr.clone())
        .await?
        .start(rpc);

    let ws_server: AnyhowJoinHandle = tokio::spawn(async move {
        log::info!("Websocket Server started at {ws_addr:?}");
        ws_server_handle.stopped().await;
        anyhow::bail!("Websocket server stopped");
    });

    let http_server: AnyhowJoinHandle = tokio::spawn(async move {
        log::info!("HTTP Server started at {http_addr:?}");
        http_server_handle.stopped().await;
        anyhow::bail!("HTTP server stopped");
    });

    tokio::select! {
        res = ws_server => {
            anyhow::bail!("WebSocket server {res:?}");
        },
        res = http_server => {
            anyhow::bail!("HTTP server {res:?}");
        },
    }
}
