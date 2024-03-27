use bytes::Bytes;
use std::time::Duration;
use tonic::metadata::{errors::InvalidMetadataValue, AsciiMetadataValue};
use tonic::service::Interceptor;
use tonic::transport::ClientTlsConfig;
use tonic_health::pb::health_client::HealthClient;
use yellowstone_grpc_client::{GeyserGrpcClient, InterceptorXToken};
use yellowstone_grpc_proto::geyser::geyser_client::GeyserClient;
use yellowstone_grpc_proto::tonic;

pub async fn connect_with_timeout_hacked<E, T>(
    endpoint: E,
    x_token: Option<T>,
) -> anyhow::Result<GeyserGrpcClient<impl Interceptor>>
where
    E: Into<Bytes>,
    T: TryInto<AsciiMetadataValue, Error = InvalidMetadataValue>,
{
    let endpoint = tonic::transport::Endpoint::from_shared(endpoint)?
        .buffer_size(Some(65536))
        .initial_connection_window_size(4194304)
        .initial_stream_window_size(4194304)
        .connect_timeout(Duration::from_secs(10))
        .timeout(Duration::from_secs(10))
        // .http2_adaptive_window()
        .tls_config(ClientTlsConfig::new())?;

    let x_token: Option<AsciiMetadataValue> = x_token.map(|v| v.try_into()).transpose()?;
    let interceptor = InterceptorXToken { x_token };

    let channel = endpoint.connect_lazy();
    let client = GeyserGrpcClient::new(
        HealthClient::with_interceptor(channel.clone(), interceptor.clone()),
        GeyserClient::with_interceptor(channel, interceptor)
            .max_decoding_message_size(GeyserGrpcClient::max_decoding_message_size()),
    );
    Ok(client)
}
