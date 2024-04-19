use std::time::Duration;

use bytes::Bytes;
use geyser_grpc_connector::{
    yellowstone_grpc_util::GeyserGrpcClientBufferConfig, GeyserGrpcClient, GeyserGrpcClientResult,
};
use tonic::{
    codec::CompressionEncoding,
    metadata::{errors::InvalidMetadataValue, AsciiMetadataValue},
    service::Interceptor,
    transport::ClientTlsConfig,
};
use tonic_health::pb::health_client::HealthClient;
use yellowstone_grpc_client::InterceptorXToken;
use yellowstone_grpc_proto::geyser::geyser_client::GeyserClient;

pub async fn connect_with_timeout_with_buffers_and_compression<E, T>(
    endpoint: E,
    x_token: Option<T>,
    tls_config: Option<ClientTlsConfig>,
    connect_timeout: Option<Duration>,
    request_timeout: Option<Duration>,
    buffer_config: GeyserGrpcClientBufferConfig,
) -> GeyserGrpcClientResult<GeyserGrpcClient<impl Interceptor>>
where
    E: Into<Bytes>,
    T: TryInto<AsciiMetadataValue, Error = InvalidMetadataValue>,
{
    // see https://github.com/blockworks-foundation/geyser-grpc-connector/issues/10
    let mut endpoint = tonic::transport::Endpoint::from_shared(endpoint)?
        .buffer_size(buffer_config.buffer_size)
        .initial_connection_window_size(buffer_config.conn_window)
        .initial_stream_window_size(buffer_config.stream_window);

    if let Some(tls_config) = tls_config {
        endpoint = endpoint.tls_config(tls_config)?;
    }

    if let Some(connect_timeout) = connect_timeout {
        endpoint = endpoint.timeout(connect_timeout);
    }

    if let Some(request_timeout) = request_timeout {
        endpoint = endpoint.timeout(request_timeout);
    }

    let x_token: Option<AsciiMetadataValue> = match x_token {
        Some(x_token) => Some(x_token.try_into()?),
        None => None,
    };
    let interceptor = InterceptorXToken { x_token };

    let channel = endpoint.connect_lazy();
    let client = GeyserGrpcClient::new(
        HealthClient::with_interceptor(channel.clone(), interceptor.clone()),
        GeyserClient::with_interceptor(channel, interceptor)
            .max_decoding_message_size(GeyserGrpcClient::max_decoding_message_size())
            .accept_compressed(CompressionEncoding::Gzip)
            .send_compressed(CompressionEncoding::Gzip),
    );
    Ok(client)
}
