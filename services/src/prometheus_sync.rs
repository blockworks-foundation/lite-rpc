use prometheus::{Encoder, TextEncoder};
use tokio::{
    io::AsyncWriteExt,
    net::{TcpListener, TcpStream, ToSocketAddrs},
    task::JoinHandle,
};

pub struct PrometheusSync;

impl PrometheusSync {
    fn create_response(payload: &str) -> String {
        format!(
            "HTTP/1.1 200 OK\r\nContent-Length: {}\r\n\r\n{}",
            payload.len(),
            payload
        )
    }

    async fn handle_stream(&self, stream: &mut TcpStream) -> anyhow::Result<()> {
        let mut metrics_buffer = Vec::new();
        let encoder = TextEncoder::new();

        let metric_families = prometheus::gather();
        encoder
            .encode(&metric_families, &mut metrics_buffer)
            .unwrap();

        let metrics_buffer = String::from_utf8(metrics_buffer).unwrap();
        let response = Self::create_response(&metrics_buffer);

        stream.writable().await?;
        stream.write_all(response.as_bytes()).await?;

        stream.flush().await?;

        Ok(())
    }

    pub fn sync(self, addr: impl ToSocketAddrs + Send + 'static) -> JoinHandle<anyhow::Result<()>> {
        #[allow(unreachable_code)]
        tokio::spawn(async move {
            let listener = TcpListener::bind(addr).await?;

            loop {
                let Ok((mut stream, _addr)) =  listener.accept().await else {
                    continue;
                };

                let _ = self.handle_stream(&mut stream).await;
            }

            Ok(())
        })
    }
}
