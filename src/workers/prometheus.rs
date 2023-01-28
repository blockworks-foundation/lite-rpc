use std::collections::HashMap;

use tokio::{
    io::AsyncWriteExt,
    net::{TcpListener, TcpStream},
    task::JoinHandle,
};

use super::MetricsCapture;

#[derive(Clone)]
pub struct PrometheusSync {
    metrics_capture: MetricsCapture,
}

impl PrometheusSync {
    pub fn new(metrics_capture: MetricsCapture) -> Self {
        Self { metrics_capture }
    }

    fn create_response(payload: &str) -> String {
        format!(
            "HTTP/1.1 200 OK\r\nContent-Length: {}\r\n\r\n{}",
            payload.len(),
            payload
        )
    }

    async fn handle_stream(&self, stream: &mut TcpStream) -> anyhow::Result<()> {
        let metrics = self.metrics_capture.get_metrics().await;
        let metrics = serde_prometheus::to_string(&metrics, None, HashMap::new())?;

        let response = Self::create_response(&metrics);

        stream.writable().await?;
        stream.write_all(response.as_bytes()).await?;

        stream.flush().await?;

        Ok(())
    }

    pub fn sync(self) -> JoinHandle<anyhow::Result<()>> {
        #[allow(unreachable_code)]
        tokio::spawn(async move {
            let listener = TcpListener::bind("[::]:9500").await?;

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
