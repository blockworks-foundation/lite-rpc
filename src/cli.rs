use clap::Parser;
use solana_cli_config::ConfigInput;

/// Holds the configuration for a single run of the benchmark
#[derive(Parser, Debug)]
#[command(
    version,
    about = "A lite version of solana rpc to send and confirm transactions.",
    long_about = "Lite rpc is optimized to send and confirm transactions for solana blockchain. \
    When it recieves a transaction it will directly send it to next few leaders. It then adds the signature into internal map. It listen to block subscriptions for confirmed and finalized blocks. \
    It also has a websocket port for subscription to onSlotChange and onSignature subscriptions. \
    We have to find the optimum value for batch_size and fanout. 
    batch_size : Lite rpc will wait for 1 ms to create a batch of this size and send the batch to TPU.
    fanout : Lite rpc will send transactions to leaders of fanout slots. (min 1, max 100)
    "
)]
pub struct Args {
    #[arg(short, long, default_value_t = 9000)]
    pub port: u16,
    #[arg(short, long, default_value_t = 9001)]
    pub subscription_port: u16,
    #[arg(short, long, default_value_t = String::from("http://localhost:8899"))]
    pub rpc_url: String,
    #[arg(short, long,  default_value_t = String::from("ws://localhost:8900"))]
    pub websocket_url: String,
    #[arg(short, long, default_value_t = 16)]
    pub batch_size: u16,
    #[arg(short, long, default_value_t = 32)]
    pub fanout: u8,
}

impl Args {
    pub fn resolve_address(&mut self) {
        if self.rpc_url.is_empty() {
            let (_, rpc_url) = ConfigInput::compute_json_rpc_url_setting(
                self.rpc_url.as_str(),
                &ConfigInput::default().json_rpc_url,
            );
            self.rpc_url = rpc_url;
        }
        if self.websocket_url.is_empty() {
            let (_, ws_url) = ConfigInput::compute_websocket_url_setting(
                &self.websocket_url.as_str(),
                &ConfigInput::default().websocket_url,
                self.rpc_url.as_str(),
                &ConfigInput::default().json_rpc_url,
            );
            self.websocket_url = ws_url;
        }
    }
}
