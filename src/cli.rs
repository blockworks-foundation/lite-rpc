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
                "",
                self.rpc_url.as_str(),
                "",
            );
            self.websocket_url = ws_url;
        }
    }
}
