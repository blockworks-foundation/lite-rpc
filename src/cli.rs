use clap::Subcommand;

use clap::Parser;

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

    #[clap(subcommand)]
    pub command:Command,

    /*
    #[arg(short, long, default_value_t = String::from("8899"))]
    pub port: String,
    #[arg(short, long, default_value_t = String::from("8900"))]
    pub subscription_port: String,
    #[arg(short, long, default_value_t = String::from("http://localhost:8899"))]
    pub rpc_url: String,
    #[arg(short, long,  default_value_t = String::new())]
    pub websocket_url: String,
    */
}
/* 
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
*/
#[derive(Subcommand,Debug)]
pub enum Command {
    Run {
        #[arg(short, long, default_value_t = String::from("8899"))]
        port: String,
        #[arg(short, long, default_value_t = String::from("8900"))]
        subscription_port: String,
        #[arg(short, long, default_value_t = String::from("http://localhost:8899"))]
        rpc_url: String,
        #[arg(short, long,  default_value_t = String::new())]
        websocket_url: String,
    },
    Test,
}
