use clap::Parser;
use const_env::from_env;


/// 25 slots in 10s send to little more leaders
#[from_env]
pub const DEFAULT_FANOUT_SIZE: u64 = 10;


#[derive(Parser, Debug)]
#[command(author, version, about, long_about = None)]
pub struct Args {
    #[arg(short = 'k', long, default_value_t = String::new())]
    pub identity_keypair: String,
    // e.g. 0.0.0.0:11111 or "localhost:11111"
    #[arg(short = 'l', long, env)]
    pub proxy_listen_addr: String,
    /// tpu fanout
    #[arg(short = 'f', long, default_value_t = DEFAULT_FANOUT_SIZE) ]
    pub fanout_size: u64,
}
