use clap::Parser;

#[derive(Parser, Debug)]
#[command(author, version, about, long_about = None)]
pub struct Args {
    #[arg(short = 'k', long, default_value_t = String::new())]
    pub identity_keypair: String,
    // e.g. 0.0.0.0:11111
    #[arg(short = 'l', long, env)]
    pub proxy_listen_addr: String,
    /// enable metrics to prometheus at addr
    #[arg(short = 'm', long, default_value_t = String::from("[::]:9092"))]
    pub prometheus_addr: String,
}
