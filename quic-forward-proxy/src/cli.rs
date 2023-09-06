use clap::Parser;

#[derive(Parser, Debug)]
#[command(author, version, about, long_about = None)]
pub struct Args {
    #[arg(short = 'k', long, default_value_t = String::new())]
    pub identity_keypair: String,
    // e.g. 0.0.0.0:11111 or "localhost:11111"
    #[arg(short = 'l', long)]
    pub proxy_listen_addr: String,
}
