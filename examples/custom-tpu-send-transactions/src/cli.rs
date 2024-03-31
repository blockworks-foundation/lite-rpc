use clap::Parser;

#[derive(Parser, Debug, Clone)]
#[command(author, version, about, long_about = None)]
pub struct Args {
    /// config.json
    #[arg(short, long, default_value = "http://127.0.0.1:8899")]
    pub rpc_url: String,

    #[arg(short, long)]
    pub grpc_url: Option<String>,

    #[arg(short, long)]
    pub x_token: Option<String>,

    #[arg(short, long)]
    pub transaction_count: Option<usize>,

    #[arg(short, long, default_value_t = 1)]
    pub number_of_seconds: usize,

    #[arg(short, long)]
    pub fee_payer: String,

    #[arg(short, long)]
    pub staked_identity: Option<String>,

    #[arg(short, long)]
    pub priority_fees: Option<u64>,
}
