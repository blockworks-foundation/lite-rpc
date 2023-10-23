use clap::{command, Parser};

#[derive(Parser, Debug)]
#[command(author, version, about, long_about = None)]
pub struct Args {
    /// Number of tx(s) sent in each run
    #[arg(short = 'n', long, default_value_t = 5_000)]
    pub tx_count: usize,
    /// Number of bench runs
    #[arg(short = 'r', long, default_value_t = 1)]
    pub runs: usize,
    /// Interval between each bench run (ms)
    #[arg(short = 'i', long, default_value_t = 1000)]
    pub run_interval_ms: u64,
    /// Metrics output file name
    #[arg(short = 'm', long, default_value_t = String::from("metrics.csv"))]
    pub metrics_file_name: String,
    /// Lite Rpc Address
    #[arg(short = 'l', long, default_value_t = String::from("http://127.0.0.1:8890"))]
    pub lite_rpc_addr: String,
    #[arg(short = 't', long, default_value_t = String::from("transactions.csv"))]
    pub transaction_save_file: String,
    // choose between small (179 bytes) and large (1186 bytes) transactions
    #[arg(short = 'L', long, default_value_t = false)]
    pub large_transactions: bool,
    #[arg(long, default_value_t = false)]
    pub pingthing_enable: bool,
    #[arg(long,)]
    pub pingthing_va_api_key: Option<String>,
}
