use clap::{command, Parser};

#[derive(Parser, Debug)]
#[command(author, version, about, long_about = None)]
pub struct Args {
    /// Number of tx(s) sent in each run
    #[arg(short = 't', long, default_value_t = 20_000)]
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
}
