use bench::tx_size::TxSize;
use clap::Parser;

#[derive(Parser, Debug)]
#[command(author, version, about, long_about = None)]
pub struct Args {
    /// interval in milliseconds to run the benchmark
    #[arg(short = 'b', long, default_value_t = 60_000)]
    pub bench_interval: u64,
    #[arg(short = 'n', long, default_value_t = 10)]
    pub tx_count: usize,
    #[clap(short, long, default_value_t = TxSize::Small)]
    pub size_tx: TxSize,
}
