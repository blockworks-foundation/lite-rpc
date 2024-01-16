use bench::cli::Args;
use bench::strategies::Strategy;
use clap::Parser;

#[tokio::main(flavor = "multi_thread", worker_threads = 16)]
async fn main() {
    dotenv::dotenv().ok();

    tracing_subscriber::fmt::init();

    let Args {
        metrics_file_name,
        strategy,
    } = Args::parse();

    let res = strategy.execute().await.unwrap();

    if res.is_empty() {
        return;
    }

    let mut csv_writer = csv::Writer::from_path(&metrics_file_name).unwrap();

    let headers = res
        .first()
        .unwrap()
        .as_object()
        .unwrap()
        .keys()
        .collect::<Vec<_>>();
    csv_writer.write_record(&headers).unwrap();

    res.iter().for_each(|x| {
        let row = x
            .as_object()
            .unwrap()
            .values()
            .map(|v| v.to_string())
            .collect::<Vec<_>>();
        csv_writer.write_record(&row).unwrap();
    });

    csv_writer.flush().unwrap();
}
