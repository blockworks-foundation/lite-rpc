use anyhow::bail;
use itertools::Itertools;
use log::{error, info};
use solana_lite_rpc_cluster_endpoints::endpoint_stremers::EndpointStreaming;
use solana_lite_rpc_cluster_endpoints::json_rpc_subscription::create_json_rpc_polling_subscription;
use solana_lite_rpc_core::structures::epoch::{Epoch, EpochCache, EpochRef};
use solana_lite_rpc_core::structures::slot_notification::SlotNotification;
use solana_lite_rpc_core::types::SlotStream;
use solana_lite_rpc_history::postgres::postgres_epoch::PostgresEpoch;
use solana_lite_rpc_history::postgres::postgres_session::PostgresSession;
use solana_rpc_client::nonblocking::rpc_client::RpcClient;
use solana_sdk::clock::Slot;
use std::sync::Arc;
use std::time::Instant;
use tokio::sync::broadcast::error::RecvError;
use tokio::sync::watch;
use tokio::sync::watch::Sender;
use tokio::task::JoinHandle;
use tokio_postgres::Client;

const NUM_PARALLEL_TASKS: usize = 1;

#[tokio::main(flavor = "multi_thread", worker_threads = 16)]
pub async fn main() -> anyhow::Result<()> {
    tracing_subscriber::fmt::init();

    let sessions = vec![
        PostgresSession::new_from_env().await.unwrap(),
        PostgresSession::new_from_env().await.unwrap(),
        PostgresSession::new_from_env().await.unwrap(),
        PostgresSession::new_from_env().await.unwrap(),
        PostgresSession::new_from_env().await.unwrap(),
        PostgresSession::new_from_env().await.unwrap(),
        PostgresSession::new_from_env().await.unwrap(),
        PostgresSession::new_from_env().await.unwrap(),
    ];

    let rpc_url = std::env::var("RPC_URL").expect("env var RPC_URL is mandatory");
    let rpc_client = Arc::new(RpcClient::new(rpc_url));
    let epoch_data = EpochCache::bootstrap_epoch(&rpc_client).await.unwrap();
    let (subscriptions, _cluster_endpoint_tasks) =
        create_json_rpc_polling_subscription(rpc_client.clone(), NUM_PARALLEL_TASKS).unwrap();

    let EndpointStreaming { slot_notifier, .. } = subscriptions;

    let (tx, mut rx) = watch::channel(0);

    let jh2 = slot_listener(slot_notifier.resubscribe(), tx);

    let mut ticker = tokio::time::interval(tokio::time::Duration::from_secs(1));
    loop {
        let slot = *rx.borrow_and_update();
        if slot == 0 {
            continue;
        }

        info!("processed slot: {}", slot);

        // ATM we are 4000 slots behind ...
        // TODO reduce 4000 to 0
        let slot = 231541684;
        let delta = 50 + rand::random::<u64>() % 100;
        let query_slot = slot - delta;
        info!("query slot (-{}): {}", delta, query_slot);

        let epoch: EpochRef = epoch_data.get_epoch_at_slot(query_slot).into();

        let futures = (0..3)
            .map(|i| {
                let si = rand::random::<usize>() % sessions.len();
                let session = sessions[si].clone();
                query_database(session, epoch, query_slot + i)
            })
            .collect_vec();

        futures_util::future::join_all(futures).await;

        ticker.tick().await;
        let result = rx.changed().await;
        if result.is_err() {
            bail!("Watcher failed - sender was dropped!");
        }
    }

    unreachable!()
}

async fn query_database(postgres_session: PostgresSession, epoch: EpochRef, slot: Slot) {
    let statement = format!(
        r#"
                SELECT 1 FROM {schema}.transactions WHERE slot = {slot}
            "#,
        schema = PostgresEpoch::build_schema_name(epoch),
        slot = slot,
    );

    let started = tokio::time::Instant::now();
    let result = postgres_session.query_list(&statement, &[]).await.unwrap();
    info!(
        "result= {} (took {:.3}ms)",
        result.len(),
        started.elapsed().as_secs_f64() * 1000.0
    );
}

fn slot_listener(slot_notifier: SlotStream, watch: Sender<Slot>) -> JoinHandle<()> {
    let join_handle = tokio::spawn(async move {
        let mut slot_notifier = slot_notifier;
        loop {
            match slot_notifier.recv().await {
                Ok(slot_update) => {
                    // info!("slot -> {}", slot_update.processed_slot);
                    watch.send(slot_update.processed_slot).unwrap();
                }
                Err(_err) => {}
            }
        }
    });

    join_handle
}
