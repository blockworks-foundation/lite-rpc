///
/// test program to query postgres (get blocks) behind the tip of the slots
///
use anyhow::bail;
use itertools::Itertools;
use log::info;
use solana_lite_rpc_cluster_endpoints::endpoint_stremers::EndpointStreaming;
use solana_lite_rpc_cluster_endpoints::json_rpc_subscription::create_json_rpc_polling_subscription;
use solana_lite_rpc_core::structures::epoch::{EpochCache, EpochRef};
use solana_lite_rpc_core::types::SlotStream;
use solana_lite_rpc_history::block_stores::postgres::{PostgresEpoch, PostgresSession};
use solana_rpc_client::nonblocking::rpc_client::RpcClient;
use solana_sdk::clock::Slot;
use std::sync::Arc;
use tokio::sync::watch;
use tokio::sync::watch::Sender;
use tokio::task::JoinHandle;

const NUM_PARALLEL_TASKS: usize = 1;

#[tokio::main(flavor = "multi_thread", worker_threads = 16)]
pub async fn main() -> anyhow::Result<()> {
    tracing_subscriber::fmt::init();

    let sessions = vec![
        PostgresSession::new_from_env().await.unwrap(),
        // PostgresSession::new_from_env().await.unwrap(),
        // PostgresSession::new_from_env().await.unwrap(),
        // PostgresSession::new_from_env().await.unwrap(),
        // PostgresSession::new_from_env().await.unwrap(),
        // PostgresSession::new_from_env().await.unwrap(),
        // PostgresSession::new_from_env().await.unwrap(),
        // PostgresSession::new_from_env().await.unwrap(),
    ];

    let rpc_url = std::env::var("RPC_URL").expect("env var RPC_URL is mandatory");
    let rpc_client = Arc::new(RpcClient::new(rpc_url));
    let epoch_data = EpochCache::bootstrap_epoch(&rpc_client).await.unwrap();
    let (subscriptions, _cluster_endpoint_tasks) =
        create_json_rpc_polling_subscription(rpc_client.clone(), NUM_PARALLEL_TASKS).unwrap();

    let EndpointStreaming { slot_notifier, .. } = subscriptions;

    let (tx, mut rx) = watch::channel(0);

    let _jh2 = slot_listener(slot_notifier.resubscribe(), tx);

    let mut ticker = tokio::time::interval(tokio::time::Duration::from_secs(1));
    loop {
        let slot = *rx.borrow_and_update();
        if slot == 0 {
            continue;
        }

        info!("processed slot: {}", slot);

        // ATM we are 4000 slots behind ...
        // TODO reduce 4000 to 0
        let slot: u64 = 234332620; // literpc3 - local
                                   // let slot = 231541684;
        let delta = 50 + rand::random::<u64>() % 100;
        let query_slot = slot.saturating_sub(delta);
        info!("query slot (-{}): {}", delta, query_slot);

        let (epoch_cache, _) = &epoch_data;
        let epoch: EpochRef = epoch_cache.get_epoch_at_slot(query_slot).into();

        let futures = (0..3)
            .map(|i| {
                let si = rand::random::<usize>() % sessions.len();
                let session = sessions[si].clone();
                query_database(session, epoch, query_slot + i)
                // query_database_simple(session)
            })
            .collect_vec();

        futures_util::future::join_all(futures).await;

        ticker.tick().await;
        let result = rx.changed().await;
        if result.is_err() {
            bail!("Watcher failed - sender was dropped!");
        }
    }
}

async fn query_database(postgres_session: PostgresSession, epoch: EpochRef, slot: Slot) {
    let statement = format!(
        r#"
                SELECT min(slot),max(slot) FROM {schema}.transactions_todo WHERE slot = {slot}
            "#,
        schema = PostgresEpoch::build_schema_name(epoch),
        slot = slot,
    );

    let started = tokio::time::Instant::now();
    let result = postgres_session.query_list(&statement, &[]).await.unwrap();
    info!(
        "num_rows: {} (took {:.2}ms)",
        result.len(),
        started.elapsed().as_secs_f64() * 1000.0
    );
}

fn slot_listener(slot_notifier: SlotStream, watch: Sender<Slot>) -> JoinHandle<()> {
    tokio::spawn(async move {
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
    })
}
