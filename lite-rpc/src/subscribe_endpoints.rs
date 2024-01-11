use std::{sync::Arc, thread::JoinHandle};

use solana_lite_rpc_cluster_endpoints::endpoint_stremers::EndpointStreaming;
use solana_lite_rpc_core::{stores::data_cache::DataCache, AnyhowJoinHandle, structures::notifications::NotificationSender};
use solana_lite_rpc_services::{data_caching_service::DataCachingService, tpu_utils::tpu_service::TpuService, tx_sender::{TxSender, self}, transaction_replayer::TransactionReplayer};
use solana_rpc_client::nonblocking::rpc_client::{self, RpcClient};

use crate::{service_spawner::ServiceSpawner, DEFAULT_MAX_NUMBER_OF_TXS_IN_QUEUE};

pub async fn subscribe_to_endpoints(
    subscriptions: EndpointStreaming,
    data_cache_service: DataCachingService,
    tpu_service: TpuService,
    service_spawner: ServiceSpawner,
    tx_sender: TxSender,
    tx_replayer: TransactionReplayer,
    maximum_retries_per_tx: usize,
    notification_channel: Option<NotificationSender>,
) -> anyhow::Result<Vec<AnyhowJoinHandle>> {
    let EndpointStreaming {
        blocks_notifier,
        cluster_info_notifier,
        slot_notifier,
        vote_account_notifier,
    } = subscriptions;

    // to avoid laggin we resubscribe to block notification
    let mut services = data_cache_service.listen(
        blocks_notifier.resubscribe(),
        slot_notifier.resubscribe(),
        cluster_info_notifier,
        vote_account_notifier,
    );
    drop(blocks_notifier);

    //init grpc leader schedule and vote account is configured.
    let (leader_schedule, rpc_stakes_send): (Arc<dyn LeaderFetcherInterface>, Option<_>) =
        if use_grpc && calculate_leader_schedule_form_geyser {
            //init leader schedule grpc process.

            //1) get stored leader schedule and stakes (or via RPC if not present)
            solana_lite_rpc_stakevote::bootstrap_literpc_leader_schedule(
                rpc_client.url(),
                &data_cache,
                current_epoch_info.epoch,
            )
            .await;

            //2) start stake vote and leader schedule.
            let (rpc_stakes_send, rpc_stakes_recv) = mpsc::channel(1000);
            let stake_vote_jh = solana_lite_rpc_stakevote::start_stakes_and_votes_loop(
                data_cache.clone(),
                slot_notifier.resubscribe(),
                rpc_stakes_recv,
                Arc::clone(&rpc_client),
                grpc_addr,
            )
            .await?;

            //
            tokio::spawn(async move {
                let err = stake_vote_jh.await;
                log::error!("Vote and stake Services exit with error: {err:?}");
            });

            (
                Arc::new(GrpcLeaderGetter::new(
                    Arc::clone(&data_cache.leader_schedule),
                    data_cache.epoch_data.clone(),
                )),
                Some(rpc_stakes_send),
            )
        } else {
            (
                Arc::new(JsonRpcLeaderGetter::new(rpc_client.clone(), 1024, 128)),
                None,
            )
        };
    let (transaction_service, tx_service_jh) = service_spawner.spawn_tx_service(
        tx_sender,
        tx_replayer,
        tpu_service,
        DEFAULT_MAX_NUMBER_OF_TXS_IN_QUEUE,
        notification_channel,
        maximum_retries_per_tx,
        slot_notifier.resubscribe(),
    );

    drop(slot_notifier);

    services.push(tx_service_jh);
    Ok(services)
}
