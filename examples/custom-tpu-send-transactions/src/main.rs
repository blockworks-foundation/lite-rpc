use std::{str::FromStr, sync::Arc, time::Duration};

use clap::Parser;
use dashmap::DashMap;
use itertools::Itertools;
use rand::{
    distributions::{Alphanumeric, Distribution},
    SeedableRng,
};
use solana_lite_rpc_cluster_endpoints::{
    geyser_grpc_connector::{GrpcConnectionTimeouts, GrpcSourceConfig},
    grpc_subscription::create_grpc_subscription,
    json_rpc_leaders_getter::JsonRpcLeaderGetter,
    json_rpc_subscription::create_json_rpc_polling_subscription,
};
use solana_lite_rpc_core::{
    keypair_loader::load_identity_keypair,
    stores::{
        block_information_store::{BlockInformation, BlockInformationStore},
        cluster_info_store::ClusterInfo,
        data_cache::{DataCache, SlotCache},
        subscription_store::SubscriptionStore,
        tx_store::TxStore,
    },
    structures::{
        epoch::EpochCache, identity_stakes::IdentityStakes, leaderschedule::CalculatedSchedule,
    },
    utils::get_latest_block_info,
};
use solana_lite_rpc_services::{
    data_caching_service::DataCachingService,
    quic_connection_utils::QuicConnectionParameters,
    tpu_utils::{
        tpu_connection_path::TpuConnectionPath,
        tpu_service::{TpuService, TpuServiceConfig},
    },
    transaction_replayer::TransactionReplayer,
    transaction_service::TransactionServiceBuilder,
    tx_sender::TxSender,
};
use solana_sdk::{
    commitment_config::CommitmentConfig,
    compute_budget,
    hash::Hash,
    instruction::Instruction,
    message::Message,
    pubkey::Pubkey,
    signature::{Keypair, Signature},
    signer::Signer,
    transaction::Transaction,
};
use tokio::sync::RwLock;

use crate::cli::Args;
use solana_rpc_client::nonblocking::rpc_client::RpcClient;

mod cli;

const MEMO_PROGRAM_ID: &str = "MemoSq4gqABAXKb96qnH8TysNcWxMyWCqXgDLGmfcHr";

pub fn create_memo_tx(msg: &[u8], payer: &Keypair, blockhash: Hash, prio_fees: u64) -> Transaction {
    let memo = Pubkey::from_str(MEMO_PROGRAM_ID).unwrap();

    let cb_1 = compute_budget::ComputeBudgetInstruction::set_compute_unit_limit(20000);
    let cb_2 = compute_budget::ComputeBudgetInstruction::set_compute_unit_price(prio_fees);
    let instruction = Instruction::new_with_bytes(memo, msg, vec![]);
    let message = Message::new(&[cb_1, cb_2, instruction], Some(&payer.pubkey()));
    Transaction::new(&[payer], message, blockhash)
}

pub fn generate_random_strings(
    num_of_txs: usize,
    random_seed: Option<u64>,
    n_chars: usize,
) -> Vec<Vec<u8>> {
    let seed = random_seed.map_or(0, |x| x);
    let mut rng = rand_chacha::ChaCha8Rng::seed_from_u64(seed);
    (0..num_of_txs)
        .map(|_| Alphanumeric.sample_iter(&mut rng).take(n_chars).collect())
        .collect()
}

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    let args = Args::parse();

    let rpc_url = args.rpc_url;
    let rpc_client = Arc::new(RpcClient::new(rpc_url));

    let leader_schedule = Arc::new(JsonRpcLeaderGetter::new(rpc_client.clone(), 1024, 128));

    let payer = Arc::new(
        load_identity_keypair(Some(args.fee_payer))
            .await
            .expect("Payer should be set or keypair file not found")
            .unwrap(),
    );


    let validator_identity = Arc::new(
        load_identity_keypair(args.staked_identity)
            .await?
            .unwrap_or_else(Keypair::new),
    );

    // START ALL SERVICES REQUIRED BY LITE_RPC
    // setup endpoint, GRPC/RPC Polling
    println!("Setting up lite-rpc tpu service");
    let (endpoints, _handles) = if let Some(grpc_addr) = args.grpc_url {
        let timeouts = GrpcConnectionTimeouts {
            connect_timeout: Duration::from_secs(10),
            request_timeout: Duration::from_secs(10),
            subscribe_timeout: Duration::from_secs(10),
            receive_timeout: Duration::from_secs(10),
        };
        create_grpc_subscription(
            rpc_client.clone(),
            vec![GrpcSourceConfig::new(
                grpc_addr,
                args.x_token.clone(),
                None,
                timeouts,
            )],
            vec![],
        )?
    } else {
        create_json_rpc_polling_subscription(rpc_client.clone(), 100)?
    };

    let finalized_block_information = get_latest_block_info(
        endpoints.blockinfo_notifier.resubscribe(),
        CommitmentConfig::finalized(),
    )
    .await;

    let block_height = rpc_client.get_block_height_with_commitment(CommitmentConfig::finalized()).await?;
    let (blockhash, _) = rpc_client.get_latest_blockhash_with_commitment(CommitmentConfig::finalized()).await?;
    let finalize_slot = finalized_block_information.slot;

    println!("finalized blockheight : {:?}, slot: {}, hash: {}", finalized_block_information.block_height, finalized_block_information.slot, finalized_block_information.blockhash.to_string());
    println!("From RPC blockheight : {block_height:?}, hash: {}", blockhash);

    let finalized_block_information = BlockInformation {
        slot: finalized_block_information.slot,
        block_height,
        last_valid_blockheight: finalized_block_information.block_height + 300,
        cleanup_slot: finalized_block_information.slot + 1000000,
        blockhash: finalized_block_information.blockhash,
        commitment_config: CommitmentConfig::finalized(),
        block_time: 0,
    };

    let block_information_store = BlockInformationStore::new(finalized_block_information);

    let data_cache = DataCache {
        block_information_store,
        cluster_info: ClusterInfo::default(),
        identity_stakes: IdentityStakes::new(validator_identity.pubkey()),
        slot_cache: SlotCache::new(finalize_slot),
        tx_subs: SubscriptionStore::default(),
        txs: TxStore {
            store: Arc::new(DashMap::new()),
        },
        epoch_data: EpochCache::new_for_tests(),
        leader_schedule: Arc::new(RwLock::new(CalculatedSchedule::default())),
    };
    
    let data_cache_service = DataCachingService {
        data_cache: data_cache.clone(),
        clean_duration: Duration::from_secs(120),
    };

    // start listning the cluster data and filling the cache
    data_cache_service.listen(
        endpoints.blocks_notifier,
        endpoints.blockinfo_notifier,
        endpoints.slot_notifier.resubscribe(),
        endpoints.cluster_info_notifier,
        endpoints.vote_account_notifier,
    );

    let tpu_config = TpuServiceConfig {
        fanout_slots: 100,
        maximum_transaction_in_queue: 2000000,
        quic_connection_params: QuicConnectionParameters {
            connection_timeout: Duration::from_secs(60),
            connection_retry_count: 10,
            finalize_timeout: Duration::from_millis(10000),
            max_number_of_connections: 4,
            unistream_timeout: Duration::from_millis(1000),
            write_timeout: Duration::from_secs(10),
            number_of_transactions_per_unistream: 1,
            unistreams_to_create_new_connection_in_percentage: 5,
        },
        tpu_connection_path: TpuConnectionPath::QuicDirectPath,
    };

    let tpu_service: TpuService = TpuService::new(
        tpu_config,
        validator_identity,
        leader_schedule,
        data_cache.clone(),
    )
    .await?;
    let transaction_service_builder = TransactionServiceBuilder::new(
        TxSender::new(data_cache.clone(), tpu_service.clone()),
        TransactionReplayer::new(
            tpu_service.clone(),
            data_cache.clone(),
            Duration::from_secs(1),
        ),
        tpu_service,
        10000,
    );
    let (transaction_service, _) = transaction_service_builder.start(
        None,
        data_cache.block_information_store.clone(),
        10,
        endpoints.slot_notifier,
    );

    // CREATE TRANSACTIONS
    log::info!("Creating memo transactions");
    let count = args.transaction_count.unwrap_or(10);

    let memo_msgs = generate_random_strings(count * args.number_of_seconds, None, 5);


    let mut tx_to_confirm = vec![];
    let mut second = 1;
    for chunk in memo_msgs.chunks(count) {
        let instant = tokio::time::Instant::now();
        let mut current_txs = vec![];
        println!("Sending memo transactions :{}", second);
        second += 1;
        let bh = data_cache.block_information_store.get_latest_blockhash(CommitmentConfig::finalized()).await;
        let transactions = chunk
        .iter()
        .map(|x| {
            create_memo_tx(
                x,
                &payer,
                bh,
                args.priority_fees.unwrap_or_default(),
            )
        })
        .collect_vec();
        
        for transaction in transactions {
            let sig = transaction_service
                .send_transaction(transaction, Some(10))
                .await?;
            current_txs.push(Signature::from_str(&sig).unwrap());
        }

        tx_to_confirm.push(current_txs);

        let millis = instant.elapsed().as_millis() as u64;
        if millis < 1000 {
            tokio::time::sleep(Duration::from_millis(1000 - millis)).await;
        } else {
            println!( "took {millis:?} millis to send {count:?} transactions" );
        }
    }

    println!(
        "{} memo transactions sent, waiting for a minute to confirm them",
        count
    );

    tokio::time::sleep(Duration::from_secs(60)).await;

    let mut second = 1;
    for seconds_sigs in tx_to_confirm {

        let mut tx_confirmed = 0;
        for sig in seconds_sigs {
            if data_cache.txs.is_transaction_confirmed(&sig) {
                tx_confirmed += 1;
            }
        }

        println!("{} or {} transactions were confirmed for the {} second", tx_confirmed, count, second);
        second += 1;
    }
    Ok(())
}
