use std::{collections::HashSet, ops::Mul, str::FromStr, sync::Arc, time::Duration};

use clap::Parser;
use dashmap::{DashMap, DashSet};
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
        transaction_sent_info::SentTransactionInfo,
    },
    utils::wait_till_block_of_commitment_is_recieved,
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
    native_token::LAMPORTS_PER_SOL,
    pubkey::Pubkey,
    signature::{Keypair, Signature},
    signer::Signer,
    system_instruction,
    transaction::Transaction,
};
use tokio::sync::{Mutex, RwLock};

use crate::cli::Args;
use solana_rpc_client::nonblocking::rpc_client::RpcClient;

mod cli;

const MEMO_PROGRAM_ID: &str = "MemoSq4gqABAXKb96qnH8TysNcWxMyWCqXgDLGmfcHr";

pub fn create_memo_tx(msg: &[u8], payer: &Keypair, blockhash: Hash, prio_fees: u64) -> Transaction {
    let memo = Pubkey::from_str(MEMO_PROGRAM_ID).unwrap();

    let cb_1 = compute_budget::ComputeBudgetInstruction::set_compute_unit_limit(7000);
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

pub async fn create_signers_from_payer(
    rpc_client: Arc<RpcClient>,
    payer: Arc<Keypair>,
    nb_signers: usize,
    signer_balance: u64,
    prio_fees: u64,
) -> Vec<Arc<Keypair>> {
    let signers = (0..nb_signers)
        .map(|_| Arc::new(Keypair::new()))
        .collect_vec();
    let mut signers_to_transfer: HashSet<Pubkey> = signers.iter().map(|kp| kp.pubkey()).collect();

    while !signers_to_transfer.is_empty() {
        let Ok(blockhash) = rpc_client.get_latest_blockhash().await else {
            tokio::time::sleep(Duration::from_secs(1)).await;
            continue;
        };

        let cb_1 = compute_budget::ComputeBudgetInstruction::set_compute_unit_limit(5000);
        let cb_2 = compute_budget::ComputeBudgetInstruction::set_compute_unit_price(prio_fees);

        let transactions = signers_to_transfer
            .iter()
            .map(|signer| {
                let instruction =
                    system_instruction::transfer(&payer.pubkey(), signer, signer_balance);
                let message = Message::new(
                    &[cb_1.clone(), cb_2.clone(), instruction],
                    Some(&payer.pubkey()),
                );
                (*signer, Transaction::new(&[&payer], message, blockhash))
            })
            .collect_vec();
        let tasks = transactions
            .iter()
            .map(|(signer, tx)| {
                let rpc_client = rpc_client.clone();
                let tx = tx.clone();
                let signer = *signer;
                tokio::spawn(
                    async move { (signer, rpc_client.send_and_confirm_transaction(&tx).await) },
                )
            })
            .collect_vec();

        let results = futures::future::join_all(tasks).await;
        for result in results {
            match result {
                Ok((signer, Ok(_signature))) => {
                    signers_to_transfer.remove(&signer);
                }
                Ok((signer, Err(e))) => {
                    log::error!("Error transfering to {signer:?}, {e:?}");
                }
                _ => {
                    // retry
                }
            }
        }
    }
    signers
}

pub async fn transfer_back_to_payer(
    rpc_client: Arc<RpcClient>,
    payer: Arc<Keypair>,
    signers: Vec<Arc<Keypair>>,
    prio_fees: u64,
) {
    let mut signers_to_transfer: HashSet<Pubkey> = signers.iter().map(|kp| kp.pubkey()).collect();
    let payer_pubkey = payer.pubkey();

    while !signers_to_transfer.is_empty() {
        let Ok(blockhash) = rpc_client.get_latest_blockhash().await else {
            tokio::time::sleep(Duration::from_secs(1)).await;
            continue;
        };

        let transfers = signers
            .iter()
            .map(|signer| {
                let rpc_client = rpc_client.clone();
                let signer = signer.clone();
                tokio::spawn(async move {
                    let balance = rpc_client.get_balance(&signer.pubkey()).await.unwrap();

                    let cb_1 =
                        compute_budget::ComputeBudgetInstruction::set_compute_unit_limit(5000);
                    let cb_2 =
                        compute_budget::ComputeBudgetInstruction::set_compute_unit_price(prio_fees);
                    let balance_to_transfer = balance
                        .saturating_sub(5000)
                        .saturating_sub(5000 * prio_fees);
                    let instruction = system_instruction::transfer(
                        &signer.pubkey(),
                        &payer_pubkey,
                        balance_to_transfer,
                    );
                    let message = Message::new(
                        &[cb_1.clone(), cb_2.clone(), instruction],
                        Some(&signer.pubkey()),
                    );
                    (
                        signer.pubkey(),
                        rpc_client
                            .send_and_confirm_transaction(&Transaction::new(
                                &[&signer],
                                message,
                                blockhash,
                            ))
                            .await,
                    )
                })
            })
            .collect_vec();

        let results = futures::future::join_all(transfers).await;
        for result in results {
            match result {
                Ok((signer, Ok(_signature))) => {
                    signers_to_transfer.remove(&signer);
                }
                Ok((signer, Err(e))) => {
                    log::error!("Error transfering to {signer:?}, {e:?}");
                }
                _ => {
                    // retry
                }
            }
        }
    }
}

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    tracing_subscriber::fmt::init();
    let args = Args::parse();

    let rpc_url = args.rpc_url;
    let rpc_client = Arc::new(RpcClient::new(rpc_url));

    let leader_schedule = Arc::new(JsonRpcLeaderGetter::new(rpc_client.clone(), 1024, 128));

    let fee_payer = Arc::new(
        load_identity_keypair(Some(args.fee_payer))
            .await
            .expect("Payer should be set or keypair file not found")
            .unwrap(),
    );

    let priority_fee = args.priority_fees.unwrap_or_default();
    let nb_signers = args.additional_signers;
    let signer_balance = args.signers_transfer_balance.mul(LAMPORTS_PER_SOL as f64) as u64;
    let signers = create_signers_from_payer(
        rpc_client.clone(),
        fee_payer.clone(),
        nb_signers,
        signer_balance,
        priority_fee,
    )
    .await;

    println!(
        "Creating {} users with {} SOL balance",
        nb_signers, signer_balance
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

    let finalized_block_information = wait_till_block_of_commitment_is_recieved(
        endpoints.blockinfo_notifier.resubscribe(),
        CommitmentConfig::finalized(),
    )
    .await;

    let block_height = rpc_client
        .get_block_height_with_commitment(CommitmentConfig::finalized())
        .await?;
    let (blockhash, _) = rpc_client
        .get_latest_blockhash_with_commitment(CommitmentConfig::finalized())
        .await?;
    let finalize_slot = finalized_block_information.slot;

    println!(
        "finalized blockheight : {:?}, slot: {}, hash: {}",
        finalized_block_information.block_height,
        finalized_block_information.slot,
        finalized_block_information.blockhash
    );
    println!(
        "From RPC blockheight : {block_height:?}, hash: {}",
        blockhash
    );

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
        endpoints.blocks_notifier.resubscribe(),
        endpoints.blockinfo_notifier,
        endpoints.slot_notifier.resubscribe(),
        endpoints.cluster_info_notifier,
        endpoints.vote_account_notifier,
    );

    let count = args.transaction_count.unwrap_or(10);
    let prioritization_heap_size = Some(count * args.number_of_seconds);

    let tpu_config = TpuServiceConfig {
        fanout_slots: args.fanout_slots.unwrap_or(16),
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
            prioritization_heap_size,
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

    let memo_msgs = generate_random_strings(count * args.number_of_seconds, None, 5);

    let mut tx_to_confirm = vec![];
    let mut second = 1;
    let mut signer_count = 0;
    let map_of_signature = Arc::new(DashSet::<Signature>::new());
    let transactions_in_blocks = Arc::new(Mutex::new(Vec::<u64>::new()));
    let mut block_stream = endpoints.blocks_notifier;

    let block_tps_task = {
        let map_of_signature = map_of_signature.clone();
        let transactions_in_blocks = transactions_in_blocks.clone();
        tokio::spawn(async move {
            let mut start_tracking = false;
            while let Ok(block) = block_stream.recv().await {
                let mut count = 0;
                for transaction in &block.transactions {
                    if map_of_signature.contains(&transaction.signature) {
                        count += 1;
                        map_of_signature.remove(&transaction.signature);
                    }
                }

                // start tracking once we have first block with some transactions sent by us
                if start_tracking || count > 0 {
                    start_tracking = true;
                    let mut lk = transactions_in_blocks.lock().await;
                    lk.push(count);
                }
            }
        })
    };

    for chunk in memo_msgs.chunks(count) {
        let instant = tokio::time::Instant::now();
        let mut current_txs = vec![];
        println!("Sending memo transactions :{}", second);
        second += 1;
        let bh = data_cache
            .block_information_store
            .get_latest_blockhash(CommitmentConfig::finalized())
            .await;
        let last_valid_block_height =
            data_cache.block_information_store.get_last_blockheight() + 300;
        let transactions = chunk
            .iter()
            .map(|x| {
                signer_count += 1;
                create_memo_tx(x, &signers[signer_count % nb_signers], bh, priority_fee)
            })
            .collect_vec();

        for transaction in transactions {
            let signature = transaction.signatures[0];
            let raw_tx = bincode::serialize(&transaction).unwrap();
            let slot = data_cache.slot_cache.get_current_slot();
            map_of_signature.insert(signature);

            let transaction_info = SentTransactionInfo {
                signature,
                last_valid_block_height,
                slot,
                transaction: Arc::new(raw_tx),
                prioritization_fee: priority_fee,
            };
            let _ = transaction_service
                .transaction_channel
                .send(transaction_info.clone())
                .await;
            current_txs.push(signature);
        }

        tx_to_confirm.push(current_txs);

        let millis = instant.elapsed().as_millis() as u64;
        if millis < 1000 {
            tokio::time::sleep(Duration::from_millis(1000 - millis)).await;
        } else {
            println!("took {millis:?} millis to send {count:?} transactions");
        }
    }

    println!(
        "{} memo transactions sent, waiting for a minute to confirm them",
        count * args.number_of_seconds
    );

    tokio::time::sleep(Duration::from_secs(120)).await;

    let mut second = 1;
    for seconds_sigs in tx_to_confirm {
        let mut tx_confirmed = 0;
        for sig in seconds_sigs {
            if data_cache.txs.is_transaction_confirmed(&sig) {
                tx_confirmed += 1;
            }
        }

        println!(
            "{} or {} transactions were confirmed for the {} second",
            tx_confirmed, count, second
        );
        second += 1;
    }

    block_tps_task.abort();

    let lk = transactions_in_blocks.lock().await;
    // stop tracking by removing trailling 0s
    let mut vec = lk.clone();
    vec.reverse();
    let mut transaction_blocks = vec.iter().skip_while(|x| **x == 0).cloned().collect_vec();
    transaction_blocks.reverse();
    println!(
        "BLOCKS transactions : {}",
        transaction_blocks.iter().map(|x| x.to_string()).join(", ")
    );
    let sum = transaction_blocks.iter().sum::<u64>();
    let seconds = (transaction_blocks.len() * 400 / 1000) as u64;
    let tps = sum / seconds;
    println!("EFFECTIVE TPS: {tps:?}");

    println!("Transfering remaining lamports to payer");
    transfer_back_to_payer(rpc_client, fee_payer, signers, priority_fee).await;
    Ok(())
}
