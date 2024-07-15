use std::{collections::HashSet, sync::Arc};

use itertools::Itertools;
use lite_account_manager_common::{
    account_data::{Account, AccountData, AccountNotificationMessage, AccountStream, Data},
    account_filter::{AccountFilterType, AccountFilters},
};
use lite_account_manager_common::{
    account_store_interface::AccountStorageInterface,
    accounts_source_interface::AccountsSourceInterface,
};
use quic_geyser_client::non_blocking::client::Client as QuicClient;
use quic_geyser_common::{
    filters::{
        AccountFilter, AccountFilterType as QuicAccountFilterType, Filter as QuicGeyserFilter,
        MemcmpFilter as QuicGeyserMemcmpFilter,
    },
    types::connections_parameters::ConnectionParameters,
};
use solana_lite_rpc_core::AnyhowJoinHandle;
use solana_sdk::{commitment_config::CommitmentConfig, pubkey::Pubkey};

use crate::quic::quic_subsciption::QUIC_GEYSER_ACCOUNT_NOTIFICATIONS;

pub struct QuicAccountSource {
    client: QuicClient,
    rpc_url: String,
}

#[async_trait::async_trait]
impl AccountsSourceInterface for QuicAccountSource {
    async fn subscribe_accounts(&self, account: HashSet<Pubkey>) -> anyhow::Result<()> {
        let filter = QuicGeyserFilter::Account(AccountFilter {
            accounts: Some(account),
            owner: None,
            filter: None,
        });
        self.client.subscribe(vec![filter]).await
    }

    async fn subscribe_program_accounts(
        &self,
        program_id: Pubkey,
        filters: Option<Vec<AccountFilterType>>,
    ) -> anyhow::Result<()> {
        let filters = filters.map_or(
            vec![QuicGeyserFilter::Account(AccountFilter {
                owner: Some(program_id),
                accounts: None,
                filter: None,
            })],
            |filters| {
                filters
                    .iter()
                    .map(|filter| {
                        let quic_geyser_filter = match filter {
                            AccountFilterType::Datasize(size) => {
                                QuicAccountFilterType::Datasize(*size)
                            }
                            AccountFilterType::Memcmp(memcmp) => {
                                QuicAccountFilterType::Memcmp(QuicGeyserMemcmpFilter {
                                    offset: memcmp.offset,
                                    data: quic_geyser_common::filters::MemcmpFilterData::Bytes(
                                        memcmp.bytes(),
                                    ),
                                })
                            }
                        };
                        QuicGeyserFilter::Account(AccountFilter {
                            owner: Some(program_id),
                            accounts: None,
                            filter: Some(quic_geyser_filter),
                        })
                    })
                    .collect_vec()
            },
        );
        self.client.subscribe(filters).await
    }

    async fn save_snapshot(
        &self,
        storage: Arc<dyn AccountStorageInterface>,
        account_filters: AccountFilters,
    ) -> anyhow::Result<()> {
        crate::rpc_polling::rpc_gpa::get_program_account(
            self.rpc_url.clone(),
            &account_filters,
            100,
            10,
            100,
            storage,
        )
        .await
    }
}

pub async fn create_quic_account_source_endpoint(
    quic_url: String,
    rpc_url: String,
) -> anyhow::Result<(QuicAccountSource, AccountStream, Vec<AnyhowJoinHandle>)> {
    let (client, mut messages, mut tasks) =
        QuicClient::new(quic_url, ConnectionParameters::default()).await?;
    let (account_sx, account_rx) = tokio::sync::broadcast::channel(64 * 1024);

    // start listening to deleted accounts
    client
        .subscribe(vec![QuicGeyserFilter::DeletedAccounts])
        .await?;
    let quic_account_src = QuicAccountSource { client, rpc_url };

    let tasks_jh = tokio::spawn(async move {
        while let Some(message) = messages.recv().await {
            match message {
                quic_geyser_common::message::Message::AccountMsg(account) => {
                    QUIC_GEYSER_ACCOUNT_NOTIFICATIONS.inc();
                    if account_sx.send(AccountNotificationMessage {
                        data: AccountData {
                            pubkey: account.pubkey,
                            account: Arc::new(Account {
                                lamports: account.lamports,
                                data: match account.compression_type {
                                    quic_geyser_common::compression::CompressionType::None => {
                                        Data::Uncompressed(account.data)
                                    }
                                    quic_geyser_common::compression::CompressionType::Lz4Fast(
                                        _
                                    )
                                    | quic_geyser_common::compression::CompressionType::Lz4(_) => {
                                        Data::Lz4 {
                                            binary: account.data,
                                            len: account.data_length as usize,
                                        }
                                    }
                                },
                                owner: account.owner,
                                executable: account.executable,
                                rent_epoch: account.rent_epoch,
                            }),
                            updated_slot: account.slot_identifier.slot,
                            write_version: account.write_version,
                        },
                        commitment: CommitmentConfig::processed(),
                    }).is_err() {
                        log::error!("accounts channel closed");
                        break;
                    }
                }
                _ => {
                    // should not have any other message
                    log::error!("error account processing thread recieved unknown message from geyser : {:?}", message);
                }
            }
        }
        anyhow::bail!("error accounts notification processing task closed");
    });
    tasks.push(tasks_jh);
    Ok((quic_account_src, account_rx, tasks))
}
