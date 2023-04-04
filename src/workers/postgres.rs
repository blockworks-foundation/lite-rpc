use anyhow::{bail, Context};
use chrono::{DateTime, Utc};
use futures::{future::join_all, join};
use log::{info, warn};
use postgres_native_tls::MakeTlsConnector;
use std::{sync::Arc, time::Duration};

use prometheus::{core::GenericGauge, opts, register_int_gauge};
use tokio::{
    sync::{
        mpsc::{UnboundedReceiver, UnboundedSender},
        RwLock, RwLockReadGuard,
    },
    task::JoinHandle,
};
use tokio_postgres::{
    config::SslMode, tls::MakeTlsConnect, types::ToSql, Client, NoTls, Socket, Statement,
};

use native_tls::{Certificate, Identity, TlsConnector};

use crate::encoding::BinaryEncoding;

lazy_static::lazy_static! {
    pub static ref MESSAGES_IN_POSTGRES_CHANNEL: GenericGauge<prometheus::core::AtomicI64> = register_int_gauge!(opts!("literpc_messages_in_postgres", "Number of messages in postgres")).unwrap();
}

#[derive(Debug)]
pub struct PostgresTx {
    pub signature: String,
    pub recent_slot: i64,
    pub forwarded_slot: i64,
    pub forwarded_local_time: DateTime<Utc>,
    pub processed_slot: Option<i64>,
    pub cu_consumed: Option<i64>,
    pub cu_requested: Option<i64>,
    pub quic_response: i16,
}

#[derive(Debug)]
pub struct PostgresUpdateTx {
    pub processed_slot: i64,
    pub cu_consumed: Option<i64>,
    pub cu_requested: Option<i64>,
}

#[derive(Debug)]
pub struct PostgresBlock {
    pub slot: i64,
    pub leader_id: i64,
    pub parent_slot: i64,
    pub cluster_time: DateTime<Utc>,
    pub local_time: Option<DateTime<Utc>>,
}

#[derive(Debug)]
pub struct PostgreAccountAddr {
    pub id: u32,
    pub addr: String,
}

#[derive(Debug)]
pub enum PostgresMsg {
    PostgresTx(Vec<PostgresTx>),
    PostgresBlock(PostgresBlock),
    PostgreAccountAddr(PostgreAccountAddr),
    PostgresUpdateTx(PostgresUpdateTx, String),
}

pub type PostgresMpscRecv = UnboundedReceiver<PostgresMsg>;
pub type PostgresMpscSend = UnboundedSender<PostgresMsg>;

pub struct PostgresSession {
    client: Client,
    update_tx_statement: Statement,
}

impl PostgresSession {
    pub async fn new() -> anyhow::Result<Self> {
        let pg_config = std::env::var("PG_CONFIG").context("env PG_CONFIG not found")?;
        let pg_config = pg_config.parse::<tokio_postgres::Config>()?;

        let client = if let SslMode::Disable = pg_config.get_ssl_mode() {
            Self::spawn_connection(pg_config, NoTls).await?
        } else {
            let ca_pem_b64 = std::env::var("CA_PEM_B64").context("env CA_PEM_B64 not found")?;
            let client_pks_b64 =
                std::env::var("CLIENT_PKS_B64").context("env CLIENT_PKS_B64 not found")?;
            let client_pks_password =
                std::env::var("CLIENT_PKS_PASS").context("env CLIENT_PKS_PASS not found")?;

            let ca_pem = BinaryEncoding::Base64
                .decode(ca_pem_b64)
                .context("ca pem decode")?;

            let client_pks = BinaryEncoding::Base64
                .decode(client_pks_b64)
                .context("client pks decode")?;

            let connector = TlsConnector::builder()
                .add_root_certificate(Certificate::from_pem(&ca_pem)?)
                .identity(
                    Identity::from_pkcs12(&client_pks, &client_pks_password).context("Identity")?,
                )
                .danger_accept_invalid_hostnames(true)
                .danger_accept_invalid_certs(true)
                .build()?;

            Self::spawn_connection(pg_config, MakeTlsConnector::new(connector)).await?
        };

        let update_tx_statement = client
            .prepare(
                r#"
                UPDATE lite_rpc.txs 
                SET processed_slot = $1, processed_cluster_time = $2, processed_local_time = $3, cu_consumed = $4, cu_requested = $5,
                WHERE signature = $6
            "#,
            )
            .await?;

        Ok(Self {
            client,
            update_tx_statement,
        })
    }

    async fn spawn_connection<T>(
        pg_config: tokio_postgres::Config,
        connector: T,
    ) -> anyhow::Result<Client>
    where
        T: MakeTlsConnect<Socket> + Send + 'static,
        <T as MakeTlsConnect<Socket>>::Stream: Send,
    {
        let (client, connection) = pg_config
            .connect(connector)
            .await
            .context("Connecting to Postgres failed")?;

        tokio::spawn(async move {
            if let Err(err) = connection.await {
                log::error!("Connection to Postgres broke {err:?}");
            }
        });

        Ok(client)
    }

    pub fn multiline_query(query: &mut String, args: usize, rows: usize) {
        let mut arg_index = 1usize;
        for row in 0..rows {
            query.push('(');

            for i in 0..args {
                query.push_str(&format!("${arg_index}"));
                arg_index += 1;
                if i != (args - 1) {
                    query.push(',');
                }
            }

            query.push(')');

            if row != (rows - 1) {
                query.push(',');
            }
        }
    }

    pub async fn send_txs(&self, txs: &[PostgresTx]) -> anyhow::Result<()> {
        const NUMBER_OF_ARGS: usize = 7;

        if txs.is_empty() {
            return Ok(());
        }

        let mut args: Vec<&(dyn ToSql + Sync)> = Vec::with_capacity(NUMBER_OF_ARGS * txs.len());

        for tx in txs.iter() {
            let PostgresTx {
                signature,
                recent_slot,
                forwarded_slot,
                forwarded_local_time,
                processed_slot,
                cu_consumed,
                cu_requested,
                quic_response,
            } = tx;

            args.push(signature);
            args.push(recent_slot);
            args.push(forwarded_slot);
            args.push(forwarded_local_time);
            args.push(processed_slot);
            args.push(cu_consumed);
            args.push(cu_requested);
            args.push(quic_response);
        }

        let mut query = String::from(
            r#"
                INSERT INTO lite_rpc.Txs 
                (signature, recent_slot, forwarded_slot, forwarded_local_time, processed_slot, cu_consumed, cu_requested, quic_response)
                VALUES
            "#,
        );

        Self::multiline_query(&mut query, NUMBER_OF_ARGS, txs.len());

        self.client.execute(&query, &args).await?;

        Ok(())
    }

    pub async fn send_blocks(&self, blocks: &[PostgresBlock]) -> anyhow::Result<()> {
        const NUMBER_OF_ARGS: usize = 3;

        if blocks.is_empty() {
            return Ok(());
        }

        let mut args: Vec<&(dyn ToSql + Sync)> = Vec::with_capacity(NUMBER_OF_ARGS * blocks.len());

        for block in blocks.iter() {
            let PostgresBlock {
                slot,
                leader_id,
                parent_slot,
                cluster_time,
                local_time,
            } = block;

            args.push(slot);
            args.push(leader_id);
            args.push(parent_slot);
            args.push(cluster_time);
            args.push(local_time);
        }

        let mut query = String::from(
            r#"
                INSERT INTO lite_rpc.Blocks 
                (slot, leader_id, parent_slot, cluster_time, local_time)
                VALUES
            "#,
        );

        Self::multiline_query(&mut query, NUMBER_OF_ARGS, blocks.len());

        self.client.execute(&query, &args).await?;

        Ok(())
    }

    pub async fn update_tx(&self, tx: &PostgresUpdateTx, signature: &str) -> anyhow::Result<()> {
        let PostgresUpdateTx {
            processed_slot,
            cu_consumed,
            cu_requested,
        } = tx;

        self.client
            .execute(
                &self.update_tx_statement,
                &[processed_slot, cu_consumed, cu_requested, &signature],
            )
            .await?;

        Ok(())
    }
}

pub struct Postgres {
    session: Arc<RwLock<PostgresSession>>,
}

impl Postgres {
    /// # Return
    /// (connection join handle, Self)
    ///
    /// returned join handle is required to be polled
    pub async fn new() -> anyhow::Result<Self> {
        let session = PostgresSession::new().await?;
        let session = Arc::new(RwLock::new(session));

        Ok(Self { session })
    }

    async fn get_session(&mut self) -> anyhow::Result<RwLockReadGuard<PostgresSession>> {
        if self.session.read().await.client.is_closed() {
            *self.session.write().await = PostgresSession::new().await?;
        }

        Ok(self.session.read().await)
    }

    pub fn start(mut self, mut recv: PostgresMpscRecv) -> JoinHandle<anyhow::Result<()>> {
        tokio::spawn(async move {
            info!("Writing to postgres");

            let mut tx_que = Vec::<PostgresTx>::new();
            let mut block_que = Vec::new();
            let mut update_que = Vec::new();

            loop {
                loop {
                    let msg = recv.try_recv();

                    match msg {
                        Ok(msg) => {
                            MESSAGES_IN_POSTGRES_CHANNEL.dec();

                            match msg {
                                PostgresMsg::PostgresTx(mut tx) => tx_que.append(&mut tx),
                                PostgresMsg::PostgresBlock(block) => block_que.push(block),
                                PostgresMsg::PostgresUpdateTx(tx, sig) => {
                                    update_que.push((tx, sig))
                                }
                                PostgresMsg::PostgreAccountAddr(_) => todo!(),
                            }
                        }
                        Err(tokio::sync::mpsc::error::TryRecvError::Empty) => break,
                        Err(tokio::sync::mpsc::error::TryRecvError::Disconnected) => {
                            bail!("Postgres channel broke")
                        }
                    }
                }

                let Ok(session) = self.get_session().await else {
                    const TIME_OUT:Duration = Duration::from_millis(1000);
                    warn!("Unable to get postgres session. Retrying in {TIME_OUT:?}");
                    tokio::time::sleep(TIME_OUT).await;
                    continue;
                };

                let tx_update_fut = update_que
                    .iter()
                    .map(|(tx, sig)| session.update_tx(tx, sig));

                let (res_txs, res_block, res_tx_update) = join!(
                    session.send_txs(&tx_que),
                    session.send_blocks(&block_que),
                    join_all(tx_update_fut)
                );

                if let Err(err) = res_txs {
                    warn!("Error sending tx batch to postgres {err:?}");
                } else {
                    tx_que.clear();
                }

                if let Err(err) = res_block {
                    warn!("Error sending block batch to postgres {err:?}");
                } else {
                    block_que.clear();
                }

                let mut update_que_iter = update_que.into_iter();
                update_que = res_tx_update
                    .iter()
                    .filter_map(|res| {
                        let item = update_que_iter.next();
                        if let Err(err) = res {
                            warn!("Error updating tx to postgres {err:?}");
                            return item;
                        }
                        None
                    })
                    .collect();

                //{
                //    let mut batcher =
                //        Batcher::new(&mut tx_que, MAX_BATCH_SIZE, BatcherStrategy::Start);

                //    while let Some(txs) = batcher.next_batch() {
                //        if let Err(err) = session.send_txs(txs).await {
                //            warn!("Error sending tx batch to postgres {err:?}");
                //        }
                //    }
                //}

                //{
                //    let mut batcher =
                //        Batcher::new(&mut block_que, MAX_BATCH_SIZE, BatcherStrategy::Start);

                //    while let Some(txs) = batcher.next_batch() {
                //        if let Err(err) = session.send_blocks(txs).await {
                //            warn!("Error sending block batch to postgres {err:?}");
                //        }
                //    }
                //}
            }
        })
    }
}

#[test]
fn multiline_query_test() {
    let mut query = String::new();

    PostgresSession::multiline_query(&mut query, 3, 2);
    assert_eq!(query, "($1,$2,$3),($4,$5,$6)");
}
