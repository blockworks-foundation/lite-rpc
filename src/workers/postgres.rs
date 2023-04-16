use anyhow::{bail, Context};
use chrono::{DateTime, Utc};
use futures::join;
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
use tokio_postgres::{config::SslMode, tls::MakeTlsConnect, types::ToSql, Client, NoTls, Socket};

use native_tls::{Certificate, Identity, TlsConnector};

use crate::encoding::BinaryEncoding;

lazy_static::lazy_static! {
    pub static ref MESSAGES_IN_POSTGRES_CHANNEL: GenericGauge<prometheus::core::AtomicI64> = register_int_gauge!(opts!("literpc_messages_in_postgres", "Number of messages in postgres")).unwrap();
    pub static ref POSTGRES_SESSION_ERRORS: GenericGauge<prometheus::core::AtomicI64> = register_int_gauge!(opts!("literpc_session_errors", "Number of failures while establishing postgres session")).unwrap();
}

const MAX_QUERY_SIZE: usize = 200_000; // 0.2 mb

trait SchemaSize {
    const DEFAULT_SIZE: usize = 0;
    const MAX_SIZE: usize = 0;
}

const fn get_max_safe_inserts<T: SchemaSize>() -> usize {
    if T::DEFAULT_SIZE == 0 {
        panic!("DEFAULT_SIZE can't be 0. SchemaSize impl should override the DEFAULT_SIZE const");
    }

    MAX_QUERY_SIZE / T::DEFAULT_SIZE
}

const fn get_max_safe_updates<T: SchemaSize>() -> usize {
    if T::MAX_SIZE == 0 {
        panic!("MAX_SIZE can't be 0. SchemaSize impl should override the MAX_SIZE const");
    }

    MAX_QUERY_SIZE / T::MAX_SIZE
}

#[derive(Debug)]
pub struct PostgresTx {
    pub signature: String,                   // 88 bytes
    pub recent_slot: i64,                    // 8 bytes
    pub forwarded_slot: i64,                 // 8 bytes
    pub forwarded_local_time: DateTime<Utc>, // 8 bytes
    pub processed_slot: Option<i64>,
    pub cu_consumed: Option<i64>,
    pub cu_requested: Option<i64>,
    pub quic_response: i16, // 8 bytes
}

impl SchemaSize for PostgresTx {
    const DEFAULT_SIZE: usize = 88 + (4 * 8);
    const MAX_SIZE: usize = Self::DEFAULT_SIZE + (3 * 8);
}

#[derive(Debug)]
pub struct PostgresUpdateTx {
    pub signature: String,   // 88 bytes
    pub processed_slot: i64, // 8 bytes
    pub cu_consumed: Option<i64>,
    pub cu_requested: Option<i64>,
}

impl SchemaSize for PostgresUpdateTx {
    const DEFAULT_SIZE: usize = 88 + 8;
    const MAX_SIZE: usize = Self::DEFAULT_SIZE + (2 * 8);
}

#[derive(Debug)]
pub struct PostgresBlock {
    pub slot: i64,                   // 8 bytes
    pub leader_id: i64,              // 8 bytes
    pub parent_slot: i64,            // 8 bytes
    pub cluster_time: DateTime<Utc>, // 8 bytes
    pub local_time: Option<DateTime<Utc>>,
}

impl SchemaSize for PostgresBlock {
    const DEFAULT_SIZE: usize = 4 * 8;
    const MAX_SIZE: usize = Self::DEFAULT_SIZE + 8;
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
    PostgresUpdateTx(Vec<PostgresUpdateTx>),
}

pub type PostgresMpscRecv = UnboundedReceiver<PostgresMsg>;
pub type PostgresMpscSend = UnboundedSender<PostgresMsg>;

pub struct PostgresSession {
    client: Client,
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

        Ok(Self { client })
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
            log::info!("Connecting to Postgres");

            if let Err(err) = connection.await {
                log::error!("Connection to Postgres broke {err:?}");
                return;
            }

            unreachable!("Postgres thread returned")
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
        const NUMBER_OF_ARGS: usize = 8;

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
        const NUMBER_OF_ARGS: usize = 5;

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

    pub async fn update_txs(&self, txs: &[PostgresUpdateTx]) -> anyhow::Result<()> {
        const NUMBER_OF_ARGS: usize = 4;

        if txs.is_empty() {
            return Ok(());
        }

        let mut args: Vec<&(dyn ToSql + Sync)> = Vec::with_capacity(NUMBER_OF_ARGS * txs.len());

        for tx in txs.iter() {
            let PostgresUpdateTx {
                signature,
                processed_slot,
                cu_consumed,
                cu_requested,
            } = tx;

            args.push(signature);
            args.push(processed_slot);
            args.push(cu_consumed);
            args.push(cu_requested);
        }

        let mut query = String::from(
            r#"
                UPDATE lite_rpc.Txs as t1 set
                    processed_slot  = t2.processed_slot,
                    cu_consumed = t2.cu_consumed,
                    cu_requested = t2.cu_requested
                FROM (VALUES
            "#,
        );

        Self::multiline_query(&mut query, NUMBER_OF_ARGS, txs.len());

        query.push_str(
            r#"
                ) AS t2(signature, processed_slot, cu_consumed, cu_requested)
                WHERE t1.signature = t2.signature
            "#,
        );

        if let Err(err) = self.client.execute(&query, &args).await {
            return Err(anyhow::format_err!("could not execute query={query} err={err:?}"));
        }

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
            info!("start postgres worker");

            const TX_MAX_CAPACITY: usize = get_max_safe_inserts::<PostgresTx>();
            const BLOCK_MAX_CAPACITY: usize = get_max_safe_inserts::<PostgresBlock>();
            const UPDATE_MAX_CAPACITY: usize = get_max_safe_updates::<PostgresUpdateTx>();

            let mut tx_batch: Vec<PostgresTx> = Vec::with_capacity(TX_MAX_CAPACITY);
            let mut block_batch: Vec<PostgresBlock> = Vec::with_capacity(BLOCK_MAX_CAPACITY);
            let mut update_batch = Vec::<PostgresUpdateTx>::with_capacity(UPDATE_MAX_CAPACITY);

            let mut session_establish_error = false;

            loop {
                // drain channel until we reach max capacity for any statement type
                loop {
                    if session_establish_error {
                        break;
                    }

                    // check for capacity
                    if tx_batch.len() >= TX_MAX_CAPACITY
                        || block_batch.len() >= BLOCK_MAX_CAPACITY
                        || update_batch.len() >= UPDATE_MAX_CAPACITY
                    {
                        break;
                    }

                    match recv.try_recv() {
                        Ok(msg) => {
                            MESSAGES_IN_POSTGRES_CHANNEL.dec();

                            match msg {
                                PostgresMsg::PostgresTx(mut tx) => tx_batch.append(&mut tx),
                                PostgresMsg::PostgresBlock(block) => block_batch.push(block),
                                PostgresMsg::PostgresUpdateTx(mut update) => {
                                    update_batch.append(&mut update)
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

                // if there's nothing to do, yield for a brief time
                if tx_batch.is_empty() && block_batch.is_empty() && update_batch.is_empty() {
                    tokio::time::sleep(Duration::from_millis(10)).await;
                    continue;
                }

                // Establish session with postgres or get an existing one
                let session = self.get_session().await;
                session_establish_error = session.is_err();

                let Ok(session) = session else {
                    POSTGRES_SESSION_ERRORS.inc();

                    const TIME_OUT:Duration = Duration::from_millis(1000);
                    warn!("Unable to get postgres session. Retrying in {TIME_OUT:?}");
                    tokio::time::sleep(TIME_OUT).await;

                    continue;
                };

                POSTGRES_SESSION_ERRORS.set(0);

                // write to database when a successful connection is made
                let (res_txs, res_blocks, res_update) = join!(
                    session.send_txs(&tx_batch),
                    session.send_blocks(&block_batch),
                    session.update_txs(&update_batch)
                );

                // clear batches only if results were successful
                if let Err(err) = res_txs {
                    warn!(
                        "Error sending tx batch ({:?}) to postgres {err:?}",
                        tx_batch.len()
                    );
                } else {
                    tx_batch.clear();
                }
                if let Err(err) = res_blocks {
                    warn!(
                        "Error sending block batch ({:?}) to postgres {err:?}",
                        block_batch.len()
                    );
                } else {
                    block_batch.clear();
                }
                if let Err(err) = res_update {
                    warn!(
                        "Error sending update batch ({:?}) to postgres {err:?}",
                        update_batch.len()
                    );
                } else {
                    update_batch.clear();
                }
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
