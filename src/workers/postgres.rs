use std::sync::Arc;

use anyhow::{bail, Context, Ok};
use log::{info, warn};
use postgres_native_tls::MakeTlsConnector;

use prometheus::{core::GenericGauge, opts, register_int_gauge};
use tokio::{
    sync::{
        mpsc::{UnboundedReceiver, UnboundedSender},
        RwLock, RwLockReadGuard,
    },
    task::JoinHandle,
};
use tokio_postgres::{Client, Statement};

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
}

#[derive(Debug)]
pub struct PostgreAccountAddr {
    pub id: u32,
    pub addr: String,
}

#[derive(Debug)]
pub enum PostgresMsg {
    PostgresTx(PostgresTx),
    PostgresBlock(PostgresBlock),
    PostgreAccountAddr(PostgreAccountAddr),
    PostgresUpdateTx(PostgresUpdateTx, String),
}

pub type PostgresMpscRecv = UnboundedReceiver<PostgresMsg>;
pub type PostgresMpscSend = UnboundedSender<PostgresMsg>;

pub struct PostgresSession {
    client: Client,
    insert_tx_statement: Statement,
    update_tx_statement: Statement,
    insert_block_statement: Statement,
}

impl PostgresSession {
    pub async fn new() -> anyhow::Result<Self> {
        let ca_pem_b64 = std::env::var("CA_PEM_B64").context("env CA_PEM_B64 not found")?;
        let client_pks_b64 =
            std::env::var("CLIENT_PKS_B64").context("env CLIENT_PKS_B64 not found")?;
        let client_pks_password =
            std::env::var("CLIENT_PKS_PASS").context("env CLIENT_PKS_PASS not found")?;
        let pg_config = std::env::var("PG_CONFIG").context("env PG_CONFIG not found")?;

        let ca_pem = BinaryEncoding::Base64
            .decode(ca_pem_b64)
            .context("ca pem decode")?;

        let client_pks = BinaryEncoding::Base64
            .decode(client_pks_b64)
            .context("client pks decode")?;

        let connector = TlsConnector::builder()
            .add_root_certificate(Certificate::from_pem(&ca_pem)?)
            .identity(Identity::from_pkcs12(&client_pks, &client_pks_password).context("Identity")?)
            .danger_accept_invalid_hostnames(true)
            .danger_accept_invalid_certs(true)
            .build()?;

        let connector = MakeTlsConnector::new(connector);
        let (client, connection) = tokio_postgres::connect(&pg_config, connector.clone()).await?;

        let insert_block_statement = client
            .prepare(
                r#"
                INSERT INTO lite_rpc.Blocks 
                (slot, leader_id, parent_slot)
                VALUES
                ($1, $2, $3)
            "#,
            )
            .await?;

        let insert_tx_statement = client.prepare(
            r#"
                INSERT INTO lite_rpc.Txs 
                (signature, recent_slot, forwarded_slot, processed_slot, cu_consumed, cu_requested, quic_response)
                VALUES
                ($1, $2, $3, $4, $5, $6, $7)
            "#,
        ).await?;

        let update_tx_statement = client
            .prepare(
                r#"
                UPDATE lite_rpc.txs 
                SET processed_slot = $1, cu_consumed = $2, cu_requested = $3
                WHERE signature = $4
            "#,
            )
            .await?;

        tokio::spawn(async move {
            if let Err(err) = connection.await {
                log::error!("Connection to Postgres broke {err:?}");
            };
        });

        Ok(Self {
            client,
            insert_tx_statement,
            insert_block_statement,
            update_tx_statement,
        })
    }

    pub async fn send_block(&self, block: PostgresBlock) -> anyhow::Result<()> {
        let PostgresBlock {
            slot,
            leader_id,
            parent_slot,
        } = block;

        self.client
            .execute(
                &self.insert_block_statement,
                &[&slot, &leader_id, &parent_slot],
            )
            .await?;

        Ok(())
    }

    pub async fn send_tx(&self, tx: PostgresTx) -> anyhow::Result<()> {
        let PostgresTx {
            signature,
            recent_slot,
            forwarded_slot,
            processed_slot,
            cu_consumed,
            cu_requested,
            quic_response,
        } = tx;

        self.client
            .execute(
                &self.insert_tx_statement,
                &[
                    &signature,
                    &recent_slot,
                    &forwarded_slot,
                    &processed_slot,
                    &cu_consumed,
                    &cu_requested,
                    &quic_response,
                ],
            )
            .await?;

        Ok(())
    }

    pub async fn update_tx(&self, tx: PostgresUpdateTx, signature: &str) -> anyhow::Result<()> {
        let PostgresUpdateTx {
            processed_slot,
            cu_consumed,
            cu_requested,
        } = tx;

        self.client
            .execute(
                &self.update_tx_statement,
                &[&processed_slot, &cu_consumed, &cu_requested, &signature],
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

    pub fn start(self, mut recv: PostgresMpscRecv) -> JoinHandle<anyhow::Result<()>> {
        tokio::spawn(async move {
            info!("Writing to postgres");

            while let Some(msg) = recv.recv().await {
                MESSAGES_IN_POSTGRES_CHANNEL.inc();
                let session = self.get_session().await;

                let Err(err) = (
                    match msg {
                    PostgresMsg::PostgresTx(tx) => session.send_tx(tx).await,
                    PostgresMsg::PostgresUpdateTx(tx, sig) => session.update_tx(tx, &sig).await,
                    PostgresMsg::PostgresBlock(block) => session.send_block(block).await,
                    PostgresMsg::PostgreAccountAddr(_) => todo!(),
                } ) else {
                    continue;
                };

                warn!("Error writing to postgres {err}");
            }

            bail!("Postgres channel closed")
        })
    }
}
