use std::sync::Arc;

use anyhow::{bail, Context, Ok};
use log::{info, warn};
use postgres_native_tls::MakeTlsConnector;

use tokio::{
    fs,
    sync::mpsc::{UnboundedReceiver, UnboundedSender},
    task::JoinHandle,
};
use tokio_postgres::Client;

use native_tls::{Certificate, Identity, TlsConnector};

#[derive(Clone)]
pub struct Postgres {
    client: Arc<Client>,
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
}

pub type PostgresMpscRecv = UnboundedReceiver<PostgresMsg>;
pub type PostgresMpscSend = UnboundedSender<PostgresMsg>;

impl Postgres {
    /// # Return
    /// (connection join handle, Self)
    ///
    /// returned join handle is required to be polled
    pub async fn new(
        porstgres_config: &str,
    ) -> anyhow::Result<(JoinHandle<anyhow::Result<()>>, Self)> {
        let connector = TlsConnector::builder()
            .add_root_certificate(Certificate::from_pem(&fs::read("ca.pem").await?)?)
            .identity(
                Identity::from_pkcs12(&fs::read("client.pks").await?, "p").context("Identity")?,
            )
            .danger_accept_invalid_hostnames(true)
            .danger_accept_invalid_certs(true)
            .build()?;

        info!("making tls config");

        let connector = MakeTlsConnector::new(connector);
        let (client, connection) = tokio_postgres::connect(porstgres_config, connector).await?;
        let client = Arc::new(client);

        Ok((
            tokio::spawn(async move { Ok(connection.await?) }),
            Self { client },
        ))
    }

    pub async fn send_block(&self, block: PostgresBlock) -> anyhow::Result<()> {
        let PostgresBlock {
            slot,
            leader_id,
            parent_slot,
        } = block;

        self.client
            .execute(
                r#"
                INSERT INTO lite_rpc.Blocks 
                (slot, leader_id, parent_slot)
                VALUES
                ($1, $2, $3)
            "#,
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

        warn!("{}", signature.len());

        self.client.execute(
            r#"
                INSERT INTO lite_rpc.Txs 
                (signature, recent_slot, forwarded_slot, processed_slot, cu_consumed, cu_requested, quic_response)
                VALUES
                ($1, $2, $3, $4, $5, $6, $7)
            "#,
            &[&signature, &recent_slot, &forwarded_slot, &processed_slot, &cu_consumed, &cu_requested, &quic_response],
        ).await?;

        Ok(())
    }

    pub fn start(self, mut recv: PostgresMpscRecv) -> JoinHandle<anyhow::Result<()>> {
        tokio::spawn(async move {
            info!("Writing to postgres");

            while let Some(msg) = recv.recv().await {
                let Err(err) = (
                    match msg {
                    PostgresMsg::PostgresTx(tx) => self.send_tx(tx).await,
                    PostgresMsg::PostgresBlock(block) => self.send_block(block).await,
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
