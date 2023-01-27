use std::sync::Arc;

use anyhow::Context;
use log::info;
use postgres_native_tls::MakeTlsConnector;
use tokio::fs;
use tokio::task::JoinHandle;
use tokio_postgres::Client;

use native_tls::{Certificate, Identity, TlsConnector};

#[derive(Clone)]
pub struct Postgres {
    client: Arc<Client>,
}

pub struct PostgresTx<'a> {
    pub signature: &'a [u8],
    pub recent_slot: i64,
    pub forwarded_slot: i64,
    pub processed_slot: Option<i64>,
    pub cu_consumed: Option<i64>,
    pub cu_requested: Option<i64>,
    pub quic_response: u32,
}

pub struct PostgresBlock {
    pub slot: i64,
    pub leader_id: i64,
    pub parent_slot: i64,
}

pub struct PostgreAccountAddr {
    pub id: u32,
    pub addr: String,
}

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

    pub async fn send_tx<'a>(&self, tx: PostgresTx<'a>) -> anyhow::Result<()> {
        let PostgresTx {
            signature,
            recent_slot,
            forwarded_slot,
            processed_slot,
            cu_consumed,
            cu_requested,
            quic_response,
        } = tx;

        self.client.execute(
            r#"
                INSERT INTO lite_rpc.Txs 
                (signature, recent_slot, forwarded_slot, processed_slot, cu_consumed, cu_requested, quic_response)
                VALUES
                ($1, $2, $3, $4, $5, $6)
            "#,
            &[&signature, &recent_slot, &forwarded_slot, &processed_slot, &cu_consumed, &cu_requested, &quic_response],
        ).await?;

        Ok(())
    }
}
