use dashmap::DashMap;
use solana_rpc_client_api::response::RpcContactInfo;
use solana_sdk::pubkey::Pubkey;
use std::{str::FromStr, sync::Arc};

use crate::types::ClusterInfoStream;

#[derive(Debug, Clone, Default)]
pub struct ClusterInfo {
    pub cluster_nodes: Arc<DashMap<Pubkey, Arc<RpcContactInfo>>>,
}

impl ClusterInfo {
    pub async fn load_cluster_info(
        &self,
        contact_info_reciever: &mut ClusterInfoStream,
    ) -> anyhow::Result<()> {
        let cluster_nodes = contact_info_reciever
            .recv()
            .await
            .expect("Failed to recieve on broadcast channel");
        cluster_nodes.iter().for_each(|x| {
            if let Ok(pubkey) = Pubkey::from_str(x.pubkey.as_str()) {
                self.cluster_nodes.insert(pubkey, Arc::new(x.clone()));
            }
        });
        Ok(())
    }
}
