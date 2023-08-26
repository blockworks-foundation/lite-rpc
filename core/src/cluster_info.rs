use std::{sync::Arc, str::FromStr};
use dashmap::DashMap;
use solana_rpc_client_api::response::RpcContactInfo;
use solana_sdk::pubkey::Pubkey;
use tokio::sync::broadcast::Receiver;

pub struct ClusterInfo {
    pub cluster_nodes: Arc<DashMap<Pubkey, Arc<RpcContactInfo>>>,
}

impl ClusterInfo {

    pub async fn load_cluster_info(&self, mut contact_info_reciever: Receiver<Vec<RpcContactInfo>>) -> anyhow::Result<()> {
        let cluster_nodes = contact_info_reciever.recv().await.expect("Failed to recieve on broadcast channel");
        cluster_nodes.iter().for_each(|x| {
            if let Ok(pubkey) = Pubkey::from_str(x.pubkey.as_str()) {
                self.cluster_nodes.insert(pubkey, Arc::new(x.clone()));
            }
        });
        Ok(())
    }
}