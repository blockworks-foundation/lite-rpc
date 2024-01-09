use futures::StreamExt;
use jsonrpsee::{SubscriptionMessage, SubscriptionSink};
use solana_sdk::{account::Account, pubkey::Pubkey};
use std::collections::HashMap;
use yellowstone_grpc_client::GeyserGrpcClient;
use yellowstone_grpc_proto::geyser::{
    subscribe_update::UpdateOneof, SubscribeRequest, SubscribeRequestFilterAccounts,
    SubscribeRequestFilterSlots,
};

pub struct AccountSubscription {
    grpc_address: String,
    grpc_token: Option<String>,
}

impl AccountSubscription {
    pub fn new(grpc_address: String, grpc_token: Option<String>) -> Self {
        Self {
            grpc_address,
            grpc_token,
        }
    }

    pub fn subscribe(
        &self,
        subscribed_accounts: Vec<String>,
        subscription_sink: SubscriptionSink,
    ) -> anyhow::Result<()> {
        let grpc_address = self.grpc_address.clone();
        let grpc_token = self.grpc_token.clone();
        tokio::spawn(async move {
            let mut accounts = HashMap::new();
            accounts.insert(
                "client".to_string(),
                SubscribeRequestFilterAccounts {
                    account: subscribed_accounts,
                    owner: vec![],
                    filters: vec![],
                },
            );
            let mut slots = HashMap::new();
            slots.insert(
                "client".to_string(),
                SubscribeRequestFilterSlots {
                    filter_by_commitment: Some(true),
                },
            );

            let request = SubscribeRequest {
                slots,
                accounts,
                transactions: HashMap::default(),
                entry: HashMap::default(),
                blocks: HashMap::default(),
                blocks_meta: HashMap::default(),
                commitment: Some(4),
                accounts_data_slice: vec![],
                ping: None,
            };
            let mut client =
                GeyserGrpcClient::connect(grpc_address.clone(), grpc_token.clone(), None)
                    .expect("Should be able to connect");
            let mut stream = client
                .subscribe_once2(request)
                .await
                .expect("Should be able to subscribe");
            while let Some(Ok(message)) = stream.next().await {
                let Some(update) = message.update_oneof else {
                continue;
            };
                match update {
                    UpdateOneof::Account(acc) => {
                        if let Some(acc) = acc.account {
                            let account = Account {
                                lamports: acc.lamports,
                                executable: acc.executable,
                                owner: Pubkey::new_from_array(acc.owner[0..32].try_into().unwrap()),
                                rent_epoch: acc.rent_epoch,
                                data: acc.data,
                            };
                            if let Err(e) = subscription_sink
                                .send(SubscriptionMessage::from_json(&account).unwrap())
                                .await
                            {
                                log::error!("account subscription error {e:?}");
                            }
                        }
                    }
                    _ => {
                        // do nothing
                    }
                }
            }
            log::error!("Exiting the channel");
        });
        Ok(())
    }
}
