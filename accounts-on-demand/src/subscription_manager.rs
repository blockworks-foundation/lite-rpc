use std::sync::Arc;

use solana_lite_rpc_accounts::account_store_interface::AccountStorageInterface;
use solana_lite_rpc_cluster_endpoints::{
    geyser_grpc_connector::GrpcSourceConfig,
    grpc::gprc_accounts_streaming::create_grpc_account_streaming,
};
use solana_lite_rpc_core::structures::{
    account_data::AccountNotificationMessage, account_filter::AccountFilters,
};
use tokio::{
    sync::{broadcast::Sender, Mutex},
    task::AbortHandle,
};

pub struct SubscriptionManger {
    grpc_sources: Vec<GrpcSourceConfig>,
    accounts_storage: Arc<dyn AccountStorageInterface>,
    current_handles: Mutex<Arc<Vec<AbortHandle>>>,
    account_notification_sender: Sender<AccountNotificationMessage>,
}

impl SubscriptionManger {
    pub fn new(
        grpc_sources: Vec<GrpcSourceConfig>,
        accounts_storage: Arc<dyn AccountStorageInterface>,
        account_notification_sender: Sender<AccountNotificationMessage>,
    ) -> Self {
        Self {
            grpc_sources,
            accounts_storage,
            current_handles: Mutex::new(Arc::new(vec![])),
            account_notification_sender,
        }
    }

    pub async fn update_subscriptions(&self, filters: AccountFilters) {
        let (new_subscription_handles, mut account_stream) =
            create_grpc_account_streaming(self.grpc_sources.clone(), filters);

        let mut current_handle_lock = self.current_handles.lock().await;
        // delete the old handles once we start getting notification from the new handles
        let old_handles = current_handle_lock.clone();
        let account_store = self.accounts_storage.clone();
        let account_notification_sender = self.account_notification_sender.clone();

        let task = tokio::spawn(async move {
            let mut old_handles = Some(old_handles);
            loop {
                match account_stream.recv().await {
                    Ok(account_notification) => {
                        if let Some(handles_to_abort) = old_handles {
                            // abort old account fetching streams as they are no longer needed
                            old_handles = None;
                            handles_to_abort.iter().for_each(|x| x.abort());
                        }
                        log::info!("Account updated");
                        if account_store
                            .update_account(
                                account_notification.data.clone(),
                                account_notification.commitment,
                            )
                            .await
                        {
                            let _ = account_notification_sender.send(account_notification);
                        }
                    }
                    Err(tokio::sync::broadcast::error::RecvError::Lagged(e)) => {
                        log::error!(
                            "Accounts On Demand Stream Lagged by {}, we may have missed some account updates",
                            e
                        );
                        continue;
                    }
                    Err(tokio::sync::broadcast::error::RecvError::Closed) => {
                        break;
                    }
                }
            }
        });
        *current_handle_lock = Arc::new(vec![
            new_subscription_handles.abort_handle(),
            task.abort_handle(),
        ]);
    }
}
