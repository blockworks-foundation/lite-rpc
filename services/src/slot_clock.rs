use solana_client::rpc_client;
use solana_lite_rpc_core::{grpc_client::GrpcClient, solana_utils::SolanaUtils, AtomicSlot};
use solana_rpc_client::rpc_client::RpcClient;
use solana_sdk::{commitment_config::CommitmentLevel, slot_history::Slot};
use tokio::sync::mpsc::{UnboundedReceiver, UnboundedSender};

lazy_static::lazy_static! {
    static ref CURRENT_SLOT: GenericGauge<prometheus::core::AtomicI64> =
    register_int_gauge!(opts!("literpc_current_slot", "Current slot seen by last rpc")).unwrap();

    static ref ESTIMATED_SLOT: GenericGauge<prometheus::core::AtomicI64> =
    register_int_gauge!(opts!("literpc_estimated_slot", "Estimated slot seen by last rpc")).unwrap();
}

/// a centralized clock
#[derive(Debug, Clone, Default)]
pub struct SlotClock {
    /// last verified slot from validator
    current_slot: AtomicSlot,
    /// estimated slot in case of log
    estimated_slot: AtomicSlot,
    /// current slot broadcast rx
    current_slot_rx: Option<broadcast::Receiver<Slot>>,
    /// estimated slot broadcast rx
    estimated_slot_rx: Option<broadcast::Receiver<Slot>>,
}

impl SlotClock {
    pub async fn run(self, addr: &str, grpc: bool) -> anyhow::Result<Self> {
        let (current_slot_tx, current_slot_rx) = broadcast::channel(1);
        let (estimated_slot_tx, estimated_slot_rx) = broadcast::channel(1);

        self.current_slot_rx = Some(current_slot_rx);
        self.estimated_slot_rx = Some(estimated_slot_rx);

        // create unbouded channel
        let (raw_slot_tx, raw_slot_rx) = mpsc::unbounded_channel();

        let raw_slot_fetcher_task = tokio::spawn(async move {
            // TODO: run for some amount of errors

            if grpc {
                let mut client = GeyserGrpcClient::connect(addr, None::<&'static str>, None)
                    .context("error creating grpc client")?;

                GrpcClient::subscribe(
                    &mut client,
                    Some(raw_slot_tx),
                    None,
                    Some(CommitmentLevel::Processed),
                )
                .await?;
            } else {
                let mut client = RpcClient::new(addr);

                SolanaUtils::subscribe_to_rpc_slot(&mut client, raw_slot_tx).await?;
            }

            bail!("Slot clock raw slot fetcher task exited unexpectedly");
        });

        let slot_estimator_task = tokio::spawn(async move {
            loop {
                let current_slot = self.current_slot.load(Ordering::Relaxed);
                let estimated_slot = self.current_slot.load(Ordering::Relaxed);

                let (new_slot, new_estimated_slot) =
                    SolanaUtils::slot_estimator(raw_slot_rx, current_slot, estimated_slot).await;

                if new_slot != current_slot {
                    CURRENT_SLOT.set(new_slot);
                    self.current_slot.store(new_slot, Ordering::Relaxed);

                    if let Some(current_slot_tx) = &self.current_slot_rx {
                        current_slot_tx.send(new_slot).unwrap();
                    }
                }

                if new_estimated_slot != estimated_slot {
                    ESTIMATED_SLOT.set(new_estimated_slot);
                    self.estimated_slot
                        .store(new_estimated_slot, Ordering::Relaxed);

                    if let Some(estimated_slot_tx) = &self.estimated_slot_rx {
                        estimated_slot_tx.send(new_estimated_slot).unwrap();
                    }
                }
            }
        });

        tokio::select! {
            res = raw_slot_fetcher_task => {
                bail!("Slot clock raw slot fetcher task exited unexpectedly {res:?}");
            }
            res = slot_estimator_task => {
                bail!("Slot clock slot estimator task exited unexpectedly {res:?}");
            }
        }
    }

    pub async fn current_slot(&self) -> Slot {
        self.current_slot.load(Ordering::Relaxed)
    }

    pub async fn estimated_slot(&self) -> Slot {
        self.estimated_slot.load(Ordering::Relaxed)
    }

    pub fn current_slot_rx(&self) -> UnboundedReceiver<Slot> {
        self.current_slot_rx
            .expect("Slot Clock Not Executed yet")
            .clone()
    }

    pub fn estimated_slot_rx(&self) -> UnboundedReceiver<Slot> {
        self.estimated_slot_rx
            .expect("Slot Clock Not Executed yet")
            .clone()
    }
}
