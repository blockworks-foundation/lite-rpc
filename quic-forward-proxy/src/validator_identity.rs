use solana_sdk::pubkey::Pubkey;
use solana_sdk::signature::Keypair;
use solana_sdk::signer::{Signer};
use std::fmt::Display;
use std::sync::Arc;

#[derive(Clone)]
pub struct ValidatorIdentity {
    keypair: Arc<Keypair>,
}

impl ValidatorIdentity {
    pub fn new(keypair: Option<Keypair>) -> Self {
        let keypair = keypair.unwrap_or(Keypair::new());
        ValidatorIdentity {
            keypair: Arc::new(keypair),
        }
    }

    pub fn get_keypair_for_tls(&self) -> Arc<Keypair> {
        self.keypair.clone()
    }

    pub fn get_pubkey(&self) -> Pubkey {
        self.keypair.pubkey()
    }
}

impl Display for ValidatorIdentity {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}", self.keypair.pubkey().to_string())
    }
}
