use solana_sdk::pubkey::Pubkey;
use solana_sdk::signature::Keypair;
use solana_sdk::signer::Signer;
use std::fmt::Display;
use std::sync::Arc;

#[derive(Clone)]
pub struct ValidatorIdentity {
    keypair: Arc<Keypair>,
    is_dummy_keypair: bool,
}

impl ValidatorIdentity {
    pub fn new(keypair: Option<Keypair>) -> Self {
        match keypair {
            Some(keypair) => ValidatorIdentity {
                keypair: Arc::new(keypair),
                is_dummy_keypair: false,
            },
            None => ValidatorIdentity {
                keypair: Arc::new(Keypair::new()),
                is_dummy_keypair: true,
            },
        }
    }

    pub fn get_keypair_for_tls(&self) -> Arc<Keypair> {
        self.keypair.clone()
    }

    pub fn get_pubkey(&self) -> Pubkey {
        let keypair = self.keypair.clone();
        keypair.pubkey()
    }
}

impl Display for ValidatorIdentity {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        if self.is_dummy_keypair {
            write!(f, "no keypair")
        } else {
            write!(f, "{}", self.keypair.pubkey())
        }
    }
}
