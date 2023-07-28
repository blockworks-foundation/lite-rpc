use std::fmt::Display;
use std::sync::Arc;
use solana_sdk::pubkey::Pubkey;
use solana_sdk::signature::Keypair;
use solana_sdk::signer::Signer;

#[derive(Clone)]
pub struct ValidatorIdentity {
    keypair: Option<Arc<Keypair>>,
    dummy_keypair: Arc<Keypair>,
}

impl ValidatorIdentity {
    pub fn new(keypair: Option<Keypair>) -> Self {
        let dummy_keypair = Keypair::new();
        ValidatorIdentity {
            keypair: keypair.map(|kp| Arc::new(kp)),
            dummy_keypair: Arc::new(dummy_keypair),
        }
    }

    pub fn get_keypair_for_tls(&self) -> Arc<Keypair> {
        match &self.keypair {
            Some(keypair) => keypair.clone(),
            None => self.dummy_keypair.clone(),
        }
    }

    pub fn get_pubkey(&self) -> Pubkey {
        let keypair = match &self.keypair {
            Some(keypair) => keypair.clone(),
            None => self.dummy_keypair.clone(),
        };
        keypair.pubkey()
    }
}

impl Display for ValidatorIdentity {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match &self.keypair {
            Some(keypair) => write!(f, "{}", keypair.pubkey().to_string()),
            None => write!(f, "no keypair"),
        }
    }
}
