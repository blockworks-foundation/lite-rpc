use anyhow::Context;
use solana_sdk::signature::Keypair;
use std::env;

// note this is duplicated from lite-rpc module
pub async fn load_identity_keypair(
    identity_path: Option<String>,
) -> anyhow::Result<Option<Keypair>> {
    let identity_str = if let Some(identity_from_cli) = identity_path {
        tokio::fs::read_to_string(identity_from_cli)
            .await
            .context("Cannot find the identity file provided")?
    } else if let Ok(identity_env_var) = env::var("IDENTITY") {
        identity_env_var
    } else {
        return Ok(None);
    };

    let identity_bytes: Vec<u8> =
        serde_json::from_str(&identity_str).context("Invalid identity format expected Vec<u8>")?;

    Ok(Some(
        Keypair::from_bytes(identity_bytes.as_slice()).context("Invalid identity")?,
    ))
}
