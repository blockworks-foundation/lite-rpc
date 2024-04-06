use anyhow::Context;
use solana_sdk::signature::Keypair;
use std::env;

pub async fn load_identity_keypair(
    identity_keyfile_path: Option<String>,
) -> anyhow::Result<Option<Keypair>> {
    let identity_jsonarray_str = if let Ok(identity_env_var) = env::var("IDENTITY") {
        identity_env_var
    } else if let Some(identity_path) = identity_keyfile_path {
        tokio::fs::read_to_string(identity_path)
            .await
            .context("Cannot find the identity file provided")?
    } else {
        return Ok(None);
    };

    let identity_bytes: Vec<u8> = serde_json::from_str(&identity_jsonarray_str)
        .context("Invalid identity format expected Vec<u8>")?;

    Ok(Some(
        Keypair::from_bytes(identity_bytes.as_slice()).context("Invalid identity")?,
    ))
}
