use async_trait::async_trait;
use solana_sdk::{message::v0::MessageAddressTableLookup, pubkey::Pubkey};

#[async_trait]
pub trait AddressLookupTableInterface: Send + Sync {
    async fn get_address_lookup_table(
        &self,
        message_address_table_lookup: &MessageAddressTableLookup,
    ) -> (Vec<Pubkey>, Vec<Pubkey>);
}
