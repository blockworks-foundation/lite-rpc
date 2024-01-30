use solana_sdk::message::v0::MessageAddressTableLookup;

pub trait AddressLookupTableFetcher {
    fn get_address_lookup_table(&self, message_address_table_lookup : &MessageAddressTableLookup );
}