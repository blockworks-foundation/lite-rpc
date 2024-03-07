use jsonrpsee::types::error::INVALID_PARAMS_CODE;
use log::info;
use solana_rpc_client::rpc_client::RpcClient;
use solana_rpc_client_api::client_error::{Error, ErrorKind};
use solana_rpc_client_api::client_error::ErrorKind::RpcError;
use solana_rpc_client_api::config::RpcBlockConfig;
use solana_rpc_client_api::request::RpcError::{RpcRequestError, RpcResponseError};
use solana_rpc_client_api::request::RpcResponseErrorData;
use solana_sdk::commitment_config::CommitmentConfig;
use solana_sdk::message::{MessageHeader, v0, VersionedMessage};
use solana_sdk::pubkey::Pubkey;
use solana_sdk::signature::Signature;
use solana_sdk::transaction::VersionedTransaction;
use solana_transaction_status::{ConfirmedBlock, EncodedConfirmedBlock, EncodedTransactionWithStatusMeta, Rewards, TransactionDetails, TransactionStatusMeta, TransactionWithStatusMeta, UiTransactionEncoding, VersionedTransactionWithStatusMeta};
use solana_lite_rpc_core::structures::produced_block::TransactionInfo;
use crate::errors::JsonRpcError;

fn map_versioned_transaction(ti: TransactionInfo) -> VersionedTransaction {
    let tx: VersionedTransaction = VersionedTransaction {
        signatures: vec![ti.signature], // TODO check if that is correct
        message: ti.message,
    };
    tx
}

fn create_test_message() -> VersionedMessage {
    VersionedMessage::V0(v0::Message {
        header: MessageHeader {
            num_required_signatures: 1,
            ..MessageHeader::default()
        },
        account_keys: vec![Pubkey::new_unique()],
        ..v0::Message::default()
    })
}

fn create_test_tx(signature: Signature) -> TransactionInfo {
    TransactionInfo {
        signature,
        index: 0,
        is_vote: false,
        err: None,
        cu_requested: Some(40000),
        prioritization_fees: Some(5000),
        cu_consumed: Some(32000),
        recent_blockhash: solana_sdk::hash::Hash::new_unique(),
        message: create_test_message(),
        writable_accounts: vec![],
        readable_accounts: vec![],
        address_lookup_tables: vec![],
    }
}


#[test]
fn map_confirmed() {

    let ti = create_test_tx(Signature::new_unique());

    let tx: TransactionWithStatusMeta = TransactionWithStatusMeta::Complete(
        VersionedTransactionWithStatusMeta {
            transaction: map_versioned_transaction(ti),
            meta: TransactionStatusMeta {
                status: Ok(()),
                fee: 0,
                pre_balances: vec![],
                post_balances: vec![],
                inner_instructions: None,
                log_messages: None,
                pre_token_balances: None,
                post_token_balances: None,
                rewards: None,
                loaded_addresses: Default::default(),
                return_data: None,
                compute_units_consumed: None,
            },
        }
    );

    let _confirmed_block: ConfirmedBlock = ConfirmedBlock {
        previous_blockhash: "previous_blockhash".to_string(),
        blockhash: "blockhash".to_string(),
        parent_slot: 1,
        transactions: vec![tx],
        rewards: vec![],
        block_time: None,
        block_height: None,
    };

}


