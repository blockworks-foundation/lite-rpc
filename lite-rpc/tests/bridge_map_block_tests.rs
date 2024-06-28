use std::str::FromStr;

use assert_json_diff::assert_json_eq;
use serde_json::Value;
use solana_account_decoder::parse_token::UiTokenAmount;
use solana_sdk::clock::UnixTimestamp;
use solana_sdk::instruction::CompiledInstruction;
use solana_sdk::message::{legacy, MessageHeader, v0, VersionedMessage};
use solana_sdk::pubkey::Pubkey;
use solana_sdk::reward_type::RewardType;
use solana_sdk::signature::Signature;
use solana_sdk::transaction::VersionedTransaction;
use solana_sdk::vote::instruction::VoteInstruction;
use solana_transaction_status::{
    BlockEncodingOptions, ConfirmedBlock
    , InnerInstructions, Reward
    , TransactionDetails, TransactionStatusMeta,
    TransactionWithStatusMeta, UiTransactionEncoding, UiTransactionTokenBalance,
    VersionedTransactionWithStatusMeta,
};
use solana_transaction_status::option_serializer::OptionSerializer;

use solana_lite_rpc_core::encoding::BinaryEncoding;
use solana_lite_rpc_core::structures::produced_block::TransactionInfo;

#[test]
fn test_map_tx_with_meta() {
    let ti = create_test_tx(Signature::new_unique());

    let tx1: VersionedTransaction = VersionedTransaction {
        signatures: vec![ti.signature],
        message: ti.message,
    };
    let tx: TransactionWithStatusMeta =
        TransactionWithStatusMeta::Complete(VersionedTransactionWithStatusMeta {
            transaction: tx1,
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
        });

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

#[test]
fn test_serialize_empty_vec() {
    {
        let empty_vec: Vec<u8> = vec![];
        let serialized = BinaryEncoding::Base64.encode(bincode::serialize(&empty_vec).unwrap());
        assert_eq!(serialized, "AAAAAAAAAAA=");
    }
    {
        let empty_vec: Vec<InnerInstructions> = vec![];
        let serialized = BinaryEncoding::Base64.encode(bincode::serialize(&empty_vec).unwrap());
        assert_eq!(serialized, "AAAAAAAAAAA=");
    }
}

#[test]
fn test_decode() {
    let base58_blob = r#"Fk63Q321enfa7mN4TibcZkzYHBEhvTgZ4W1c4rzdbaHKAQ3262j4bLyCTdHwFx3dSC76VmdbGa4PpAY2pXgJZuPRnvttdJJLTQoaymujmbGqMe5gMXNcJ15AzXJNDcZNBWFpvFPTFENGrRznFy6WmosM2xMB7D"#;
    let raw = bs58::decode(&base58_blob).into_vec().unwrap();

    let obj = bincode::deserialize::<VoteInstruction>(&raw).unwrap();
    match obj {
        VoteInstruction::InitializeAccount(_) => {}
        VoteInstruction::Authorize(_, _) => {}
        VoteInstruction::Vote(_) => {
            println!("Vote");
        }
        VoteInstruction::Withdraw(_) => {}
        VoteInstruction::UpdateValidatorIdentity => {}
        VoteInstruction::UpdateCommission(_) => {}
        VoteInstruction::VoteSwitch(_, _) => {}
        VoteInstruction::AuthorizeChecked(_) => {}
        VoteInstruction::UpdateVoteState(_) => {}
        VoteInstruction::UpdateVoteStateSwitch(_, _) => {}
        VoteInstruction::AuthorizeWithSeed(_) => {}
        VoteInstruction::AuthorizeCheckedWithSeed(_) => {}
        VoteInstruction::CompactUpdateVoteState(_) => {}
        VoteInstruction::CompactUpdateVoteStateSwitch(_, _) => {}
    }
}

#[test]
fn test_get_block_with_versioned_tx() {
    // block 256912126 from testnet, 2024-03-07

    let signature1 = Signature::from_str(
        "3mJ2rMus6q9XEtgrbnnAfcXYtZrsjo4ehdHPkVWbM78YACUwvr9XkXLnYTtaqZ98S1gft6PrcCqMis3WuBtacPv3",
    )
    .unwrap();

    let data = bs58::decode("Fk63Q321enfa7mN4TibcZkzYHBEhvTgZ4W1c4rzdbaHKAQ3262j4bLyCTdHwFx3dSC76VmdbGa4PpAY2pXgJZuPRnvttdJJLTQoaymujmbGqMe5gMXNcJ15AzXJNDcZNBWFpvFPTFENGrRznFy6WmosM2xMB7D").into_vec().unwrap();

    let ins1 = CompiledInstruction::new_from_raw_parts(2, data, vec![1, 0]);

    let message1 = VersionedMessage::Legacy(legacy::Message {
        header: MessageHeader {
            num_required_signatures: 1,
            num_readonly_signed_accounts: 0,
            num_readonly_unsigned_accounts: 1,
        },
        account_keys: vec![
            Pubkey::from_str("AweDwMst78mJA1pWyqmueYEns6Mdm9QDNv4SpZJdDaNs").unwrap(),
            Pubkey::from_str("J5RP9MhwfrFzA9Qfm8DUAzeFgVMEjjah77zGFNZC9HJv").unwrap(),
            Pubkey::from_str("Vote111111111111111111111111111111111111111").unwrap(),
        ],
        recent_blockhash: solana_sdk::hash::Hash::from_str(
            "EgPc6CgZPTtqy1wUJTbbU3TZF2SsCFWHCRrzvnufWjtb",
        )
        .unwrap(),
        instructions: vec![ins1],
    });

    let tx1: TransactionWithStatusMeta =
        TransactionWithStatusMeta::Complete(VersionedTransactionWithStatusMeta {
            transaction: VersionedTransaction {
                signatures: vec![signature1], // TODO check if that is correct
                message: message1,
            },
            meta: TransactionStatusMeta {
                status: Ok(()),
                fee: 5000,
                pre_balances: vec![5219591194, 145052450, 1],
                post_balances: vec![5219586194, 145052450, 1],
                inner_instructions: Some(vec![]),
                log_messages: Some(vec![
                    "Program Vote111111111111111111111111111111111111111 invoke [1]".to_string(),
                    "Program Vote111111111111111111111111111111111111111 success".to_string(),
                ]),
                pre_token_balances: Some(vec![]),
                post_token_balances: Some(vec![]),
                rewards: Some(vec![]),
                loaded_addresses: Default::default(),
                return_data: None,
                compute_units_consumed: Some(2100),
            },
        });

    let confirmed_block = ConfirmedBlock {
        previous_blockhash: "Egoc9s7MWfAUrxt2kCat7oGik49mwdKbBDsZ1KgBPTcb".to_string(),
        blockhash: "APBi3GZqo24ya8Fm4nWfRU2vMZSaaGLbXissLi2GA3Fh".to_string(),
        parent_slot: 256912125,
        transactions: vec![tx1], // TODO fill
        rewards: vec![Reward {
            pubkey: "8HL5VpqGfTG9SVkHyWU9gjo4xdQYbbdVS7ExTrou3zCE".to_string(),
            lamports: 12042400,
            post_balance: 31559596374,
            reward_type: Some(RewardType::Fee),
            commission: None,
        }],
        block_time: Some(1709807265 as UnixTimestamp),
        block_height: Some(220980772),
    };

    let options = BlockEncodingOptions {
        transaction_details: TransactionDetails::Full,
        show_rewards: true,
        max_supported_transaction_version: Some(0),
    };
    let confirmed_block = confirmed_block
        .encode_with_options(UiTransactionEncoding::Json, options)
        .unwrap();

    assert_json_eq!(
            confirmed_block,
            serde_json::from_str::<Value>(
            r#"
            {
              "previousBlockhash": "Egoc9s7MWfAUrxt2kCat7oGik49mwdKbBDsZ1KgBPTcb",
              "blockhash": "APBi3GZqo24ya8Fm4nWfRU2vMZSaaGLbXissLi2GA3Fh",
              "parentSlot": 256912125,
              "transactions": [
                {
                  "transaction": {
                    "signatures": [
                      "3mJ2rMus6q9XEtgrbnnAfcXYtZrsjo4ehdHPkVWbM78YACUwvr9XkXLnYTtaqZ98S1gft6PrcCqMis3WuBtacPv3"
                    ],
                    "message": {
                      "header": {
                        "numRequiredSignatures": 1,
                        "numReadonlySignedAccounts": 0,
                        "numReadonlyUnsignedAccounts": 1
                      },
                      "accountKeys": [
                        "AweDwMst78mJA1pWyqmueYEns6Mdm9QDNv4SpZJdDaNs",
                        "J5RP9MhwfrFzA9Qfm8DUAzeFgVMEjjah77zGFNZC9HJv",
                        "Vote111111111111111111111111111111111111111"
                      ],
                      "recentBlockhash": "EgPc6CgZPTtqy1wUJTbbU3TZF2SsCFWHCRrzvnufWjtb",
                      "instructions": [
                        {
                          "programIdIndex": 2,
                          "accounts": [
                            1,
                            0
                          ],
                          "data": "Fk63Q321enfa7mN4TibcZkzYHBEhvTgZ4W1c4rzdbaHKAQ3262j4bLyCTdHwFx3dSC76VmdbGa4PpAY2pXgJZuPRnvttdJJLTQoaymujmbGqMe5gMXNcJ15AzXJNDcZNBWFpvFPTFENGrRznFy6WmosM2xMB7D",
                          "stackHeight": null
                        }
                      ]
                    }
                  },
                  "meta": {
                    "err": null,
                    "status": {
                      "Ok": null
                    },
                    "fee": 5000,
                    "preBalances": [
                      5219591194,
                      145052450,
                      1
                    ],
                    "postBalances": [
                      5219586194,
                      145052450,
                      1
                    ],
                    "innerInstructions": [],
                    "logMessages": [
                      "Program Vote111111111111111111111111111111111111111 invoke [1]",
                      "Program Vote111111111111111111111111111111111111111 success"
                    ],
                    "preTokenBalances": [],
                    "postTokenBalances": [],
                    "rewards": [],
                    "loadedAddresses": {
                      "writable": [],
                      "readonly": []
                    },
                    "computeUnitsConsumed": 2100
                  },
                  "version": "legacy"
                }
              ],
              "rewards": [
                {
                  "pubkey": "8HL5VpqGfTG9SVkHyWU9gjo4xdQYbbdVS7ExTrou3zCE",
                  "lamports": 12042400,
                  "postBalance": 31559596374,
                  "rewardType": "Fee",
                  "commission": null
                }
              ],
              "blockTime": 1709807265,
              "blockHeight": 220980772
            }
            "#).unwrap());
}

#[test]
fn test_map_tokenbalance() {
    let token_balance = UiTransactionTokenBalance {
        account_index: 6,
        mint: "EXMVRXYMMsMxtxfaJt4tncwtp2nNBLPitEu7tFrPnLyk".to_string(),
        owner: OptionSerializer::Some("5Q544fKrFoe6tsEbD7S8EmxGTJYAKtTVhAW5Q5pge4j1".to_string()),
        program_id: OptionSerializer::Some(
            "TokenkegQfeZyiNwAJbNbGKPFXCWuBvf9Ss623VQ5DA".to_string(),
        ),
        ui_token_amount: UiTokenAmount {
            amount: "173462449762313559".to_string(),
            decimals: 9,
            ui_amount: Some(173462449.76231357),
            ui_amount_string: "173462449.762313559".to_string(),
        },
    };
    assert_json_eq!(
        token_balance,
        serde_json::from_str::<Value>(
            r#"{
        "accountIndex": 6,
        "mint": "EXMVRXYMMsMxtxfaJt4tncwtp2nNBLPitEu7tFrPnLyk",
        "owner": "5Q544fKrFoe6tsEbD7S8EmxGTJYAKtTVhAW5Q5pge4j1",
        "programId": "TokenkegQfeZyiNwAJbNbGKPFXCWuBvf9Ss623VQ5DA",
        "uiTokenAmount": {
          "amount": "173462449762313559",
          "decimals": 9,
          "uiAmount": 173462449.76231357,
          "uiAmountString": "173462449.762313559"
        }
      }"#
        )
        .unwrap()
    );
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
        fee: 5000,
        pre_balances: vec![99999],
        post_balances: vec![100001],
        inner_instructions: None,
        log_messages: Some(vec!["log line1".to_string(), "log line2".to_string()]),
        pre_token_balances: vec![],
        post_token_balances: vec![],
    }
}
