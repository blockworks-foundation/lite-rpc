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

// "lite-rpc grpc blockstore -testnet"

#[cfg(test)]
mod tests {
    use std::str::FromStr;
    use assert_json_diff::{assert_json_eq, assert_json_include};
    use bincode::options;
    use serde_json::Value;
    use solana_account_decoder::parse_token::UiTokenAmount;
    use solana_sdk::blake3::Hash;
    use solana_sdk::clock::{Slot, UnixTimestamp};
    use solana_sdk::hash::Hasher;
    use solana_sdk::instruction::CompiledInstruction;
    use solana_sdk::message::{legacy, LegacyMessage};
    use solana_sdk::reward_type::RewardType;
    use solana_sdk::system_instruction::SystemInstruction;
    use solana_sdk::transaction::{TransactionVersion, VersionedTransaction};
    use solana_sdk::vote;
    use solana_sdk::vote::instruction::VoteInstruction;
    use solana_transaction_status::{BlockEncodingOptions, EncodedTransaction, Reward, TransactionBinaryEncoding, TransactionTokenBalance, UiConfirmedBlock, UiTransactionTokenBalance};
    use solana_transaction_status::TransactionDetails::Signatures;
    use super::*;

    #[test]
    fn reject_request_for_processed() {
        let rpc_client = create_local_rpc_client();
        let slot = get_recent_slot();
        let config = RpcBlockConfig {
            encoding: Some(UiTransactionEncoding::Base58),
            transaction_details: Some(TransactionDetails::None),
            rewards: Some(true),
            commitment: Some(CommitmentConfig::processed()),
            max_supported_transaction_version: Some(0),
        };
        let result = rpc_client.get_block_with_config(slot, config);
        assert_eq!(result.unwrap_err().to_string(), "RPC response error -32602: Method does not support commitment below `confirmed` ");
    }


    #[test]
    fn transactions_none_base58() {
        let slot = get_recent_slot();
        let config = RpcBlockConfig {
            encoding: Some(UiTransactionEncoding::Base58),
            transaction_details: Some(TransactionDetails::None),
            rewards: Some(true),
            commitment: None,
            max_supported_transaction_version: Some(0),
        };

        assert_same_response_success(slot, config);
    }


    #[test]
    fn transactions_v0() {
        let slot = get_recent_slot();

        for slot in slot-200..slot {

            let config = RpcBlockConfig {
                encoding: Some(UiTransactionEncoding::Base58),
                transaction_details: Some(TransactionDetails::Full),
                rewards: Some(true),
                commitment: None,
                max_supported_transaction_version: Some(0),
            };

            let rpc_client = create_testnet_rpc_client();
            let blockoption = rpc_client.get_block_with_config(slot, config);
            println!("check slot {}", slot);
            if blockoption.is_err() {
                continue;
            }
            let block = blockoption.unwrap();

            let option = block.transactions.clone();
            if option.is_none() {
                continue;
            }
            let v0_tx = option.unwrap().iter().any(|tx| {
                match tx.version.as_ref().unwrap() {
                    TransactionVersion::Legacy(_) => {
                        true
                    }
                    TransactionVersion::Number(_) => {
                        true
                    }
                }
            });

            if v0_tx {
                info!("found new version of transaction");
                let json = serde_json::to_string(&block).unwrap();
                println!("{}", json);
            }
        }

    }


    #[test]
    fn transactions_signatures_base58() {
        let slot = get_recent_slot();
        let config = RpcBlockConfig {
            encoding: Some(UiTransactionEncoding::Base58),
            transaction_details: Some(TransactionDetails::Signatures),
            rewards: Some(true),
            commitment: None,
            max_supported_transaction_version: Some(0),
        };

        assert_same_response_success(slot, config);
    }

    #[test]
    fn transactions_full() {
        let slot = get_recent_slot();
        let config = RpcBlockConfig {
            encoding: Some(UiTransactionEncoding::Json),
            transaction_details: Some(TransactionDetails::Full),
            rewards: Some(true),
            commitment: None,
            max_supported_transaction_version: Some(0),
        };

        assert_same_response_success(slot, config);
    }

    #[test]
    fn transaction_order_full_vs_signatures() {
        let rpc_client = create_local_rpc_client();
        let slot = 256903604;
        let config1 = RpcBlockConfig {
            encoding: Some(UiTransactionEncoding::Base58),
            transaction_details: Some(TransactionDetails::Full),
            rewards: Some(true),
            commitment: None,
            max_supported_transaction_version: Some(0),
        };
        let config2 = RpcBlockConfig {
            encoding: Some(UiTransactionEncoding::Base58),
            transaction_details: Some(TransactionDetails::Signatures),
            rewards: Some(true),
            commitment: None,
            max_supported_transaction_version: Some(0),
        };

        let block1 = rpc_client.get_block_with_config(slot, config1).unwrap();
        let block2 = rpc_client.get_block_with_config(slot, config2).unwrap();

        // for tx in &block1.transactions.unwrap() {
        //     let zzz: VersionedTransaction = tx.transaction.decode().unwrap();
        //     println!("tx: {}", zzz.signatures);
        // }
    }

    #[test]
    fn decode_base58() {
        let raw_testnet = "5nejtVuPH9GteV6kHsoh7EqDgU4gG8kj5uVd84L1dNuJzfwLNRx4shuJEmREsBXW5Lxtr1pEtb79798NBKTMfVSwVHPTjVEYrfoKNbSZyKj8N1vcXSbftWCDf96HGE4guXMf9n52TLTj7JLkNEpaJi1gP1eiTfj34LZqp3cTA3azcnwDLfS75L1UgrGb4F3yScGqJjTzLAaN5UiJ2kbE5926TH3VDRmAVEq3zfnjpsd93JZhzpLBfqKKyaMQZuoT8uF6C4jhjSNvB4pkR8RELUw3rgYSKonhJRjZKPjhKFkFjWaD9qx3dTgAEyL5kiNZN8379gNfsxptCqG3hoiZLL2Wdj8MRVKzuLv4sn4o8WQyDB2gq4bPGaSDbtSgfca3BDjSTzSPpjUpZDxHrFHLivxU7Z4sHFHrGUVmxzBNqbgPUHn5xvK";
        let raw_local = "2cG57yNZbRy627js22ofBBn3Uj3Ymf92MufP2n1M9uhCdyMctdBc8gcP5E1SBTicXMdebEMVJM7vGW4uw6ZqDmUaWej1mdh7xPUhV5pee4YUiab4tiuxfE1JkN4fgYksLUNcohUJ1eggoP8WBbeNbQ7Kav3RAV7tJFU1xWzd5J9nKKoCGL2HBwhzpWDKKmdD6WorwaXyvAdiepZrKLuGoyZ9SYg8uTv3D2mhr2gxqc4y3RKt26N9RBY4atYYphR6NifYD4pJ6pcimG6KpJJBNXrYnNbhxb4wuZC1Afeye2qHd3mwUZnRq8ytr3d8hVy7snNxjo5MKGUzP3vWc2XFR3RFdZvaHy1iGBPTLQhCgrHKDZvAEUXmfTxf1JvELCjHMFhxybcAWRS4k479PSjgn3JvRSwabRV2VihtsxrgE8Y2jKJd1R5rLo";
        let transaction = EncodedTransaction::Binary(raw_local.to_string(),
                                                     TransactionBinaryEncoding::Base58,
        );

        println!("decoded: {:?}", transaction.decode());
    }

    ///
    ///```
    /// decoded: Some(VersionedTransaction { signatures: [3mJ2rMus6q9XEtgrbnnAfcXYtZrsjo4ehdHPkVWbM78YACUwvr9XkXLnYTtaqZ98S1gft6PrcCqMis3WuBtacPv3], message: V0(Message { header: MessageHeader { num_required_signatures: 1, num_readonly_signed_accounts: 0, num_readonly_unsigned_accounts: 1 }, account_keys: [AweDwMst78mJA1pWyqmueYEns6Mdm9QDNv4SpZJdDaNs, J5RP9MhwfrFzA9Qfm8DUAzeFgVMEjjah77zGFNZC9HJv, Vote111111111111111111111111111111111111111], recent_blockhash: EgPc6CgZPTtqy1wUJTbbU3TZF2SsCFWHCRrzvnufWjtb, instructions: [CompiledInstruction { program_id_index: 2, accounts: [1, 0], data: [12, 0, 0, 0, 221, 42, 80, 15, 0, 0, 0, 0, 31, 1, 31, 1, 30, 1, 29, 1, 28, 1, 27, 1, 26, 1, 25, 1, 24, 1, 23, 1, 22, 1, 21, 1, 20, 1, 19, 1, 18, 1, 17, 1, 16, 1, 15, 1, 14, 1, 13, 1, 12, 1, 11, 1, 10, 1, 9, 1, 8, 1, 7, 1, 6, 1, 5, 1, 4, 1, 3, 1, 2, 1, 1, 206, 250, 30, 156, 156, 173, 198, 110, 93, 108, 251, 131, 2, 14, 151, 249, 94, 13, 29, 107, 189, 215, 47, 117, 88, 236, 106, 15, 94, 186, 88, 180, 1, 159, 150, 233, 101, 0, 0, 0, 0] }], address_table_lookups: [] }) })
    //
    //
    // decoded: Some(VersionedTransaction { signatures: [3mJ2rMus6q9XEtgrbnnAfcXYtZrsjo4ehdHPkVWbM78YACUwvr9XkXLnYTtaqZ98S1gft6PrcCqMis3WuBtacPv3], message: Legacy(Message { header: MessageHeader { num_required_signatures: 1, num_readonly_signed_accounts: 0, num_readonly_unsigned_accounts: 1 }, account_keys: [AweDwMst78mJA1pWyqmueYEns6Mdm9QDNv4SpZJdDaNs, J5RP9MhwfrFzA9Qfm8DUAzeFgVMEjjah77zGFNZC9HJv, Vote111111111111111111111111111111111111111], recent_blockhash: EgPc6CgZPTtqy1wUJTbbU3TZF2SsCFWHCRrzvnufWjtb, instructions: [CompiledInstruction { program_id_index: 2, accounts: [1, 0], data: [12, 0, 0, 0, 221, 42, 80, 15, 0, 0, 0, 0, 31, 1, 31, 1, 30, 1, 29, 1, 28, 1, 27, 1, 26, 1, 25, 1, 24, 1, 23, 1, 22, 1, 21, 1, 20, 1, 19, 1, 18, 1, 17, 1, 16, 1, 15, 1, 14, 1, 13, 1, 12, 1, 11, 1, 10, 1, 9, 1, 8, 1, 7, 1, 6, 1, 5, 1, 4, 1, 3, 1, 2, 1, 1, 206, 250, 30, 156, 156, 173, 198, 110, 93, 108, 251, 131, 2, 14, 151, 249, 94, 13, 29, 107, 189, 215, 47, 117, 88, 236, 106, 15, 94, 186, 88, 180, 1, 159, 150, 233, 101, 0, 0, 0, 0] }] }) })
    ///```


    // solana-transction-status
    #[test]
    fn test_decode_base58_transaction() {
        let raw_testnet = "5nejtVuPH9GteV6kHsoh7EqDgU4gG8kj5uVd84L1dNuJzfwLNRx4shuJEmREsBXW5Lxtr1pEtb79798NBKTMfVSwVHPTjVEYrfoKNbSZyKj8N1vcXSbftWCDf96HGE4guXMf9n52TLTj7JLkNEpaJi1gP1eiTfj34LZqp3cTA3azcnwDLfS75L1UgrGb4F3yScGqJjTzLAaN5UiJ2kbE5926TH3VDRmAVEq3zfnjpsd93JZhzpLBfqKKyaMQZuoT8uF6C4jhjSNvB4pkR8RELUw3rgYSKonhJRjZKPjhKFkFjWaD9qx3dTgAEyL5kiNZN8379gNfsxptCqG3hoiZLL2Wdj8MRVKzuLv4sn4o8WQyDB2gq4bPGaSDbtSgfca3BDjSTzSPpjUpZDxHrFHLivxU7Z4sHFHrGUVmxzBNqbgPUHn5xvK";
        let raw_local = "2cG57yNZbRy627js22ofBBn3Uj3Ymf92MufP2n1M9uhCdyMctdBc8gcP5E1SBTicXMdebEMVJM7vGW4uw6ZqDmUaWej1mdh7xPUhV5pee4YUiab4tiuxfE1JkN4fgYksLUNcohUJ1eggoP8WBbeNbQ7Kav3RAV7tJFU1xWzd5J9nKKoCGL2HBwhzpWDKKmdD6WorwaXyvAdiepZrKLuGoyZ9SYg8uTv3D2mhr2gxqc4y3RKt26N9RBY4atYYphR6NifYD4pJ6pcimG6KpJJBNXrYnNbhxb4wuZC1Afeye2qHd3mwUZnRq8ytr3d8hVy7snNxjo5MKGUzP3vWc2XFR3RFdZvaHy1iGBPTLQhCgrHKDZvAEUXmfTxf1JvELCjHMFhxybcAWRS4k479PSjgn3JvRSwabRV2VihtsxrgE8Y2jKJd1R5rLo";
        let transaction = EncodedTransaction::Binary(raw_testnet.to_string(),
            TransactionBinaryEncoding::Base58,
        );
    }


    #[test]
    fn test_decode() {


        let foo = "Fk63Q321enfa7mN4TibcZkzYHBEhvTgZ4W1c4r\
            zdbaHKAQ3262j4bLyCTdHwFx3dSC76VmdbGa4PpAY2p\
            XgJZuPRnvttdJJLTQoaymujmbGqMe5gMXNcJ15AzXJN\
            DcZNBWFpvFPTFENGrRznFy6WmosM2xMB7D";

        let raw = bs58::decode(&foo).into_vec().unwrap();

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

        let signature1 = Signature::from_str("3mJ2rMus6q9XEtgrbnnAfcXYtZrsjo4ehdHPkVWbM78YACUwvr9XkXLnYTtaqZ98S1gft6PrcCqMis3WuBtacPv3").unwrap();

        let data = bs58::decode("Fk63Q321enfa7mN4TibcZkzYHBEhvTgZ4W1c4rzdbaHKAQ3262j4bLyCTdHwFx3dSC76VmdbGa4PpAY2pXgJZuPRnvttdJJLTQoaymujmbGqMe5gMXNcJ15AzXJNDcZNBWFpvFPTFENGrRznFy6WmosM2xMB7D").into_vec().unwrap();

        let ins1 = CompiledInstruction::new_from_raw_parts(
            2, data, vec![1,0]);

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
            recent_blockhash: solana_sdk::hash::Hash::from_str("EgPc6CgZPTtqy1wUJTbbU3TZF2SsCFWHCRrzvnufWjtb").unwrap(),
            instructions: vec![ins1],
        });

        let tx1: TransactionWithStatusMeta = TransactionWithStatusMeta::Complete(
            VersionedTransactionWithStatusMeta {
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
                        "Program Vote111111111111111111111111111111111111111 success".to_string()]),
                    pre_token_balances: Some(vec![]),
                    post_token_balances: Some(vec![]),
                    rewards: Some(vec![]),
                    loaded_addresses: Default::default(),
                    return_data: None,
                    compute_units_consumed: Some(2100),
                },
            }
        );

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
                commission: None
            }],
            block_time: Some(1709807265 as UnixTimestamp),
            block_height: Some(220980772),
        };

        let options = BlockEncodingOptions {
            transaction_details: TransactionDetails::Full,
            show_rewards: true,
            max_supported_transaction_version: Some(0),
        };
        let confirmed_block = confirmed_block.encode_with_options(UiTransactionEncoding::Json, options).unwrap();

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

    fn assert_same_response_success(slot: u64, config: RpcBlockConfig) {
        let mainnet = "http://api.mainnet-beta.solana.com/";
        let testnet = format!("https://api.testnet.rpcpool.com/{testnet_api_token}", testnet_api_token = std::env::var("TESTNET_API_TOKEN").unwrap());
        let local = "http://localhost:8890";
        let rpc_client1 = solana_rpc_client::rpc_client::RpcClient::new(local.to_string());
        let rpc_client2 = solana_rpc_client::rpc_client::RpcClient::new(testnet.to_string());

        let result1 = rpc_client1.get_block_with_config(slot, config);
        let result1 = result1.expect("call to local must succeed");
        let result1 = truncate(result1);

        let result2 = rpc_client2.get_block_with_config(slot, config);
        let result2 = result2.expect("call to testnet must succeed");
        let result2 = truncate(result2);

        println!("comparing responses for block {}", slot);
        println!("-------result1-------\n");
        println!("{}", serde_json::to_string(&result1).unwrap());
        println!("=======result1=======\n");
        println!("\n");
        println!("-------result2-------\n");
        println!("{}", serde_json::to_string(&result2).unwrap());
        println!("=======result2=======\n");
        assert_json_eq!(result2, result1);
    }

    fn truncate(mut result1: UiConfirmedBlock) -> UiConfirmedBlock {
        const LIMIT: usize = 5;
        result1.transactions = Some(result1.transactions.unwrap().into_iter().take(LIMIT).collect());
        result1
    }

    fn assert_same_response_fail(slot: u64, config: RpcBlockConfig) {
        let rpc_client1 = create_local_rpc_client();
        let rpc_client2 = create_testnet_rpc_client();

        let result1 = rpc_client1.get_block_with_config(slot, config);
        let result2 = rpc_client2.get_block_with_config(slot, config);

        assert!(result1.is_err(), "call to local must fail");
        assert!(result2.is_err(), "call to testnet must fail");

        println!("comparing responses for block {}", slot);
        assert_eq!(result2.unwrap_err().to_string(), result1.unwrap_err().to_string());
    }

    fn create_testnet_rpc_client() -> RpcClient {
        let testnet = format!("https://api.testnet.rpcpool.com/{testnet_api_token}", testnet_api_token = std::env::var("TESTNET_API_TOKEN").unwrap());
        let rpc_client2 = solana_rpc_client::rpc_client::RpcClient::new(testnet.to_string());
        rpc_client2
    }

    fn create_local_rpc_client() -> RpcClient {
        let local = "http://localhost:8890";
        let rpc_client1 = solana_rpc_client::rpc_client::RpcClient::new(local.to_string());
        rpc_client1
    }

    fn get_recent_slot() -> Slot {
        let slot = create_testnet_rpc_client().get_slot().unwrap();
        let slot = slot - 40;
        slot
    }

    fn extract_error(result: Result<EncodedConfirmedBlock, Error>) {
        if let Err(error) = result {
            if let ErrorKind::RpcError(RpcResponseError {
                                           code,
                                           message,
                                           data,
                                       }) = error.kind {
                println!("Error: {}", code);
            }
        }
    }
}

