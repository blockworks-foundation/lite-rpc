use itertools::Itertools;
use serde::{Deserialize, Serialize};
use solana_rpc_client_api::filter::{Memcmp as RpcMemcmp, MemcmpEncodedBytes, RpcFilterType};

#[derive(Serialize, Deserialize, Clone, PartialEq, Eq, Debug)]
#[serde(rename_all = "camelCase")]
pub enum MemcmpFilterData {
    Bytes(Vec<u8>),
    Base58(String),
    Base64(String),
}

#[derive(Serialize, Deserialize, Clone, PartialEq, Eq, Debug)]
#[serde(rename_all = "camelCase")]
pub struct MemcmpFilter {
    pub offset: u64,
    pub data: MemcmpFilterData,
}

#[derive(Serialize, Deserialize, Clone, PartialEq, Eq, Debug)]
#[serde(rename_all = "camelCase")]
pub enum AccountFilterType {
    Datasize(u64),
    Memcmp(MemcmpFilter),
}

#[derive(Serialize, Deserialize, Clone, PartialEq, Eq, Debug)]
#[serde(rename_all = "camelCase")]
pub struct AccountFilter {
    pub accounts: Vec<String>,
    pub program_id: Option<String>,
    pub filters: Option<Vec<AccountFilterType>>,
}

impl AccountFilter {
    pub fn get_rpc_filter(&self) -> Option<Vec<RpcFilterType>> {
        self.filters.clone().map(|filters| {
            filters
                .iter()
                .map(|filter| match filter {
                    AccountFilterType::Datasize(size) => RpcFilterType::DataSize(*size),
                    AccountFilterType::Memcmp(memcpy) => {
                        let encoded_bytes = match &memcpy.data {
                            MemcmpFilterData::Bytes(bytes) => {
                                MemcmpEncodedBytes::Bytes(bytes.clone())
                            }
                            MemcmpFilterData::Base58(data) => {
                                MemcmpEncodedBytes::Base58(data.clone())
                            }
                            MemcmpFilterData::Base64(data) => {
                                MemcmpEncodedBytes::Base64(data.clone())
                            }
                        };
                        RpcFilterType::Memcmp(RpcMemcmp::new(memcpy.offset as usize, encoded_bytes))
                    }
                })
                .collect_vec()
        })
    }
}

pub type AccountFilters = Vec<AccountFilter>;

#[test]
fn test_accounts_filters_deserialization() {
    let str = "[
        {
            \"accounts\": [],
            \"programId\": \"4MangoMjqJ2firMokCjjGgoK8d4MXcrgL7XJaL3w6fVg\",
            \"filters\": [
                {
                    \"datasize\": 200
                }
            ]
        },
        {
            \"accounts\": [],
            \"programId\": \"4MangoMjqJ2firMokCjjGgoK8d4MXcrgL7XJaL3w6fVg\",
            \"filters\": [
            {
                \"memcmp\": {
                \"offset\": 100,
                \"data\": {
                        \"bytes\": [
                        115,
                        101,
                        114,
                        117,
                        109,
                        5,
                        0,
                        0,
                        0,
                        0,
                        0,
                        0,
                        0
                        ]
                    }
                }
            }
            ]
        },
        {
            \"accounts\": [],
            \"programId\": \"4MangoMjqJ2firMokCjjGgoK8d4MXcrgL7XJaL3w6fVg\"
        },
        {
            \"accounts\": [],
            \"programId\": \"4MangoMjqJ2firMokCjjGgoK8d4MXcrgL7XJaL3w6fVg\",
            \"filters\": [
                {
                    \"datasize\": 200
                },
                {
                    \"memcmp\": {
                    \"offset\": 100,
                    \"data\": {
                        \"bytes\": [
                        115,
                        101,
                        114,
                        117,
                        109,
                        5,
                        0,
                        0,
                        0,
                        0,
                        0,
                        0,
                        0
                        ]
                    }
                    }
                }
            ]
        },
        {
            \"accounts\": [\"4MangoMjqJ2firMokCjjGgoK8d4MXcrgL7XJaL3w6fVg\", \"srmqPvymJeFKQ4zGQed1GFppgkRHL9kaELCbyksJtPX\"]
        }
        ]
        ";

    let filters: AccountFilters = serde_json::from_str(str).unwrap();

    assert_eq!(
        filters[0],
        AccountFilter {
            accounts: vec![],
            program_id: Some("4MangoMjqJ2firMokCjjGgoK8d4MXcrgL7XJaL3w6fVg".to_string()),
            filters: Some(vec![AccountFilterType::Datasize(200)])
        }
    );

    assert_eq!(
        filters[1],
        AccountFilter {
            accounts: vec![],
            program_id: Some("4MangoMjqJ2firMokCjjGgoK8d4MXcrgL7XJaL3w6fVg".to_string()),
            filters: Some(vec![AccountFilterType::Memcmp(MemcmpFilter {
                offset: 100,
                data: MemcmpFilterData::Bytes(vec![
                    115, 101, 114, 117, 109, 5, 0, 0, 0, 0, 0, 0, 0
                ])
            })])
        }
    );

    assert_eq!(
        filters[2],
        AccountFilter {
            accounts: vec![],
            program_id: Some("4MangoMjqJ2firMokCjjGgoK8d4MXcrgL7XJaL3w6fVg".to_string()),
            filters: None
        }
    );

    assert_eq!(
        filters[3],
        AccountFilter {
            accounts: vec![],
            program_id: Some("4MangoMjqJ2firMokCjjGgoK8d4MXcrgL7XJaL3w6fVg".to_string()),
            filters: Some(vec![
                AccountFilterType::Datasize(200),
                AccountFilterType::Memcmp(MemcmpFilter {
                    offset: 100,
                    data: MemcmpFilterData::Bytes(vec![
                        115, 101, 114, 117, 109, 5, 0, 0, 0, 0, 0, 0, 0
                    ])
                })
            ])
        }
    );

    assert_eq!(
        filters[4],
        AccountFilter {
            accounts: vec![
                "4MangoMjqJ2firMokCjjGgoK8d4MXcrgL7XJaL3w6fVg".to_string(),
                "srmqPvymJeFKQ4zGQed1GFppgkRHL9kaELCbyksJtPX".to_string()
            ],
            program_id: None,
            filters: None
        }
    );
}
