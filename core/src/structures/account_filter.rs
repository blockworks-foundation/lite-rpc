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
    TokenAccountState,
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
                    AccountFilterType::TokenAccountState => RpcFilterType::TokenAccountState,
                })
                .collect_vec()
        })
    }
}

#[allow(deprecated)]
impl From<&RpcFilterType> for AccountFilterType {
    fn from(value: &RpcFilterType) -> Self {
        match value {
            RpcFilterType::DataSize(size) => AccountFilterType::Datasize(*size),
            RpcFilterType::Memcmp(memcmp) => {
                let bytes = memcmp.bytes().map(|x| (*x).clone()).unwrap_or_default();
                let offset = memcmp.offset as u64;
                AccountFilterType::Memcmp(MemcmpFilter {
                    offset,
                    data: MemcmpFilterData::Bytes(bytes),
                })
            }
            RpcFilterType::TokenAccountState => AccountFilterType::TokenAccountState,
        }
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

#[test]
fn workspace_write_your_own_filter() {
    // update the filters and then run the test using `cargo test -- workspace_write_your_own_filter --nocapture` then use the printed line to update config.json
    let mut filters = AccountFilters::new();

    let mango_program_filter = AccountFilter {
        accounts: vec![],
        program_id: Some("4MangoMjqJ2firMokCjjGgoK8d4MXcrgL7XJaL3w6fVg".to_string()),
        filters: None,
    };

    filters.push(mango_program_filter);

    let oracles = [
        "9BoFW2JxdCDodsa2zfxAZpyT9yiTgSYEcHdNSuA7s5Sf",
        "CYGfrBJB9HgLf9iZyN4aH5HvUAi2htQ4MjPxeXMf4Egn",
        "7UYk5yhrQtFbZV2bLX1gtqN7QdU9xpBMyAk7tFgoTatk",
        "2PRxDHabumHHv6fgcrcyvHsV8ENkWdEph27vhpbSMLn3",
        "CtJ8EkqLmeYyGB8s4jevpeNsvmD4dxVR2krfsDLcvV8Y",
        "3pxTFXBJbTAtHLCgSWjeasngGCi4ohV16F4fDnd4Xh81",
        "GVXRSBjFk6e6J3NbVPXohDJetcTjaeeuykUpbQF8UoMU",
        "2dAsTriwLdgmGt7N6Dkq1iUV6pGhSUUwqqePp4qorzor",
        "FnVC5oSSdnCHfN5W7xbu74HbxXF3Kmy63gUKWdaaZwD7",
        "6ABgrEZk8urs6kJ1JNdC1sspH5zKXRqxy8sg3ZG2cQps",
        "7moA1i5vQUpfDwSpK6Pw9s56ahB7WFGidtbL2ujWrVvm",
        "Bt1hEbY62aMriY1SyQqbeZbm8VmSbQVGBFzSzMuVNWzN",
        "4ivThkX8uRxBpHsdWSqyXYihzKF3zpRGAUCqyuagnLoV",
        "D8UUgr8a3aR3yUeHLu7v8FWK7E8Y5sSU7qrYBXUJXBQ5",
        "7fMKXU6AnatycNu1CAMndLkKmDPtjZaPNZSJSfXR92Ez",
        "JBu1AL4obBcCMqKBBxhpWCNUt136ijcuMZLFvTP7iWdB",
        "AnLf8tVYCM816gmBjiy8n53eXKKEDydT5piYjjQDPgTB",
        "4dusJxxxiYrMTLGYS6cCAyu3gPn2xXLBjS7orMToZHi1",
        "hnkVVuJTRZvX2SawUsecZz2eHJP2oGMdnhdDJa33KSY",
        "AFrYBhb5wKQtxRS9UA9YRS4V3dwFm7SqmS6DHKq6YVgo",
        "Ag7RdWj5t3U9avU4XKAY7rBbGDCNz456ckNmcpW1aHoE",
        "AwpALBTXcaz2t6BayXvQQu7eZ6h7u2UNRCQNmD9ShY7Z",
        "8ihFLu5FimgTQ1Unh4dVyEHUGodJ5gJQCrQf4KUVB9bN",
        "AV67ufGVkHrPKXdeupXE2MXdw3puq7xnkPNrTxGP3suU",
        "3uZCMHY3vnNJspSVk6TvE9qmb4iYVbrEWFQ71uCE5hFR",
        "5wRjzrwWZG3af3FE26ZrRj3s8A3BVNyeJ9Pt9Uf2ogdf",
        "2qHkYmAn7HNtAGw45hQQkRthDDNiyVyVfDJDaw6iSoRm",
        "2FGoL9PNhNGpduRKLsTa4teRaX3vfarXAc1an2KyXxQm",
        "4BA3RcS4zE32WWgp49vvvre2t6nXY1W1kMyKZxeeuUey",
        "Bfz5q3cDywSSjnWb9oXeQZqYzHwqFGp75mm34eYCPNEA",
        "H6ARHf6YXhGYeQfUzQNGk6rDNnLBQKrenN712K4AQJEG",
        "79wm3jjcPr6RaNQ4DGvP5KxG1mNd3gEBsg6FsNVFezK4",
        "Gnt27xtC473ZT2Mw5u8wZ68Z3gULkSTb5DuxJy7eJotD",
        "g6eRCbboSwK4tSWngn773RCMexr1APQr4uA9bGZBYfo",
        "91Sfpm86H7ZgngdGfAiVJTNbg42CXBPiurruf29kinMh",
        "nrYkQQQur7z8rYTST3G9GqATviK5SxTDkrqd21MW6Ue",
        "E4v1BBgoso9s64TQvmyownAVJbhbEPGyzA3qn4n46qj9",
        "EzBoEHzYSx37RULrQCh756kNcA7iLrmGesxqpzSwo4v3",
        "3vxLXJqLqF3JG5TCbYycbKWRBbCJQLxQmBGCkyqEEefL",
        "7yyaeuJ1GGtVBLT2z2xub5ZWYKaNhF28mj1RdV4VDFVk",
        "BeAZ81UvesnJR7VVGNzRQGKFHrnxm77x5ozesC1pTjrY",
        "H5hokc8gcKezGcwbqFbss99QrpA3WxsRfqGYCm6F1EBy",
        "ELrhqYY3WjLRnLwWt3u7sMykNc87EScEAsyCyrDDSAXv",
        "FYghp2wYzq36yqXYd8D3Lu6jpMWETHTtxYDZPXdpppyc",
    ];
    let oracle_filters = AccountFilter {
        accounts: oracles.iter().map(|x| x.to_string()).collect_vec(),
        program_id: None,
        filters: None,
    };
    filters.push(oracle_filters);

    let open_orders = AccountFilter {
        accounts: vec![],
        program_id: Some("srmqPvymJeFKQ4zGQed1GFppgkRHL9kaELCbyksJtPX".to_string()),
        filters: Some(vec![
            AccountFilterType::Datasize(3228),
            AccountFilterType::Memcmp(MemcmpFilter {
                offset: 0,
                data: MemcmpFilterData::Bytes(
                    [0x73, 0x65, 0x72, 0x75, 0x6d, 5, 0, 0, 0, 0, 0, 0, 0].to_vec(),
                ),
            }),
            AccountFilterType::Memcmp(MemcmpFilter {
                offset: 45,
                data: MemcmpFilterData::Bytes(
                    [
                        91, 23, 199, 200, 106, 110, 115, 159, 175, 23, 81, 129, 131, 99, 233, 79,
                        144, 139, 243, 112, 4, 206, 109, 63, 188, 241, 151, 189, 210, 245, 31, 28,
                    ]
                    .to_vec(),
                ),
            }),
        ]),
    };
    filters.push(open_orders);

    let filter_string = serde_json::to_string(&filters).unwrap();
    let filter_string = filter_string.replace('"', "\\\"");
    println!("Filter is : \n {} \n", filter_string);
}
