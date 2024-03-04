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
        "2sn1w3krTLhW4EDbLL5etf8NjwSHMhmK6CJmWYpkMoXL",
        "Fi8vncGpNKbq62gPo56G4toCehWNy77GgqGkTaAF5Lkk",
        "9puoc5B1ioxkKMMx1rs1M5kLWhCDrPawkCnTkk44jjCs",
        "7KbMt281Rjx3E3MU7GSEEBfByhNBxfU1dZbTimGaKCfV",
        "7tV5jsyNUg9j1AARv56b7AirdpLBecibRXLEJtycEgpP",
        "ARjaHVxGCQfTvvKjLd7U7srvk6orthZSE6uqWchCczZc",
        "E5AmUKMFgxjEihVwEQNrNfnri5EexYHSBC4HkicVtfxG",
        "GoXhYTpRF4vs4gx48S7XhbaukVbJXVycXimhGfzWNGLF",
        "BWt1ABFexE3gEKRFGZmMo2DFLgAU8SUJc4NZi1t3jvUP",
        "5BocsfuYVNzu5huEbx3jreueTPzsKaGQMj9Xwb5enMP",
        "7n7TafVxb1j3Zwh2sX3s8k3eXg7UHp5VZnb9uhhCLvy9",
        "EA1eJqandDNrw627mSA1Rrp2xMUvWoJBz2WwQxZYP9YX",
        "EN41nj1uHaTHmJJLPTerwVu2P9r5G8pMiNvfNX5V2PtP",
        "DvfMMNzFTvPofaEzdt3ez4U6ziRJcMCm7LGGKskznThz",
        "HDwpKCNpB9JvcGrZv6TWcXjFvzxxfzq7ci6kQ1Kv8FMY",
        "GgpDLzVuUFLyNzNn6oVVEVxbmkdp2xkjX8BrzaFFt3yA",
        "GFJjJmm7jTDb7WEM4TkYdA9eAEeJGK1t73tcdDNeZLGT",
        "CeQ7wj43PJ28EXU1QVNMPxmwrg955KejYD68bMYWTvAp",
        "27Nuo9hEVSVrrUyQ83NPnuDR4ZSE8KBb3xiU1XeaBVwL",
        "CcyCcZyvfEkBUngS7KytFPWKWSrpDfmHx81aqAtgS9oC",
        "2JAg3Rm6TmQ3gSYgUCCyZ9bCQKThD9jxHCN6U2ByTPMb",
        "Dj1qPXjnWWMkuPiq4Y51JNvSkCBqU38P541Te2ugqYpD",
        "Ed3EJ3jxXWGgDAkWLXfGojStQ7g1SbYNQGhetMSm6PKb",
        "8JK7S6h5FktYXq4wC75WeyNFWZpa1jMNXNtTNuy26ixy",
        "2mBnnBywAuMwH5FhH27UUFyDGk7J77m5LcKK4VtmwJQi",
        "Fb5BfdB7zk2zfWfqgpRtRQbYSYERASsBjz213FaT461F",
        "2oxZZ3YXaVhbZmtzagGooewBAofyVbBTzayAD9UR1eBh",
        "3BAKsQd3RuhZKES2DGysMhjBdwjZYKYmxRqnSMtZ4KSN",
        "BWMbNAMVkz197EsnhZ3rHCJAB1BYgaQxHaDzjeVvgXdk",
        "Gi8KdURhXWvsRvDFHpqy1gNfnnYuTsWexQvDpP9711id",
        "EkKZwBeKWPvhraYERfUNr2fdh1eazrbTrQXYkRZs24XB",
        "9UPokT1qLN2PVMxGNSYnRnYNgRRJTwr9dzLctFFZQFa2",
        "8CvwxZ9Db6XbLD46NZwwmVDZZRDy7eydFcAGkXKh9axa",
        "5x2sfymw7CcrWx6WvZ5UV7Bg1iZTXbwh1XUhxKqhJqni",
        "DkbVbMhFxswS32xnn1K2UY4aoBugXooBTxdzkWWDWRkH",
        "CmHpRnmd8h6kH8ogwLuihKnGYJvaS2PdXNjBt2JhFT5w",
        "FALKx6CxTcwzpTdihdR7ZuK3Wd2H2aEvCNBS4K8UfPxe",
        "AYhLYoDr6QCtVb5n1M5hsWLG74oB8VEz378brxGTnjjn",
        "8PhnCfgqpgFM7ZJvttGdBVMXHuU4Q23ACxCvWkbs1M71",
        "JCKa72xFYGWBEVJZ7AKZ2ofugWPBfrrouQviaGaohi3R",
        "FsruqicZDGnCnm7dRthjL5eFrTmaNRkkomxhPJQP2kdu",
        "FGZCgVhVGqzfWnmJFP9Hx4BvGvnFApEp1dM2whzXvg1Z",
        "YFzPfYrMTWPZEhhvq5QyHEu5otDrQYVtfxzRbkTHQvd",
        "4hgruY5SXYRHSrJFBjX4DFf39ef2wgYGGasjrUtwS9S1",
        "2KajVpMkF3Z53MgTgo7dDh23av6xWKgKssbtjogdY7Vu",
        "72h8rWaWwfPUL36PAFqyQZU8RT1V3FKG7Nc45aK89xTs",
        "6QNusiQ1g7fKierMQhNeAJxfLXomfcAX3tGRMwxfESsw",
        "74fKpZ1NFfusLacyVzQdMXXawe9Dr1Kz8Yw1cw12QQ3y",
        "BEhRuJZiKwTdVTsGYjbHRh9RmGbKBtT6xo7yPqxLiSSY",
        "3NnxQvDcZXputNMxaxsGvqiKpqgPfSYXpNigZNFcknmD",
        "8QCdRwLp5CX2XYVaKX3GFxsbc8n7M2xEtMXyAa8tL7r3",
        "H6Wvvx5dpt8yGdwsqAsz9WDkT43eQUHwAiafDvbcTQoQ",
        "28dSAygC8Vqzbm5r7f3mPnQ6vKVqXkjzoXD9SVpi75jV",
        "5jWUncPNBMZJ3sTHKmMLszypVkoRK6bfEQMQUHweeQnh",
        "DFizHnakzudcEfz4YKrsxQsRADgUMc6ifaMs3wU9pBSV",
        "4RNVNS8EZWwkYgNeRV4A75oScC3afD7cWV6VLywRcKik",
        "AmFXLH3jbcQNqgJjVuMZCeiaU2HmrW1UwMTWR5wU4ijd",
        "DG5EXd99EfnMFXqVXjciWi5HuXHUKdwkQ2WCncEsCeKW",
        "9zH66LpNcwBausdXLT765dgyLZSiTGUno22orC6Q3AFT",
        "9LezACAkFsv78P7nBJEzi6QeF9h1QF8hGx2LRN7u9Vww",
        "6nh2KwhGF8Tott22smj2E3G1R15iXhBrL7Lx6vKgdPFK",
        "4E17F3BxtNVqzVsirxguuqkpYLtFgCR6NfTpccPh82WE",
        "3FFGnQWo7LH5qHK96yXFxRzGL7wB3BZqJpW25rk6xZkP",
        "B2na8Awyd7cpC59iEU43FagJAPLigr3AP3s38KM982bu",
        "9Lyhks5bQQxb9EyyX55NtgKQzpM4WK7JCmeaWuQ5MoXD",
        "CXMRrGEseppLPmzYJsx5vYwTkaDEag4A9LJvgrAeNpF",
        "G8KnvNg5puzLmxQVeWT2cRHCm1XmurbWGG9B8Bze6mav",
        "HKm7iBQw488qHyXYh5wpqKnMpvbu3TaH4wVWf52i4d8",
        "8xdpZNtxfWY96sKH6LBmDbRYDhMqmujuXRnvr9xDF9mt",
        "4WeAXG1V8QTtt3T9ao6LkQa8m1AuwRcY8YLvVcabiuby",
        "H87FfmHABiZLRGrDsXRZtqq25YpARzaokCzL1vMYGiep",
        "AuqKXU1Nb5XvRxr5A4vRBLnnSJrdujNJV7HWsfj4KBWS",
        "DcM7ufYEveXMfB1HQru1jHh7td6DPxgDDJRL8LR79gMb",
        "27BrDDYtv9NDQCALCNnDqe3BqjYkgiaQwKBbyqCA8p8B",
        "8BnEgHoWFysVcuFFX7QztDmzuH8r5ZFvyP3sYwn1XTh6",
        "6wD9zcNZi2VpvUB8dnEsF242Gf1tn6qNhLF2UZ3w9MwD",
        "Do9Xu1dvgZukExvRLHsnH8cHzjMrhrGxY81ukEudm4XX",
        "9FjM1wHvGg2ZZaB3XyRsYELoQE7iD6uwHXizQUDKRYff",
        "BqApFW7DwXThCDZAbK13nbHksEsv6YJMCdj58sJmRLdy",
        "3wueUtibiTVWJZoL1WhphgDw48r9LLzUriT1a4CnZfGG",
        "51Tk5PRCwvz5L3Z8EGw3HpW9YMLMGwyqRiDbNV4T1GXi",
        "6yBKgj4PZK2sShV2e8rKDc8EArsvScmhLm328iKasBvh",
        "Dq2eptabWaGfFm6QezfX9BzfTuWA58jXhwXMHUnfuUe8",
        "CsB4XjwH4uZRjRXEXRBFJ3hi3mb9jujzRCW7NXg7TRtX",
        "HKEJCi2gVDWGfUB7YufNtJjyZiBgM9KZk9tgRE1r3RsX",
        "3QDSfdXSbUhGFe6K4EZbasfpyUTndAzkkLdX9HGpRbB9",
        "HCaq6dL3DJzKyWEeSCCDy9NdKFRL56Ad7G4bSVVMRchr",
        "CgPg6FRyerP6esjvadjdAJCKboLJzbu2ihWgjquHdAvn",
        "6z1KcPcBnrzebgYjHwVTzu6VgcxY6niLfo3dxF1n2xSx",
        "BbJgE7HZMaDp5NTYvRh5jZSkQPVDTU8ubPFtpogUkEj4",
        "4Ymax5Tmk7LACxDtPPFHB4w89557era7wyxodUBqd4fW",
        "8nA9AqeGsviExkDZChaviW4mGwmdw6GYQAcUYCrszj42",
        "Hs97TCZeuYiJxooo3U73qEHXg3dKpRL4uYKYRryEK9CF",
        "Dgt13dmzN6cZFgGE3hjfm4VVFE7pxCL2GeTmxbz4fmfA",
        "2BtDHBTCTUxvdur498ZEcMgimasaFrY5GzLv8wS8XgCb",
        "HvRdVui29hRwToAPMEfQJY795EC1XyW2ZoWvuSmgqq7v",
        "5EKbLBd12TVUUSh1WH4jE884S1XaikSga7pjMNA8jnxD",
        "Cop9Lvri1yYEmV8fWuG6i7EAJ2RdPynU66ZrhM8kGH9V",
        "iAHbTAWfvM269DnnzzQgMocMZLV8PnUoNF7g33i95dR",
        "DZjbn4XC8qoHKikZqzmhemykVzmossoayV9ffbsUqxVj",
        "H5uQWZrwRm3EABCzYRmDmti5UnMTUP5ifxsPHrJHpZdC",
        "Cn6Fu2MfaE6sahWwa8HGZQuKuLD6Fd8GJ784Tv7UtdRu",
        "7S2fEFvce5n9hGpjp9jd8JRfuBngcDJfykygeqqzEwmq",
        "5U2Vzi7Lwvyzw7hoHKpAZUD2SPfmsYfydRZrejmquso2",
        "H6rrYK3SUHF2eguZCyJxnSBMJqjXhUtuaki6PHiutvum",
        "EaXdHx7x3mdGA38j5RSmKYSXMzAFzzUXCLNBEDXDn1d5",
        "BBRvF4etMRitpSwFdXSMzPg547Lxnr3G95aQtBhMiWhB",
        "BAPLDQS9wa5BwzEuWeA1CMKAxjASieFzgSWW9nEJdMCc",
        "3PoEWQNTCaoDsBH32Rmp9yqbgFtg4Z4ik13kkRyN2CQf",
        "6NfwhGjfrE4dp7XFut2kYcUQsH4P4AJVSoe9cLVM2z43",
        "FbwncFP5bZjdx8J6yfDDTrCmmMkwieuape1enCvwLG33",
        "CK1X54onkDCqVnqY7hnvhcT7EosnjiLTwPBXAMLxkA2A",
        "kB3BTG6Pz3W7JzKQLHyrAZaFk1LVmeeXRP4ZiCKUaA3",
        "63XwffQkMcNqEacDNhixmBxnydkRE3uigV7VoLNfqh9k",
        "CFxJ8jyFQinvFSL6CNQCLWyPnbeYcCpCWmfPFC43qMkp",
        "21QRUCjxuXC9GAyTGvfYyumWYTqCtewM3ZcCfFDTYi5L",
        "ASUyMMNBpFzpW3zDSPYdDVggKajq1DMKFFPK1JS9hoSR",
        "FyyGTHKJBf1nGHHL44HE91EcGRFg2Y7XazA3SjQpcU3i",
        "CVwFcGuWYH1chdbR988K7ppbwVLErqfdreFCPsg9ttnq",
        "CDm1Uaos4vWPXezgEobUarGJ6ddKCywvFp8XLcNSqzU9",
        "4P4pgJC7omZWRL4qmngUMW3ETTr2yLXhgaoAPpSp8TWG",
        "3rQH87K3UfrDjbjSktHy7EwQHvX4BoRu3Py52D25gKSS",
        "CCepXEQxo8eTqCGtRHXrSnZdhCEQjQeEW3M85AH9skMJ",
        "GqEejRjBRnZTZg417SUmSwpJ4nhTK5E8Ey8JeVPd3cY5",
        "2m7ZLEKtxWF29727DSb5D91erpXPUY1bqhRWRC3wQX7u",
        "G4BZUKuUy1qkjtG3xZN8x4tXoJrjPhuxtU5t2HHgMaZe",
        "CC9VYJprbxacpiS94tPJ1GyBhfvrLQbUiUSVMWvFohNW",
        "6XsUQYAkKSy4mSQfMxYqpF4U7X3JsPDbG4vRQQEvCPb6",
        "6YSbESJWKwtC4x7rBdHbcYSVEejCWo2sZkapKQE8Y289",
        "NiWvcoCvqmUKSf1Avey5KWBXa5oAm6h7LWv4sqyCbBn",
        "AZUaEDdGXiryUbAi56MMrn1Em8t2dSy5mvq5SxX6S1np",
        "AgCBUZ6UMWqPLftTxeAqpQxtrfiCyL2HgRfmmM6QTfCj",
        "7AdBcVGejZy3981JfJ8mDywzMqAL31vBpgnD3PpnTpms",
        "Av8JgExs2LDubedW47Kvw8J47hko45P8TYqiHmtD2Ey6",
        "5aNxjc8upaPiGvcC9YJ48geiRSQLzGEdeL1zJakLn7t3",
        "9h4bddiPyfTyTztuyo7mEcpQz3QrR6cSPt1id6UmWj5H",
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
