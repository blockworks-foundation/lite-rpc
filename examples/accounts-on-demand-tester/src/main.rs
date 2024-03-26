use csv::ReaderBuilder;
use futures::future::join_all;
use futures::FutureExt;
use log::info;
use solana_rpc_client::nonblocking::rpc_client::RpcClient;
use solana_rpc_client_api::client_error::ErrorKind;
use solana_rpc_client_api::request::RpcError;
use solana_sdk::commitment_config::CommitmentConfig;
use solana_sdk::pubkey::Pubkey;
use std::collections::HashMap;
use std::str::FromStr;
use std::sync::Arc;
use tokio::time::sleep;

#[tokio::main]
pub async fn main() {
    tracing_subscriber::fmt::fmt()
        .with_max_level(tracing::Level::INFO)
        .init();
    let csv_with_test_accounts = include_str!("accounts-pubkeys-stresstest.csv");
    let mut reader = ReaderBuilder::new().from_reader(csv_with_test_accounts.as_bytes());

    let mut accounts = vec![];

    // add some account key that do not exist
    for _i in 0..100 {
        accounts.push(Pubkey::new_unique());
    }

    for line in reader.records().take(1000) {
        let record = line.unwrap();
        let pubkey = record.get(0).unwrap();
        let pubkey = Pubkey::from_str(pubkey).unwrap();
        accounts.push(pubkey);
    }

    let rpc_client = Arc::new(RpcClient::new("http://127.0.0.1:8890/".to_string()));

    for account_chunk in accounts.chunks(20) {
        let mut account_fns = vec![];
        let mut account_pks = vec![];
        for account_pk in account_chunk {
            for repeat in 0..5 {
                for _parallel in 0..3 {
                    let fun = rpc_client
                        .get_account_with_commitment(account_pk, CommitmentConfig::processed());
                    let delayed_fun =
                        sleep(std::time::Duration::from_millis(repeat * 10)).then(|_| fun);
                    account_fns.push(delayed_fun);
                    account_pks.push(account_pk);
                }
            }
        }

        let results = join_all(account_fns).await;

        let ok_count = results.iter().filter(|x| x.is_ok()).count();
        let err_count = results.iter().filter(|x| x.is_err()).count();

        let zipped = account_pks.iter().zip(results.iter());

        info!("accounts OK: {:?} ERR: {:?}", ok_count, err_count);

        let mut errors = HashMap::<Pubkey, i32>::with_capacity(1000);
        for (account_pk, ref res) in zipped {
            match res {
                Ok(_rpc_response) => {
                    let errs_so_far = errors.get(account_pk).cloned().unwrap_or(0);
                    info!(
                        "success account: {:?} - prev {} errors",
                        account_pk, errs_so_far
                    );
                }
                Err(res_err) => {
                    if let ErrorKind::RpcError(RpcError::ForUser(for_user)) = &res_err.kind {
                        //info!("ForUser: {:?}", for_user);
                        let pubkey = parse_pubkey(for_user);
                        errors
                            .entry(pubkey)
                            .and_modify(|counter| {
                                *counter *= 1;
                            })
                            .or_insert(0);
                    }
                }
            }
        }
    }
}

fn parse_pubkey(input: &str) -> Pubkey {
    let split: Vec<&str> = input.split(':').collect();
    let split = split[1].trim();
    let split = split.replace("pubkey=", "");
    Pubkey::from_str(&split).unwrap()
}

#[test]
fn parser_foruser_error() {
    let input = "AccountNotFound: pubkey=95oWcswpsjjoeWcWFZzpAQ1CzsAiYur5PwYbs8SxxZCG: error sending request for url (http://127.0.0.1:8890/): error trying to connect: tcp connect error: Connection refused (os error 61)";
    let split: Vec<&str> = input.split(':').collect();
    let split = split[1].trim();
    assert_eq!(split, "pubkey=95oWcswpsjjoeWcWFZzpAQ1CzsAiYur5PwYbs8SxxZCG");
    let split = split.replace("pubkey=", "");
    assert_eq!(split, "95oWcswpsjjoeWcWFZzpAQ1CzsAiYur5PwYbs8SxxZCG");
    assert_eq!(split, parse_pubkey(input).to_string());
}
