use std::path::PathBuf;

use solana_accounts_db::accounts::Accounts;
use solana_accounts_db::accounts_db::AccountsDb;
use solana_sdk::account::{AccountSharedData, ReadableAccount};
use solana_sdk::clock::Slot;
use solana_sdk::pubkey::Pubkey;

fn new_accounts_db(account_paths: Vec<PathBuf>) -> AccountsDb {
    AccountsDb::new_single_for_tests()
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use crate::accounts_db::create_test_accounts;
    use solana_accounts_db::accounts::Accounts;
    use solana_accounts_db::accounts_db::AccountsDb;
    use solana_sdk::pubkey::Pubkey;

    #[test]
    fn test() {
        let db = AccountsDb::new_single_for_tests();
        let accounts = Accounts::new(Arc::new(db));

        let num_slots = 4;
        let num_accounts = 10_000;

        println!("Creating {num_accounts} accounts");

        let pubkeys: Vec<_> = (0..num_slots)
            .into_iter()
            .map(|slot| {
                let mut pubkeys: Vec<Pubkey> = vec![];
                create_test_accounts(
                    &accounts,
                    &mut pubkeys,
                    num_accounts / num_slots,
                    slot as u64,
                );
                pubkeys
            })
            .collect();

        let pubkeys: Vec<_> = pubkeys.into_iter().flatten().collect();

        println!("{:?}", pubkeys);
    }
}

pub fn create_test_accounts(
    accounts: &Accounts,
    pubkeys: &mut Vec<Pubkey>,
    num: usize,
    slot: Slot,
) {
    let data_size = 0;

    for t in 0..num {
        let pubkey = solana_sdk::pubkey::new_rand();
        let account = AccountSharedData::new(
            (t + 1) as u64,
            data_size,
            AccountSharedData::default().owner(),
        );
        accounts.store_slow_uncached(slot, &pubkey, &account);
        pubkeys.push(pubkey);
    }
}
