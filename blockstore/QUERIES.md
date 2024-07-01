## Queries


#### Get Transactions Per Slot
```sql
SELECT
	tx_block.idx, tx_map.signature,
	(err is not null) as has_error
FROM rpc2a_epoch_661.transaction_blockdata tx_block
INNER JOIN rpc2a_epoch_661.transaction_ids tx_map USING (transaction_id)
WHERE slot=280323677
ORDER BY idx
```

## Conventions for SQL Table Names and Aliases


| Table Name            | Alias    |
|-----------------------|----------|
| transaction_blockdata | tx_block |
| transaction_ids       | tx_map   |
| account_ids           | acc_map  |
