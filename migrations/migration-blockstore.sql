-- backported patches; put latest on bottom

-- add index of transaction in block
ALTER TABLE rpc2a_epoch_607.transaction_blockdata ADD COLUMN idx int4 DEFAULTL 0;
ALTER TABLE rpc2a_epoch_607.transaction_blockdata ALTER COLUMN idx DROP DEFAULT;

ALTER TABLE rpc2a_epoch_607.transaction_blockdata ADD COLUMN message_version int4 DEFAULT -2020;
ALTER TABLE rpc2a_epoch_607.transaction_blockdata ALTER COLUMN message_version DROP DEFAULT;

ALTER TABLE rpc2a_epoch_607.transaction_blockdata ADD COLUMN fee int8 DEFAULT 4999;
ALTER TABLE rpc2a_epoch_607.transaction_blockdata ALTER COLUMN fee DROP DEFAULT;

ALTER TABLE rpc2a_epoch_607.transaction_blockdata ADD COLUMN pre_balances int8[] DEFAULT ARRAY[]::int8[];
ALTER TABLE rpc2a_epoch_607.transaction_blockdata ALTER COLUMN pre_balances DROP DEFAULT;

ALTER TABLE rpc2a_epoch_607.transaction_blockdata ADD COLUMN post_balances int8[] DEFAULT ARRAY[]::int8[];
ALTER TABLE rpc2a_epoch_607.transaction_blockdata ALTER COLUMN post_balances DROP DEFAULT;


