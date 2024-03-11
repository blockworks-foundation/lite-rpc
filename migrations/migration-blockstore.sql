-- backported patches; put latest on bottom
-- !!! replace "EPOCH" with actual epoch number

-- add index of transaction in block
ALTER TABLE rpc2a_epoch_EPOCH.transaction_blockdata ADD COLUMN idx int4 DEFAULT 0;
ALTER TABLE rpc2a_epoch_EPOCH.transaction_blockdata ALTER COLUMN idx DROP DEFAULT;

ALTER TABLE rpc2a_epoch_EPOCH.transaction_blockdata ADD COLUMN message_version int4 DEFAULT -2020;
ALTER TABLE rpc2a_epoch_EPOCH.transaction_blockdata ALTER COLUMN message_version DROP DEFAULT;

ALTER TABLE rpc2a_epoch_EPOCH.transaction_blockdata ADD COLUMN fee int8 DEFAULT 4999;
ALTER TABLE rpc2a_epoch_EPOCH.transaction_blockdata ALTER COLUMN fee DROP DEFAULT;

ALTER TABLE rpc2a_epoch_EPOCH.transaction_blockdata ADD COLUMN pre_balances int8[] NOT NULL DEFAULT ARRAY[]::int8[];
ALTER TABLE rpc2a_epoch_EPOCH.transaction_blockdata ALTER COLUMN pre_balances DROP DEFAULT;

ALTER TABLE rpc2a_epoch_EPOCH.transaction_blockdata ADD COLUMN post_balances int8[] NOT NULL DEFAULT ARRAY[]::int8[];
ALTER TABLE rpc2a_epoch_EPOCH.transaction_blockdata ALTER COLUMN post_balances DROP DEFAULT;

ALTER TABLE rpc2a_epoch_EPOCH.transaction_blockdata ADD COLUMN inner_instructions text;
ALTER TABLE rpc2a_epoch_EPOCH.transaction_blockdata ADD COLUMN log_messages text[]

ALTER TABLE rpc2a_epoch_EPOCH.transaction_blockdata ADD COLUMN writable_accounts text[];
ALTER TABLE rpc2a_epoch_EPOCH.transaction_blockdata ADD COLUMN readable_accounts text[];

