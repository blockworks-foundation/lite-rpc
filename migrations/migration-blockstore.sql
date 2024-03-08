-- backported patches; put latest on bottom

-- add index of transaction in block
ALTER TABLE rpc2a_epoch_607.transaction_blockdata ADD COLUMN idx int4 DEFAULTL 0;
ALTER TABLE rpc2a_epoch_607.transaction_blockdata ALTER COLUMN idx DROP DEFAULT;

ALTER TABLE rpc2a_epoch_607.transaction_blockdata ADD COLUMN message_version int4 DEFAULT -2020;
ALTER TABLE rpc2a_epoch_607.transaction_blockdata ALTER COLUMN message_version DROP DEFAULT;

