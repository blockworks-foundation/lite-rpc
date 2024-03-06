
-- add index of transaction in block
ALTER TABLE rpc2a_epoch_607.transaction_blockdata ADD COLUMN idx int4;
UPDATE rpc2a_epoch_607.transaction_blockdata set idx=0 WHERE idx is null;
ALTER TABLE rpc2a_epoch_607.transaction_blockdata SET NOT NULL;
