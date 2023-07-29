# RPCV2 Solana updates

## Summary table

Bootstrap not studied.

| Call     |  Sources  | Extern dev |
| -------- | -------- | -------- |
| getBlock |  geyser + Faithfull | None |
| getBlocks |  geyser + Faithfull | Decide where the block indexes are. |
| getSignaturesForAddress |  Geyser + Faithfull | None (see evolutions) |
| getclusternodes | None | Update geyser or use gossip |
| getepochinfo | None | Update geyser Epoch subscription |
| getLeaderSchedule | None | Update geyser see getVoteAccounts |
| getVoteAccounts | None | Update geyser to add epoch notification |
| getRecentPerformanceSamples | None | Update geyser plugin, add the call. |
| sendtransaction | Some is missing | Need getclusternodes and getLeaderSchedule call |
| getSignatureStatuses | Part. | Update geyser add Processed commitment notification. |
| getTransaction | geyser + Faithfull | None |
| getRecentPrioritizationFees | geyser + local algo | None |
| getslot |  geyser | Need Processed commitment. |
| getBlockHeight | geyser | None || GetBlockHeight |  geyser     | None     |
| getBlockTime | local algo | None |
| getFirstAvailableBlock | Faithful plugin + local algo | None |
| getLatestBlockhash | geyser | None |
| isBlockhashValid | geyser | None |
| getBlockCommitment | Geyser + local algo | None |

## Evolutions
### getBlocks
Need the Faithful block index. The index can be copied on in the RPC node and updated by the node. The query is done by the RPC node. Otherwise it's Faithful that keep its index and the service need to add the *getBlocks* method.

To be defined.

### getclusternodes
1) Geyser: :
  * a) propose the same get_cluster_nodes impl to get the cluster a any time
  * b) notify at the beginning of the epoch. Change the call name, enough for us but perhaps not for every user: change notifier to add a ClusterInfo notifier like BlockMetadataNotifier for block metadata.
    Need to change the plugin interface, add notify_cluster_at_epoch. We can notify every time the cluster info is changed but not sure Solana will accept (too much data).
2) Use gossip and use the GossipService impl like the bootstrap::start_gossip_node() function. Need to use ClusterInfo struct that use BankForks to get staked_nodes.
For node info it seems that the stake is not needed. The gossip process is:
 * For shred data: crds_gossip::new_push_messages() -> crds::insert() that update peers with CrdsData::LegacyContactInfo(node). So by getting gossip message with LegacyContactInfo we can update the cluster node info.

### getepochinfo
Add a geyser subscription for epoch that contains static epoch data: epoch, start slot, slotsInEpoch
Use full block and slot subscription for the epoch changing data: absoluteSlot, blockHeight, slotIndex, transactionCount

### getLeaderSchedule
In solana, Leader schedule is computed by the method leader_schedule_utils::leader_schedule(epoch, bank); . It need the leaders stake activated at the beginning of the epoch
1) get them using Vote account for the managed epoch in the storage see: getVoteAccounts return only the vote account of the current epoch.
2) Add geyser plugin call. Use the Validator bank. Use bank.epoch_staked_nodes(epoch) to get the stake. Can return the stake and schedule for all epoch in the validator.

### getVoteAccounts
Vote account can only be retrieve from the validator. Need to update the geyser plugin. Only need epoch stake.

Add a subscription method to geyser plugin the notify the current vote accounts when connecting and at each new epoch.

### getRecentPerformanceSamples
Add the same call to geyser plugin. The data can only be retrieve from the validator.

### sendtransaction
- leader schedule to send the Tx to the current and next leader: see getLeaderSchedule
- leader ip to send the tx: see getclusternodes

### getSignatureStatuses
geyser plugin subscribe full block with Tx. The plugin must be changed to get *processed* status block notification.

Discussion to avoid to send several time the same data, the processed block notification can only contains the signature hash and not the content for example. 

### getslot
The geyser plugin slot subscription. In the current impl the processed commitment is not notified. Need the plugin in update.

## Solana Geyser modifications
Several possibilities exist to generate the needed data. This is the actual choice of modification that seems the best to implements the RPC calls.

- Add a geyser subscription for epoch:
    - Cluster Info
    - leader schedule: can be calculate but it's better to have one algo reference
    - epoch, start slot, slotsInEpoch
    - total staking: can be calculate but it's better to have one algo reference
    - Votes accounts
Notified when connecting and at each epoch beginning.

- add geyser plugin call: getRecentPerformanceSamples
- notify slot, Block Meta and BLock at *Processed* commitment. Can send part of the data for the block.