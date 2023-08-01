### Task definition

```mermaid
stateDiagram
    a1: Architecture skeleton
    c1: geyser connector
    c2: Faithful connector
    c22: Triton block index
    c3: TPU connector
    c4: gossip listening
    s1: Data model
    s2: Storage definition
    s3: Block storage
    s4: Cluster storage
    b1: Block processing
    b5: Block RPC call
    t1: Tx processing
    t3: Sendtransacton
    t4: Tx RPC call
    h1: History getBlock
    h2: History getBlocks
    h3: History calls
    cl1: Cluster processing
    cl2: cluster RPC solana
    cl6: cluster RPC call

    
    [*] --> a1
    [*] --> c1
    [*] --> c2
    c2 --> c22
    [*] --> c3
    [*] --> c4
    [*] --> s1
    [*] --> cl1
    s1 --> s2
    s2 --> s3
    s2 --> s4
    a1 --> s2
    a1 --> b1
    b1 --> b5
    a1 --> t1
    t1 --> t3
    t1 --> t4
    cl1 --> cl2
    cl1 --> cl6
    s3 --> h1
    c22 --> h1
    b1 --> h1
    h1 --> h2
    h2 --> h3
```


#### Tasks list

- Architecture skeleton: do the mimimun code to define the first version of the architecture implementation.
- geyser connector: Geyser connector code to all geyser data. Data not available use the current RPC access.
- Faithful connector: connect to the Faithful service to query their method.
- Triton block index: research task on do we use the triton block index and how.
- TPU connector: manage TPU connection and connect to a specified validator's TPU port and send Tx batch
- gossip listening: Listen to validator gossip and extract data.
- Data model: define the stored data model + indexes for block and Cluster.
- Storage definition: implement the storage infra to host block and cluster data query.
- Block storage: implement block storage with epoch switch
- Cluster storage: implement cluster storage with epoch switch
- Block processing: implement block processing management. Base task:
    - Update block: update block process
    - get block: get block from cache and storage.
    - get current data: get block current data
- Block RPC call: implements block RPC call.
- Tx processing: implement Tx processing algo
    - notify Tx: notify Tx to other modules
    - update Tx cache
- Sendtransacton
    - send Tx
    - confirm Tx
    - replay Tx
- Tx RPC call: get Tx + Status
- History getBlock: integrate block index or/and call Faithful service
- History getBlocks: query indexes or add Faithful service call.
- History other calls: implements the other history call.
- Cluster processing: cluster data processing implementation.
    - cluster data
    - cluster epoch update
    - cluster info
    - cluster epoch notification
- Cluster RPC solana
- Cluster RPC call
