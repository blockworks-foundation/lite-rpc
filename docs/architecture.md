## Architecture

``` mermaid
flowchart TB
    rpc(RPC access)
    sup(Supervisor loop)
    sol(Solana connector)
    block(block processor)
    blstore(Block storage)
    tx(Tx processing)
    sendtx(Send TX TPU)
    clus(Cluster processing)
    clusstore(Cluster Storage)
    histo(History Processing)
    faith(Faithful service)
    subgraph Main loop
    rpc --> |new request| sup
    end
    subgraph Solana
    direction TB
    sol --> |new block-slot|sup
    sol --> |new epoch|sup
    sup --> |geyser call| sol
    end
    subgraph Block
    direction TB
    sup --> |update block-block| block
    block --> |get data| sup
    block --> |query block| blstore
    end
    subgraph History
    direction TB
    sup --> |query| histo
    histo --> |get data| block
    histo --> |get data| faith
    end
    subgraph Tx
    direction TB
    sup --> |update block|tx
    sup --> |send Tx request|tx
    tx --> |get data| sup
    tx --> |send Tx| sendtx
    end
    subgraph Cluster
    direction TB
    sup --> |update epoch|clus
    clus --> |get data| sup
    clus --> |store data|clusstore
    end
```
