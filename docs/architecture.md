# Architecture

## Modules

### Consensus
Manage real time generated data by the validator.

Implements these calls:
 * GetVoteAccounts
 * getLeaderSchedule
 * getEpochInfo
 * getSlot
 * getSignatureStatuses at process commitment.
 * new tx sent: call by the SendTx module when a Tx is processed by sendTx module.

Provide these subscription:
 * Full block
 * BLock info
 * Slots
 * Leader schedule
 * sent Tx at confirmed and / or finalized: notify when a Tx sent is confirmed or finalized. 

A new subscription is added: Sent Tx confirmed/ finalized. SendTx module send Tx signature to the consensus module and when a Tx sent is confirmed (or finalized), it is notified on this subscription.

It avoids to call getSignatureStatuses in a pull mode.

### SendTx
Manage the whole send Tx process. Represent the current Lite RPC process.

Implements the sendTx call.


### History
Manage history function like getBlocks.

A special use case is the getSignatureStatuses because on process its the Consensus module that provide tha data.

### Cluster
Manage cluster information.

Implement the call: getClusterNodes

Provide the subscription: cluster info.

### RPC
It's an entry point for all call and dispatch the call to the right function.

## Interaction diagram

```mermaid
flowchart TD
    SendTx("Send Tx
           [Domain]
        
          Send Tx to cluster")
    subgraph Hisotry
        History("History
                   [Domain]
                
                  Get Block and Tx")
        Faithfull["Faithfull Service"]
        Storage["2 epoch Storage"]
    end
    Cluster("Cluster Info
           [Domain]
        
          Cluster data")

    subgraph Validator Host

        Validator["Validator
                  Validator process
                + GRPC Geyser"]
        
        Consensus("Consensus
               [Domain]
              Manage realtime produced data
        by the validator")

    end

    subgraph RPC Entry point
        RPC["RPC API
            
              RPC Entry point"]
    end

    

    Validator-- "geyser FullBlock/Slots Sub" -->Consensus
    Validator-- "geyser Stakes and Votes account Sub" -->Consensus
    Validator== "geyser getBlockHeight" ==>RPC
    Validator-- "geyser Cluster info Sub" -->Cluster
    Consensus<== "GetVoteAccounts/getLeaderSchedule/getEpochInfo/getSlot" ==>RPC
    Consensus<== "At Process getSignaturesForAddress/getSignatureStatuses" ==>RPC
    Consensus-- "Block Info Sub" -->SendTx
    Consensus-- "Leader Schedule Sub" -->SendTx
    Consensus-- "Sent Tx confirmed Sub" -->SendTx
    Cluster-- "Cluster info Sub" -->SendTx
    Consensus-- "Full Block Sub" -->History
    RPC== "SendTx" ==> SendTx
    SendTx== "A new Tx to send" ==> Consensus
    History<== "At confirm getBlock(s)/getSignaturesForAddress/getSignatureStatuses" ==> RPC
    History<-. "getBlock(s)/getSignaturesForAddress" .-> Faithfull
    History<-. "Store Blocks + Txs" .-> Storage
    Cluster<== "getClusterNodes" ==> RPC


    classDef consensus fill:#1168bd,stroke:#0b4884,color:#ffffff
    classDef history fill:#666,stroke:#0b4884,color:#ffffff
    classDef sendtx fill:#08427b,stroke:#052e56,color:#ffffff
    classDef redgray fill:#62524F, color:#fff
    classDef greengray fill:#4F625B, color:#fff

    class SendTx sendtx
    class History redgray
    class Consensus consensus
    class Cluster greengray
```


## Message stream
Module organization can change depending on the deployment needed. For example several validator node can be started to add reliability. To ease this association between module and server installation, module communication will be done mostly via asynchronous message. This message propagation and routing will done by the Stream module.

Module register to it to get notifified and send new message to specific entry point.

The logic organization will be.

```mermaid
flowchart TD
    SendTx("Send Tx
           [Module]
        
          Send Tx to cluster")
    History("History
               [Module]
            
              Get Block and Tx")

    Cluster("Cluster Info
           [Module]
        
          Cluster data")
        
    Consensus("Consensus
           [Module]
          Manage realtime produced data
    by the validator")

    Stream("Stream
          Manage message routing
          between module.")

    Consensus-- "Send Full Block" -->Stream
    Consensus-- "Send Block Info" -->Stream
    Consensus-- "Send Slot" -->Stream
    Consensus-- "Send Leader Schedule" -->Stream
    Consensus-- "Send Epoch info" -->Stream
    Consensus-- "Send Sent Tx confirmed/finalized" -->Stream
    Consensus-- "Send geyser cluster info" -->Stream
    Cluster-- "Send Cluster Info" -->Stream
    SendTx-- "Send sendTx" -->Stream
   
    Stream-- "Cluster Info sub" -->SendTx
    Stream-- "Block Info sub" -->SendTx
    Stream-- "Leader Schedule sub" -->SendTx
    Stream-- "Sent Tx confirmed/finalized sub" -->SendTx
    Stream-- "Sent Tx sub" -->Consensus
    Stream-- "Full Block sub" -->History
    Stream-- "Epoch info sub" -->History
    Stream-- "geyser cluster info sub" -->Cluster

    classDef consensus fill:#1168bd,stroke:#0b4884,color:#ffffff
    classDef history fill:#666,stroke:#0b4884,color:#ffffff
    classDef sendtx fill:#08427b,stroke:#052e56,color:#ffffff
    classDef redgray fill:#62524F, color:#fff
    classDef greengray fill:#4F625B, color:#fff

    class SendTx sendtx
    class History redgray
    class Consensus consensus
    class Cluster greengray
```

## Bootstrap architecture
To be done

## Deployment example
```mermaid
flowchart TD
    SendTx1("Send Tx Host")
    SendTx2("Send Tx Host")
    subgraph Data_instance1
        History1("History + bootstrap")
        Cluster1("Cluster Info")
    end
    subgraph Data_instance2
        History2("History + bootstrap")
        Cluster2("Cluster Info")
    end
    subgraph Data_instance3
        History3("History + bootstrap")
        Cluster3("Cluster Info")
    end

    subgraph Validator1_Host
        Validator1["Validator"]
        Consensus1("Consensus + bootstrap")
    end
    subgraph Validator2_Host
        Validator2["Validator"]
        Consensus2("Consensus + bootstrap")
    end

    RPC["RPC entry point
        dispatch on started servers"]

    Stream("Stream")

    RPC== "Send sendTx" ==>SendTx1
    RPC== "Send sendTx" ==>SendTx2
    SendTx1-- "Send Tx send" -->Stream
    SendTx2-- "Send Tx send" -->Stream
    Stream-- "Block Info" -->SendTx1
    Stream-- "Block Info" -->SendTx2

    Consensus1-- "Send consensus data" -->Stream
    Consensus2-- "Send consensus data" -->Stream
    RPC== "Send sendTx" ==>Consensus1
    RPC== "Send sendTx" ==>Consensus2

    History1-- "Block sub" -->Stream
    History2-- "Block sub" -->Stream
    History3-- "Block sub" -->Stream
    RPC<== "getBlock" ==>History1
    RPC<== "getBlock" ==>History2
    RPC<== "getBlock" ==>History3

    Cluster1-- "Block sub" -->Stream
    Cluster2-- "Block sub" -->Stream
    Cluster3-- "Block sub" -->Stream
    RPC<== "getClusterNodes" ==>Cluster1
    RPC<== "getClusterNodes" ==>Cluster2
    RPC<== "getClusterNodes" ==>Cluster3

    classDef consensus fill:#1168bd,stroke:#0b4884,color:#ffffff
    classDef history fill:#666,stroke:#0b4884,color:#ffffff
    classDef sendtx fill:#08427b,stroke:#052e56,color:#ffffff
    classDef redgray fill:#62524F, color:#fff
    classDef greengray fill:#4F625B, color:#fff

    class SendTx1 sendtx
    class SendTx2 sendtx
    class Data_instance1 redgray
    class Data_instance2 redgray
    class Data_instance3 redgray
    class Validator1_Host consensus
    class Validator2_Host consensus
    class Cluster greengray
```

