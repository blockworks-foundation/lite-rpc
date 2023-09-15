# Architecture

## Modules

### Validator
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
#### Sub domain Cluster
Manage cluster information.

Implement the call: getClusterNodes

Provide the subscription: cluster info.

### SendTx
Manage the whole send Tx process. Represent the current Lite RPC process.

Implements the sendTx call.


### History
Manage history function like getBlocks.

A special use case is the getSignatureStatuses because on process its the Consensus module that provide tha data.

### RPC
It's an entry point for all call and dispatch the call to the right function.

## Summary diagram

```mermaid
flowchart TD
    subgraph Send Tx Domain
        SendTx("SendTx API

              send_transaction()")
    end

    subgraph History Domain
        History("History API

        at confirm/finalized
            getBlock()
            getBlocks()
            getSignaturesForAddress()
            getSignatureStatuses()
            getTransaction()")
        Faithfull["Faithfull Service"]
        Storage["2 epoch Storage"]
    end

    subgraph Validator Host
        Validator["Solana Validator
            Validator process
            + GRPC Geyser"]
        
        Consensus("Validator API

            getVoteAccounts()
            getLeaderSchedule()
            getEpochInfo()
            getSlot()...
        At process:
            getSignaturesForAddress()
            getSignatureStatuses()

              ")
        Cluster("Cluster API
            
           getClusterNodes()")
    end
    

    Validator-- "geyser data" -->Consensus
    Validator-- "Cluster info" -->Cluster
    Consensus-- "Block Info/Leader Schedule" -->SendTx
    Consensus-- "confirmed Tx" -->SendTx
    Cluster-- "Cluster info" -->SendTx
    Consensus-- "Full Block / Epoch" -->History
    History<-. "old data" .-> Faithfull
    History<-. "recent data" .-> Storage

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


## Interaction diagram

```mermaid
flowchart TD
    SendTx("Send Tx
           [Domain]
        
          Send Tx to cluster")
    subgraph History 
        History("History
                   [Domain]
                
                  Get Block and Tx")
        Faithfull["Faithfull Service"]
        Storage["2 epoch Storage"]
    end

    subgraph Validator Host

        Validator["Solana Validator
                  Validator process
                + GRPC Geyser"]
        
        Consensus("Validator
               [Domain]
              Manage realtime produced data
        by the validator")

        Cluster("Cluster Info
               [SubDomain]
        
              Cluster data")

    end

    subgraph RPC Entry point
        RPC["RPC API
            
              RPC Entry point"]
    end

    

    Validator-- "geyser FullBlock/Slots Sub" -->Consensus
    Validator-- "geyser Stakes and Votes account Sub" -->Consensus
    Validator== "geyser getBlockHeight" ==>RPC
    Validator-- "geyser Cluster info Sub" -->Cluster
    Consensus<== "getVoteAccounts/getLeaderSchedule/getEpochInfo/getSlot" ==>RPC
    Consensus<== "At Process getSignaturesForAddress/getSignatureStatuses" ==>RPC
    Consensus-- "Block Info Sub" -->SendTx
    Consensus-- "Leader Schedule Sub" -->SendTx
    Consensus-- "Sent Tx confirmed Sub" -->SendTx
    Cluster-- "Cluster info Sub" -->SendTx
    Consensus-- "Full Block / Epoch Sub" -->History
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
        
    Consensus("Validator
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
Each domain implements its own bootsrap. A domain impl running can send boostrap data to a starting one.

## Deployment example
```mermaid
flowchart TD
    SendTx1("Send Tx Host1")
    SendTx2("Send Tx Host2")
    subgraph Data_instance1
        History1("History1")
    end
    subgraph Data_instance2
        History2("History2")
    end
    subgraph Data_instance3
        History3("History3")
    end

    subgraph Validator1_Host
        Validator1["Solana Validator"]
        Consensus1("Validator")
        Cluster1("Cluster Info")
    end
    subgraph Validator2_Host
        Validator2["Solana Validator"]
        Consensus2("Validator")
        Cluster2("Cluster Info")
    end

    RPC["RPC entry point
        dispatch on started servers"]

    Stream("Stream")

    RPC== "Send sendTx" ==>SendTx1
    RPC== "Send sendTx" ==>SendTx2
    SendTx1-- "Send Tx sent" -->Stream
    SendTx2-- "Send Tx sent" -->Stream
    Stream-- "Block Info" -->SendTx1
    Stream-- "Block Info" -->SendTx2
    Stream-- "Cluster Schedule" -->SendTx1
    Stream-- "Cluster Schedule" -->SendTx2

    Consensus1-- "Send consensus data" -->Stream
    Consensus2-- "Send consensus data" -->Stream
    RPC<== "getVoteAccounts" ==>Consensus1
    RPC<== "getVoteAccounts" ==>Consensus2

    Stream-- "Block sub" -->History1
    Stream-- "Block sub" -->History2
    Stream-- "Block sub" -->History3
    RPC<== "getBlock" ==>History1
    RPC<== "getBlock" ==>History2
    RPC<== "getBlock" ==>History3

    Cluster1-- "Send Cluster info" -->Stream
    Cluster2-- "Send Cluster info" -->Stream
    RPC<== "getClusterNodes" ==>Cluster1
    RPC<== "getClusterNodes" ==>Cluster2

    Validator1-- "Geyser data" -->Consensus1
    Validator1-- "Geyser data" -->Cluster1
    Validator2-- "Geyser data" -->Consensus2
    Validator2-- "Geyser data" -->Cluster2

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

