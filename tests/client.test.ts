import { Connection, Keypair, sendAndConfirmTransaction, Transaction, PublicKey, TransactionInstruction, BlockheightBasedTransactionConfirmationStrategy, TransactionMessage, VersionedTransaction, VersionedMessage, MessageV0 } from "@solana/web3.js";
import * as fs from "fs";
import * as os from "os";
import * as crypto from "crypto";

jest.setTimeout(60000);

const MEMO_PROGRAM_ID = new PublicKey("MemoSq4gqABAXKb96qnH8TysNcWxMyWCqXgDLGmfcHr");
const connection = new Connection('http://0.0.0.0:8890', 'confirmed');
const keypair_file = fs.readFileSync(`${os.homedir}/.config/solana/id.json`, 'utf-8');
const payer = Keypair.fromSecretKey(Uint8Array.from(JSON.parse(keypair_file)));

function createTransaction(): Transaction {
    const transaction = new Transaction();

    transaction.add(
        new TransactionInstruction({
            programId: MEMO_PROGRAM_ID,
            keys: [],
            data: Buffer.from(crypto.randomBytes(20).toString('hex'))
        })
    );

    return transaction;
}

function createVersionedMessage(blockhash: string, payer: PublicKey): MessageV0 {
    return new MessageV0({
        header: {
            numRequiredSignatures: 1,
            numReadonlySignedAccounts: 0,
            numReadonlyUnsignedAccounts: 0,
        },
        staticAccountKeys: [payer, MEMO_PROGRAM_ID],
        recentBlockhash: blockhash,
        compiledInstructions: [{
            programIdIndex: 1,
            accountKeyIndexes: [],
            data: Buffer.from(crypto.randomBytes(20).toString('hex')),
          }],
        addressTableLookups: [],
      })
}

test('send and confirm transaction BlockheightBasedTransactionConfirmationStrategy', async () => {
    const tx = createTransaction();
    const { blockhash, lastValidBlockHeight } = await connection.getLatestBlockhash();
    const signature = await connection.sendTransaction(tx, [payer], {});
    console.log(`https://explorer.solana.com/tx/${signature}`);
    await connection.confirmTransaction({
        blockhash,
        lastValidBlockHeight,
        signature,
        abortSignal: undefined
    });
});

test('send and confirm transaction legacy confrim', async () => {
    const tx = createTransaction();
    const { blockhash, lastValidBlockHeight } = await connection.getLatestBlockhash();
    const signature = await connection.sendTransaction(tx, [payer]);
    console.log(`https://explorer.solana.com/tx/${signature}`);
    await connection.confirmTransaction(signature);
});

test('send and confirm versioned transaction', async () => {
    const { blockhash, lastValidBlockHeight } = await connection.getLatestBlockhash();
    const message = createVersionedMessage(blockhash, payer.publicKey);
    let versionedTransaction = new VersionedTransaction( message, [payer.secretKey])
    versionedTransaction.sign([payer])
    const signature = await connection.sendRawTransaction(versionedTransaction.serialize(), {});
    console.log(`https://explorer.solana.com/tx/${signature}`);
    await connection.confirmTransaction({
        blockhash,
        lastValidBlockHeight,
        signature,
        abortSignal: undefined
    });
});

test('send and confirm transaction', async () => {
    const tx = createTransaction();

    await sendAndConfirmTransaction(connection, tx, [payer]);
});


// test('get epoch info', async () => {
//     {
//         const {epoch, absoluteSlot, slotIndex, slotsInEpoch} = await connection.getEpochInfo();
//         expect(Math.floor(absoluteSlot/slotsInEpoch)).toBe(epoch);        
//     }

//     let process_absoluteSlot;
//     {
//         const {epoch, absoluteSlot, slotIndex, slotsInEpoch} = await connection.getEpochInfo({ commitment: 'processed' });
//         expect(Math.floor(absoluteSlot/slotsInEpoch)).toBe(epoch);
//         process_absoluteSlot = absoluteSlot;  
//     }

//     let confirmed_absoluteSlot;
//     {
//         const {epoch, absoluteSlot, slotIndex, slotsInEpoch} = await connection.getEpochInfo({ commitment: 'confirmed' });
//         expect(Math.floor(absoluteSlot/slotsInEpoch)).toBe(epoch);      
//         confirmed_absoluteSlot = absoluteSlot;  
//     }
//     expect(confirmed_absoluteSlot >= process_absoluteSlot);

//     let finalized_absoluteSlot;
//     {
//         const {epoch, absoluteSlot, slotIndex, slotsInEpoch} = await connection.getEpochInfo({ commitment: 'finalized' });
//         expect(Math.floor(absoluteSlot/slotsInEpoch)).toBe(epoch);        
//         finalized_absoluteSlot = absoluteSlot;  
//     }
//     expect(process_absoluteSlot > finalized_absoluteSlot);
//     expect(confirmed_absoluteSlot > finalized_absoluteSlot);

// });


// test('get leader schedule', async () => {
//     {
//         const leaderSchedule = await connection.getLeaderSchedule();
//         expect(Object.keys(leaderSchedule).length > 0);        
//     }
// });



