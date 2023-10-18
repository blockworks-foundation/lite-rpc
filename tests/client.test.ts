import { Connection, Keypair, sendAndConfirmTransaction, Transaction, PublicKey, TransactionInstruction, BlockheightBasedTransactionConfirmationStrategy } from "@solana/web3.js";
import * as fs from "fs";
import * as os from "os";
import * as crypto from "crypto";

jest.setTimeout(60000);

const MEMO_PROGRAM_ID = new PublicKey("MemoSq4gqABAXKb96qnH8TysNcWxMyWCqXgDLGmfcHr");
const connection = new Connection('http://0.0.0.0:8899', 'confirmed');
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

test('send and confirm transaction BlockheightBasedTransactionConfirmationStrategy', async () => {
    const tx = createTransaction();
    const { blockhash, lastValidBlockHeight } = await connection.getLatestBlockhash();

    const signature = await connection.sendTransaction(tx, [payer]);
    await connection.confirmTransaction({
        blockhash,
        lastValidBlockHeight,
        signature,
        abortSignal: undefined
    });

    console.log(`https://explorer.solana.com/tx/${signature}`)
});

test('send and confirm transaction', async () => {
    const tx = createTransaction();

    await sendAndConfirmTransaction(connection, tx, [payer]);
});


test('get epoch info', async () => {
    {
        const {epoch, absoluteSlot, slotIndex, slotsInEpoch} = await connection.getEpochInfo();
        expect(Math.floor(absoluteSlot/slotsInEpoch)).toBe(epoch);        
    }

    let process_absoluteSlot;
    {
        const {epoch, absoluteSlot, slotIndex, slotsInEpoch} = await connection.getEpochInfo({ commitment: 'processed' });
        expect(Math.floor(absoluteSlot/slotsInEpoch)).toBe(epoch);
        process_absoluteSlot = absoluteSlot;  
    }

    let confirmed_absoluteSlot;
    {
        const {epoch, absoluteSlot, slotIndex, slotsInEpoch} = await connection.getEpochInfo({ commitment: 'confirmed' });
        expect(Math.floor(absoluteSlot/slotsInEpoch)).toBe(epoch);      
        confirmed_absoluteSlot = absoluteSlot;  
    }
    expect(confirmed_absoluteSlot >= process_absoluteSlot);

    let finalized_absoluteSlot;
    {
        const {epoch, absoluteSlot, slotIndex, slotsInEpoch} = await connection.getEpochInfo({ commitment: 'finalized' });
        expect(Math.floor(absoluteSlot/slotsInEpoch)).toBe(epoch);        
        finalized_absoluteSlot = absoluteSlot;  
    }
    expect(process_absoluteSlot > finalized_absoluteSlot);
    expect(confirmed_absoluteSlot > finalized_absoluteSlot);

});


test('get leader schedule', async () => {
    {
        const leaderSchedule = await connection.getLeaderSchedule();
        expect(Object.keys(leaderSchedule).length > 0);        
    }
});



