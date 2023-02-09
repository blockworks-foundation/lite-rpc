import { Connection, Keypair, LAMPORTS_PER_SOL, SystemProgram, sendAndConfirmTransaction, Transaction, PublicKey, TransactionInstruction, Signer } from "@solana/web3.js";
import * as fs from "fs";
import * as os from "os";

jest.setTimeout(60000);

const MEMO_PROGRAM_ID = new PublicKey("MemoSq4gqABAXKb96qnH8TysNcWxMyWCqXgDLGmfcHr");

test('send and confirm transaction', async () => {
    const connection = new Connection('http://0.0.0.0:8890', 'confirmed');

    const keypair_file = fs.readFileSync(`${os.homedir}/.config/solana/id.json`, 'utf-8');
    const keypair_array = Uint8Array.from(JSON.parse(keypair_file));
    const payer = Keypair.fromSecretKey(keypair_array);

    const transaction = new Transaction();

    transaction.add(
        new TransactionInstruction({
            programId: MEMO_PROGRAM_ID,
            keys: [],
            data: Buffer.from("Hello")
        })
    );

    const sig = connection.sendTransaction(transaction, [payer]);

    console.log(`https://explorer.solana.com/tx/${sig}`)

    await sendAndConfirmTransaction(connection, transaction, [payer]);
});
