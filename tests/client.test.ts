import { Connection, Keypair, LAMPORTS_PER_SOL, SystemProgram, sendAndConfirmTransaction, Transaction, PublicKey } from "@solana/web3.js";

jest.setTimeout(60000);

test('send and confirm transaction', async () => {
    const connection = new Connection('http://127.0.0.1:8890', 'confirmed');
    const payer = Keypair.generate();
    const toAccount = Keypair.generate().publicKey;

    const airdropSignature = await connection.requestAirdrop(payer.publicKey, LAMPORTS_PER_SOL * 2);
    console.log('airdrop signature  ' + airdropSignature);
    await connection.confirmTransaction(airdropSignature, 'finalized');
    console.log('confirmed');

    const transaction = new Transaction();

    // Add an instruction to execute
    transaction.add(
        SystemProgram.transfer({
            fromPubkey: payer.publicKey,
            toPubkey: toAccount,
            lamports: LAMPORTS_PER_SOL,
        }),
    );

    await sendAndConfirmTransaction(connection, transaction, [payer]);
});
