import { Keypair, Message, VersionedTransaction } from "@solana/web3.js";
import { LiteClient } from "../lite-client-web3.js";

//jest.setTimeout(60000);

test('send and confirm transaction', async () => {
    const connection = new LiteClient("http://127.0.0.1:8899", 'confirmed');

    const payer = Keypair.generate();

//    const airdrop_sig = await connection.requestAirdrop(payer.publicKey, LAMPORTS_PER_SOL * 20);

    const recentBlockhash = (await connection.getLatestBlockhash()).blockhash;

    const versionedTx = new VersionedTransaction(
        new Message({
            header: {
                numRequiredSignatures: 1,
                numReadonlySignedAccounts: 0,
                numReadonlyUnsignedAccounts: 0,
            },
            recentBlockhash,
            instructions: [],
            accountKeys: [payer.publicKey.toBase58()],
        }),
    );

    versionedTx.sign([payer]);
    const signature = await connection.sendTransaction(versionedTx);
//    const latestBlockHash = await connection.getLatestBlockhash();

    await connection.confirmTransaction(signature);
});
