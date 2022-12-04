import { Connection, Keypair, LAMPORTS_PER_SOL, Message, VersionedTransaction } from "@solana/web3.js";
import { url } from "./urls";

test('send and confirm transaction', async () => {
  const connection = new Connection(url, 'confirmed');
  const payer = Keypair.generate();

  await connection.requestAirdrop(payer.publicKey, LAMPORTS_PER_SOL);
  //const recentBlockhash = (await connection.getLatestBlockhash('confirmed')).blockhash;
  //const versionedTx = new VersionedTransaction(
  //  new Message({
  //    header: {
  //      numRequiredSignatures: 1,
  //      numReadonlySignedAccounts: 0,
  //      numReadonlyUnsignedAccounts: 0,
  //    },
  //    recentBlockhash,
  //    instructions: [],
  //    accountKeys: [payer.publicKey.toBase58()],
  //  }),
  //);
//
  //versionedTx.sign([payer]);
  //const signature = await connection.sendTransaction(versionedTx);
  //const latestBlockHash = await connection.getLatestBlockhash();
  //await connection.confirmTransaction({
  //  blockhash: latestBlockHash.blockhash,
  //  lastValidBlockHeight: latestBlockHash.lastValidBlockHeight,
  //  signature: signature,
  //});
});