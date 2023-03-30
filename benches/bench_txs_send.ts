import { Connection, Keypair, LAMPORTS_PER_SOL, PublicKey, TransactionSignature, Transaction } from '@solana/web3.js';
import * as fs from 'fs';
import * as splToken from "@solana/spl-token";
import { deserialize_keypair, get_postional_arg, OutputFile, sleep } from './util';

// number of users
const tps: number = +get_postional_arg(2, 10);
const forSeconds: number = +get_postional_arg(3, 10);
// url
const url = get_postional_arg(4, "http://0.0.0.0:8899");
//@ts-ignore
const skip_confirmations = get_postional_arg(5, false) === "true";

(async function main() {
    const InFile: OutputFile = JSON.parse(fs.readFileSync('out.json').toString());

    console.log("benching " + tps + " transactions per second on " + url + " for " + forSeconds + " seconds");

    const connection = new Connection(url, 'finalized');

    const blockhash = await connection.getLatestBlockhash();
    console.log('blockhash : ' + blockhash.blockhash);

    const payers = InFile.fee_payers.map(deserialize_keypair);
    const users = InFile.users.map(deserialize_keypair);
    const userAccounts = InFile.tokenAccounts.map(x => new PublicKey(x));

    let signatures_to_unpack: TransactionSignature[][] = new Array<TransactionSignature[]>(forSeconds);
    let time_taken_to_send = [];

    let payer_index = 0;

    for (let i = 0; i < forSeconds; ++i) {
        console.log('Sending transaction ' + i);
        const start = performance.now();
        signatures_to_unpack[i] = new Array<TransactionSignature>(tps);
        let blockhash = (await connection.getLatestBlockhash()).blockhash;
        for (let j = 0; j < tps; ++j) {
            if (j % 100 == 0) {
                blockhash = (await connection.getLatestBlockhash()).blockhash;
            }
            const toIndex = Math.floor(Math.random() * users.length);
            let fromIndex = toIndex;
            while (fromIndex === toIndex) {
                fromIndex = Math.floor(Math.random() * users.length);
            }
            const userFrom = userAccounts[fromIndex];
            const userTo = userAccounts[toIndex];

            const transaction = new Transaction().add(
                splToken.createTransferInstruction(userFrom, userTo, users[fromIndex].publicKey, Math.ceil((Math.random() + 1) * 100))
            );
            transaction.recentBlockhash = blockhash;
            transaction.feePayer = payers[payer_index].publicKey;

            connection
                .sendTransaction(transaction, [payers[payer_index], users[fromIndex]], { skipPreflight: true })
                .then(p => { signatures_to_unpack[i][j] = p });

            payer_index++;
            if (payer_index == payers.length) {
                payer_index = 0;
            }
        }
        const end = performance.now();
        const diff = (end - start);
        time_taken_to_send[i] = diff;
        if (diff > 0 && diff < 1000) {
            await sleep(1000 - diff)
        }
    }

    console.log('finish sending transactions');
    await sleep(10000)
    console.log('checking for confirmations');
    if (skip_confirmations === false) {
        const size = signatures_to_unpack.length
        let successes: Uint32Array = new Uint32Array(size).fill(0);
        let failures: Uint32Array = new Uint32Array(size).fill(0);
        for (let i = 0; i < size; ++i) {
            const signatures = signatures_to_unpack[i];
            for (const signature of signatures) {
                const confirmed = await connection.getSignatureStatus(signature);
                if (confirmed != null && confirmed.value != null && confirmed.value.err == null) {
                    successes[i]++;
                } else {
                    failures[i]++;
                }
            }

        }
        console.log("sucesses : " + successes)
        console.log("failures : " + failures)
        //console.log("time taken to send : " + time_taken_to_send)
    }
})()
