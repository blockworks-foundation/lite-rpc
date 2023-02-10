import { Connection, Keypair, LAMPORTS_PER_SOL, PublicKey, TransactionSignature, Transaction } from '@solana/web3.js';
import * as fs from 'fs';
import * as splToken from "@solana/spl-token";
import * as os from 'os';

// number of users
const tps: number = +process.argv[2];
const forSeconds: number = +process.argv[3];
// url
const url = process.argv.length > 4 ? process.argv[4] : "http://localhost:8899";
const skip_confirmations = process.argv.length > 5 ? process.argv[5] === "true" : false;
import * as InFile from "./out.json";

function sleep(ms: number) {
    return new Promise(resolve => setTimeout(resolve, ms));
}

console.log("benching " + tps + " transactions per second on " + url + " for " + forSeconds + " seconds");

function delay(ms: number) {
    return new Promise(resolve => setTimeout(resolve, ms));
}

export async function main() {

    const connection = new Connection(url, 'finalized');
    console.log('get latest blockhash')
    const blockhash = await connection.getLatestBlockhash({
        commitment: 'finalized'
    });
    console.log('blockhash : ' + blockhash.blockhash);
    const authority = Keypair.fromSecretKey(
        Uint8Array.from(
            JSON.parse(
                process.env.KEYPAIR ||
                fs.readFileSync(os.homedir() + '/.config/solana/id.json', 'utf-8'),
            ),
        ),
    );

    const users = InFile.users.map(x => Keypair.fromSecretKey(Uint8Array.from(x.secretKey)));
    const userAccounts = InFile.tokenAccounts.map(x => new PublicKey(x));
    let signatures_to_unpack: TransactionSignature[][] = new Array<TransactionSignature[]>(forSeconds);
    let time_taken_to_send = [];

    for (let i = 0; i < forSeconds; ++i) {
        console.log('Sending transaction ' + i);
        const start = performance.now();
        signatures_to_unpack[i] = new Array<TransactionSignature>(tps);
        let blockhash = (await connection.getLatestBlockhash()).blockhash;
        for (let j = 0; j < tps; ++j) {
            if (j%100 == 0) {
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
                splToken.createTransferInstruction(userFrom, userTo, users[fromIndex].publicKey, Math.ceil((Math.random()+1) * 100))
            );
            transaction.recentBlockhash = blockhash;
            transaction.feePayer = authority.publicKey;

            connection.sendTransaction(transaction, [authority, users[fromIndex]], { skipPreflight: true }).then(p => {signatures_to_unpack[i][j] = p});
        }
        const end = performance.now();
        const diff = (end - start);
        time_taken_to_send[i] = diff;
        if (diff > 0 && diff < 1000) {
            await sleep(1000 - diff)
        }
    }

    console.log('finish sending transactions');
    await delay(10000)
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
}

main().then(x => {
    console.log('finished sucessfully')
}).catch(e => {
    console.log('caught an error : ' + e)
})
