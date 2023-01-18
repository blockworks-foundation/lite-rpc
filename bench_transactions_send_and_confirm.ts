import { Connection, Keypair, LAMPORTS_PER_SOL, PublicKey, TransactionSignature } from '@solana/web3.js';
import * as fs from 'fs';
import * as splToken from "@solana/spl-token";
import * as os from 'os';

// number of users
const tps : number = +process.argv[2];
const forSeconds : number = +process.argv[3];
// url
const url = process.argv.length > 4 ? process.argv[4] : "http://localhost:8899";
import * as InFile from "./out.json";

function sleep(ms: number) {
    return new Promise( resolve => setTimeout(resolve, ms) );
}
 
console.log("benching " + tps + " transactions per second on " + url + " for " + forSeconds + " seconds");

export async function main() {

    const connection = new Connection(url, 'confirmed');
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

    let successes : Uint32Array = new Uint32Array(forSeconds).fill(0);
    let failures : Uint32Array = new Uint32Array(forSeconds).fill(0);
    let promises : Promise<void>[] = [];

    for (let i = 0; i<forSeconds; ++i)
    {
        const start = performance.now();
        let signatures : TransactionSignature[] = []; 
        for (let j=0; j<tps; ++j)
        {
            const toIndex = Math.floor(Math.random() * users.length);
            let fromIndex = toIndex;
            while (fromIndex === toIndex)
            {
                fromIndex = Math.floor(Math.random() * users.length);
            }
            const userFrom = userAccounts[fromIndex];
            const userTo = userAccounts[toIndex];
            const p = splToken.transfer(
                connection,
                authority,
                userFrom,
                userTo,
                users[fromIndex],
                100,
            ).then((_)=> {successes[i]++}, (_) => {failures[i]++})
            promises.push(p)
        }

        const end = performance.now();
        const diff = (end - start);
        if (diff > 0) {
            await sleep(1000 - diff)
        }
    }

    for (const p of promises)
    {
        await p;
    }
    console.log("successes : " + successes);
    console.log("failures : " + failures);
}

main().then(x => {
    console.log('finished sucessfully')
}).catch(e => {
    console.log('caught an error : ' + e)
})