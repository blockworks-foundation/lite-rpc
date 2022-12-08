import { Connection, Keypair, LAMPORTS_PER_SOL, PublicKey, TransactionSignature } from '@solana/web3.js';
import * as fs from 'fs';
import * as splToken from "@solana/spl-token";
import * as os from 'os';

// number of users
const tps : number = +process.argv[2];
const forSeconds : number = +process.argv[3];
// url
const url = process.argv.length > 4 ? process.argv[4] : "http://localhost:8899";
const skip_confirmations = process.argv.length > 5 ? process.argv[5] === "true": false;
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
    let promises_to_unpack : Promise<TransactionSignature>[][] = [];

    for (let i = 0; i<forSeconds; ++i)
    {
        const start = performance.now();
        let promises : Promise<TransactionSignature>[] = []; 
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
            if(skip_confirmations === false) {
                promises.push(
                    splToken.transfer(
                        connection,
                        authority,
                        userFrom,
                        userTo,
                        users[fromIndex],
                        100,
                    )
                )
            }
        }
        if (skip_confirmations === false) 
        {
            promises_to_unpack.push(promises)
        }
        const end = performance.now();
        const diff = (end - start);
        if (diff > 0) {
            await sleep(1000 - diff)
        }
    }

    console.log('checking for confirmations');
    if(skip_confirmations === false) {
        const size = promises_to_unpack.length
        let successes : Uint32Array = new Uint32Array(size).fill(0);
        let failures : Uint32Array = new Uint32Array(size).fill(0);
        for (let i=0; i< size; ++i)
        {
            const promises = promises_to_unpack[i];

            await Promise.all( promises.map( promise => {
                promise.then((_fullfil)=>{
                    Atomics.add(successes, i, 1);
                },
                (_reject)=>{
                    Atomics.add(failures, i, 1);
                })
            }))
        }
        console.log("sucesses " +  successes)
        console.log("failures " + failures)
    }
}

main().then(x => {
    console.log('finished sucessfully')
}).catch(e => {
    console.log('caught an error : ' + e)
})