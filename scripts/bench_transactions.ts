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
import { web3 } from '@project-serum/anchor';
import { sleep } from '@blockworks-foundation/mango-client';
 
console.log("benching " + tps + " transactions per second on " + url + " for " + forSeconds + " seconds");

export async function main() {
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

    for (let i = 0; i<forSeconds; ++i)
    {
        const start = performance.now();
        let promises : Promise<TransactionSignature>[] = []; 
        for (let j=0; j<tps; ++j)
        {
            const connection = new Connection(url, 'confirmed');
            const toIndex = Math.floor(Math.random() * users.length);
            let fromIndex = toIndex;
            while (fromIndex === toIndex)
            {
                fromIndex = Math.floor(Math.random() * users.length);
            }
            const userFrom = userAccounts[fromIndex];
            const userTo = userAccounts[toIndex];
            let sig = await splToken.transfer(
                connection,
                authority,
                userFrom,
                userTo,
                users[fromIndex],
                100,
            );
            let blockhash = await connection.getLatestBlockhash();
            let res = await connection.confirmTransaction({
                signature: sig,
                blockhash: blockhash.blockhash,
                lastValidBlockHeight: blockhash.lastValidBlockHeight,
            })
            let value = "";
            if (res.value.err == null) {
                value = "Ok"
            } else {
                value = res.value.err.toString()
            }
            console.log("result for " + i + "th second and " + j + "th transaction " + value);
        }
        const end = performance.now();
        const diff = (end - start);
        if (diff > 0) {
            await sleep(1000 - diff)
        }
    }
}

main().then(x => {
    console.log('finished sucessfully')
}).catch(e => {
    console.log('caught an error : ' + e)
})