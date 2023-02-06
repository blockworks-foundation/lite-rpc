import { Connection, Keypair, LAMPORTS_PER_SOL, PublicKey } from '@solana/web3.js';
import * as fs from 'fs';
import * as splToken from "@solana/spl-token";
import * as os from 'os';

// number of users
const nbUsers = +process.argv[2];
// url
const url = process.argv.length > 3 ? process.argv[3] : "http://0.0.0.0:8899";
// outfile
const outFile = process.argv.length > 4 ? process.argv[4] : "out.json";
 
console.log("creating " + nbUsers + " Users on " + url + " out file " + outFile);
function delay(ms: number) {
    return new Promise( resolve => setTimeout(resolve, ms) );
}
export async function main() {
    const connection = new Connection(url, 'confirmed');
    let authority = Keypair.fromSecretKey(
        Uint8Array.from(
          JSON.parse(
            process.env.KEYPAIR ||
                fs.readFileSync(os.homedir() + '/.config/solana/id.json', 'utf-8'),
          ),
        ),
      );

    let userKps = [...Array(nbUsers)].map(_x => Keypair.generate())
    let mint = await splToken.createMint(
        connection,
        authority,
        authority.publicKey,
        null,
        6,
    );
    let accounts : PublicKey[] = [];
    for (const user of userKps) {
        console.log("account created");
        let account = await splToken.createAccount(
            connection,
            authority,
            mint,
            user.publicKey,
        )
        accounts.push(account)
        await delay(100)
    };

    for (const account of accounts) {
        console.log("account minted");
        await splToken.mintTo(
            connection,
            authority,
            mint,
            account,
            authority,
            1_000_000_000_000,
        )
        await delay(100)
    };

    const users = userKps.map(x => {
        const info = {
            'publicKey' : x.publicKey.toBase58(),
            'secretKey' : Array.from(x.secretKey)
        };
        return info;
    });

    const data = {
        'users' : users,
        'tokenAccounts' : accounts,
        'mint' : mint,
        'minted_amount' : 1_000_000_000_000
    };

    console.log('created ' + nbUsers + ' Users and minted 10^12 tokens for mint ' + mint);
    fs.writeFileSync(outFile, JSON.stringify(data));
}

main().then(x => {
    console.log('finished sucessfully')
}).catch(e => {
    console.log('caught an error : ' + e)
})