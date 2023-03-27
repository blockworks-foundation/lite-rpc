import { Connection, Keypair } from '@solana/web3.js';
import * as splToken from "@solana/spl-token";
import * as fs from 'fs';
import * as os from 'os';

// number of users
const nbUsers = +process.argv[2];
// url
const url = process.argv.length > 3 ? process.argv[3] : "http://0.0.0.0:8899";
// outfile
const outFile = process.argv.length > 4 ? process.argv[4] : "out.json";

(async function main() {
    console.log("Creating " + nbUsers + " Users on " + url + " out file " + outFile);
    console.time('Time taken');

    const connection = new Connection(url, 'confirmed');

    const authority = Keypair.fromSecretKey(
        Uint8Array.from(
            JSON.parse(
                process.env.KEYPAIR ||
                fs.readFileSync(os.homedir() + '/.config/solana/id.json', 'utf-8'),
            ),
        ),
    );

    const userKps = Array(nbUsers).fill(0).map(() => Keypair.generate());

    const mint = await splToken.createMint(
        connection,
        authority,
        authority.publicKey,
        null,
        6,
    );

    const accounts = await Promise.all(userKps.map(async user => {
        const account = await splToken.createAccount(
            connection,
            authority,
            mint,
            user.publicKey,
        );

        console.log("Account created");

        await splToken.mintTo(
            connection,
            authority,
            mint,
            account,
            authority,
            1_000_000_000_000,
        )

        console.log("Account minted");

        return account;
    }));

    console.timeLog('Time taken');

    const users = userKps.map(x => ({
        'publicKey': x.publicKey.toBase58(),
        'secretKey': Array.from(x.secretKey)
    }));

    const data = {
        'users': users,
        'tokenAccounts': accounts,
        'mint': mint,
        'minted_amount': 1_000_000_000_000
    };

    console.log('created ' + nbUsers + ' Users and minted 10^12 tokens for mint ' + mint);

    fs.writeFileSync(outFile, JSON.stringify(data));
})()
