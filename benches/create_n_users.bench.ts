import { Connection, Keypair, LAMPORTS_PER_SOL, sendAndConfirmTransaction, SystemProgram, Transaction } from '@solana/web3.js';
import * as splToken from "@solana/spl-token";
import * as fs from 'fs';
import * as os from 'os';
import { get_postional_arg, serialized_keypair } from './util';

// number of users
const nbUsers = +get_postional_arg(2, 10);
// url
const url = get_postional_arg(3, "http://0.0.0.0:8899");
// tokens to transfer to new  accounts 0.5 sol
const fee_payer_balance = +get_postional_arg(4, (LAMPORTS_PER_SOL / 2));
// tokens to transfer to new  accounts 0.5 sol
const number_of_fee_payers = +get_postional_arg(5, 10);
// outfile
const outFile = get_postional_arg(6, "out.json");

function check_if_out_file_exists() {
    if (!fs.existsSync(outFile))
        return;

    console.warn(`{outFile} already exists. Potential loss of funds. Remove and re run`);
    process.exit();
}

(async function main() {
    check_if_out_file_exists();

    console.log(`Creating ${nbUsers} users on ${url} with ${number_of_fee_payers} with balance ${fee_payer_balance} and output file ${outFile}`);
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

    const authority_balance = await connection.getBalance(authority.publicKey);
    const required_balance = number_of_fee_payers * fee_payer_balance;

    if (authority_balance < required_balance) {
        console.warn(`Authority doesn't have enough balance. Required at least ${required_balance} Lamport`);
        process.exit();
    }

    const fee_payers = Array(nbUsers).fill(0).map(() => Keypair.generate());

    console.log(`Sending ${fee_payer_balance} to each of ${number_of_fee_payers} fee payers`);

    await Promise.all(fee_payers.map(async fee_payer => {
        const ix = SystemProgram.transfer({
            fromPubkey: authority.publicKey,
            toPubkey: fee_payer.publicKey,
            lamports: fee_payer_balance
        });

        const tx = new Transaction().add(ix);

        const tx_sig = await sendAndConfirmTransaction(connection, tx, [authority]);

        console.log(`Sent ${tx_sig}`);
    }));

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

    const users = userKps.map(serialized_keypair);
    const fee_payers_serialized = fee_payers.map(serialized_keypair);

    const data = {
        'fee_payers': fee_payers_serialized,
        'users': users,
        'tokenAccounts': accounts,
        'mint': mint,
        'minted_amount': 1_000_000_000_000
    };

    console.log('created ' + nbUsers + ' Users and minted 10^12 tokens for mint ' + mint);

    fs.writeFileSync(outFile, JSON.stringify(data));
})()
