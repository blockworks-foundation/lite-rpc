import { Keypair, PublicKey } from "@solana/web3.js";

export type KeypairSerialized = {
    publicKey: string;
    secretKey: number[]
}

export type OutputFile = {
    fee_payers: KeypairSerialized[];
    users: KeypairSerialized[];
    tokenAccounts: PublicKey[];
    mint: PublicKey;
    minted_amount: number;
}

export function serialized_keypair(keypair: Keypair): {
} {
    return {
        'publicKey': keypair.publicKey.toBase58(),
        'secretKey': Array.from(keypair.secretKey)
    }
}

export function deserialize_keypair(keypair: KeypairSerialized): Keypair {
    return Keypair.fromSecretKey(Uint8Array.from(keypair.secretKey))
}


export function sleep(ms: number) {
    return new Promise(resolve => setTimeout(resolve, ms));
}

export function get_postional_arg<T>(index: number, default_value: T): T {
    return process.argv.length > index ? process.argv[index] as T : default_value;
}
