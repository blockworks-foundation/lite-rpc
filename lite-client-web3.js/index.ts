import { Connection, TransactionSignature } from "@solana/web3.js";

export class LiteClient extends Connection {
    async confirmTransaction(signature: TransactionSignature) {
        const unsafeRes = await this._rpcRequest('confirmTransaction', [signature]);

        const res = create(unsafeRes, SendTransactionRpcResult);

        if ('error' in res) {
            let logs;
            if ('data' in res.error) {
                logs = res.error.data.logs;
            }
            throw new SendTransactionError(
                'failed to send transaction: ' + res.error.message,
                logs,
            );
        }

        return res.result;
    }
}
