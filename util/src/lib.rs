
// http://mango.rpcpool.com/c232ab232ba2323
pub fn obfuscate_rpcurl(rpc_addr: &str) -> String {
    if rpc_addr.contains("rpcpool.com") {
        return rpc_addr.replacen(char::is_numeric, "X", 99);
    }
    rpc_addr.to_string()
}