pub fn obfuscate_rpcurl(rpc_addr: &str) -> String {
    if rpc_addr.contains("rpcpool.com") {
        return rpc_addr.replacen(char::is_numeric, "X", 99);
    }
    rpc_addr.to_string()
}

pub fn obfuscate_token(token: &Option<String>) -> String {
    match token {
        None => "n/a".to_string(),
        Some(token) => {
            let mut token = token.clone();
            token.truncate(5);
            token += "...";
            token
        }
    }
}
