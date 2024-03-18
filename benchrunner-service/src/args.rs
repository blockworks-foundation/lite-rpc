use std::collections::HashMap;
use clap::Parser;
use itertools::Itertools;


#[derive(Debug)]
pub struct TenantConfig {
    pub tenant: String,
    pub rpc_addr: String,
}


#[test]
fn test_env_vars() {
    let env_vars = vec![(String::from("TENANT1_ID"), String::from("solana-rpc")),
                        (String::from("TENANT1_RPC_ADDR"), String::from("http://localhost:8899")),
                        (String::from("TENANT2_ID"), String::from("lite-rpc")),
                        (String::from("TENANT2_RPC_ADDR"), String::from("http://localhost:8890"))];
    let tenant_configs = read_tenant_configs(env_vars);

    assert_eq!(tenant_configs.len(), 2);
    assert_eq!(tenant_configs[0].tenant, "solana-rpc");
    assert_eq!(tenant_configs[0].rpc_addr, "http://localhost:8899");
    assert_eq!(tenant_configs[1].tenant, "lite-rpc");
    assert_eq!(tenant_configs[1].rpc_addr, "http://localhost:8890");
}

pub fn read_tenant_configs(env_vars: Vec<(String, String)>) -> Vec<TenantConfig> {
    let map = env_vars.iter()
        .filter(|(k, _)| k.starts_with("TENANT"))
        .into_group_map_by(|(k, v)| {
            let tenant_id = k.split('_').nth(0).unwrap().replace("TENANT", "");
            tenant_id.to_string()
        });

    let values = map.iter().map(|(k, v)| {
        TenantConfig {
            tenant: v.iter().find(|(k, _)| k.ends_with("_ID")).expect("need ID").1.to_string(),
            rpc_addr: v.iter().find(|(k, _)| k.ends_with("_RPC_ADDR")).expect("need RPC_ADDR").1.to_string(),
        }
    }).collect::<Vec<TenantConfig>>();

    values
}