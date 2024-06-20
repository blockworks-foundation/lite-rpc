use itertools::Itertools;
use solana_sdk::signature::Keypair;

#[derive(Debug, Clone)]
pub struct TenantConfig {
    // technical identifier for the tenant, e.g. "solana-rpc"
    pub tenant_id: String,
    pub rpc_addr: String,
    // needs to point to a reliable websocket server that can be used to get tx status
    pub tx_status_ws_addr: Option<String>,
}

// recommend to use one payer keypair for all targets and fund that keypair with enough SOL
pub fn get_funded_payer_from_env() -> Keypair {
    let keypair58_string: String = std::env::var("FUNDED_PAYER_KEYPAIR58")
        .expect("need funded payer keypair on env (variable FUNDED_PAYER_KEYPAIR58)");
    Keypair::from_base58_string(&keypair58_string)
}

pub fn read_tenant_configs(env_vars: Vec<(String, String)>) -> Vec<TenantConfig> {
    let map = env_vars
        .iter()
        .filter(|(k, _)| k.starts_with("TENANT"))
        .into_group_map_by(|(k, _v)| {
            let tenant_counter = k
                .split('_')
                .next()
                .expect("tenant prefix must be split by underscore (e.g. TENANT99_SOMETHING")
                .replace("TENANT", "");
            tenant_counter
                .parse::<u32>()
                .expect("tenant counter must be a number (e.g. TENANT99)")
        });

    let values = map
        .iter()
        .sorted()
        .map(|(tc, v)| TenantConfig {
            tenant_id: v
                .iter()
                .find(|(v, _)| *v == format!("TENANT{}_ID", tc))
                .iter()
                .exactly_one()
                .expect("need TENANT_X_ID")
                .1
                .to_string(),
            rpc_addr: v
                .iter()
                .find(|(v, _)| *v == format!("TENANT{}_RPC_ADDR", tc))
                .iter()
                .exactly_one()
                .expect("need TENANT_X_RPC_ADDR")
                .1
                .to_string(),
            tx_status_ws_addr: v
                .iter()
                .find(|(v, _)| *v == format!("TENANT{}_TX_STATUS_WS_ADDR", tc))
                .iter()
                .at_most_one()
                .expect("need TENANT_X_TX_STATUS_WS_ADDR")
                .map(|(_, v)| v.to_string())
        })
        .collect::<Vec<TenantConfig>>();

    values
}

#[test]
fn test_env_vars() {
    let env_vars = vec![
        (String::from("TENANT1_ID"), String::from("solana-rpc")),
        (
            String::from("TENANT1_RPC_ADDR"),
            String::from("http://localhost:8899"),
        ),
        (String::from("TENANT2_ID"), String::from("lite-rpc")),
        (
            String::from("TENANT2_RPC_ADDR"),
            String::from("http://localhost:8890"),
        ),
    ];
    let tenant_configs = read_tenant_configs(env_vars);

    assert_eq!(tenant_configs.len(), 2);
    assert_eq!(tenant_configs[0].tenant_id, "solana-rpc");
    assert_eq!(tenant_configs[0].rpc_addr, "http://localhost:8899");
    assert_eq!(tenant_configs[1].tenant_id, "lite-rpc");
    assert_eq!(tenant_configs[1].rpc_addr, "http://localhost:8890");
}
