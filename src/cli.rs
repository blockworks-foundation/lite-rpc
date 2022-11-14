use {
    clap::{App, Arg, ArgMatches},
    solana_clap_utils::input_validators::{is_url, is_url_or_moniker},
    solana_cli_config::ConfigInput,
    std::net::SocketAddr,
};

/// Holds the configuration for a single run of the benchmark
pub struct Config {
    pub rpc_addr: SocketAddr,
    pub subscription_port: SocketAddr,
    pub json_rpc_url: String,
    pub websocket_url: String,
}

impl Default for Config {
    fn default() -> Config {
        Config {
            rpc_addr: SocketAddr::from(([127, 0, 0, 1], 8899)),
            json_rpc_url: ConfigInput::default().json_rpc_url,
            websocket_url: ConfigInput::default().websocket_url,
            subscription_port: SocketAddr::from(([127, 0, 0, 1], 8900)),
        }
    }
}

/// Defines and builds the CLI args for a run of the benchmark
pub fn build_args<'a, 'b>(version: &'b str) -> App<'a, 'b> {
    App::new("lite rpc")
        .about("a lite version of solana rpc to send and confirm transactions")
        .version(version)
        .arg(
            Arg::with_name("json_rpc_url")
                .short("u")
                .long("url")
                .value_name("URL_OR_MONIKER")
                .takes_value(true)
                .global(true)
                .validator(is_url_or_moniker)
                .help(
                    "URL for Solana's JSON RPC or moniker (or their first letter): \
                     [mainnet-beta, testnet, devnet, localhost]",
                ),
        )
        .arg(
            Arg::with_name("websocket_url")
                .long("ws")
                .value_name("URL")
                .takes_value(true)
                .global(true)
                .validator(is_url)
                .help("WebSocket URL for the solana cluster"),
        )
        .arg(
            Arg::with_name("port")
                .long("port")
                .short("p")
                .takes_value(true)
                .global(true)
                .min_values(1025)
                .help("Port on which which lite rpc will listen to rpc requests"),
        )
        .arg(
            Arg::with_name("subscription_port")
                .long("sub_port")
                .short("sp")
                .takes_value(true)
                .global(true)
                .min_values(1025)
                .help("subscription port on which which lite rpc will use to create subscriptions"),
        )
}

pub fn extract_args(matches: &ArgMatches) -> Config {
    let mut args = Config::default();

    let config = if let Some(config_file) = matches.value_of("config_file") {
        solana_cli_config::Config::load(config_file).unwrap_or_default()
    } else {
        solana_cli_config::Config::default()
    };
    let (_, json_rpc_url) = ConfigInput::compute_json_rpc_url_setting(
        matches.value_of("json_rpc_url").unwrap_or(""),
        &config.json_rpc_url,
    );
    args.json_rpc_url = json_rpc_url;

    let (_, websocket_url) = ConfigInput::compute_websocket_url_setting(
        matches.value_of("websocket_url").unwrap_or(""),
        &config.websocket_url,
        matches.value_of("json_rpc_url").unwrap_or(""),
        &config.json_rpc_url,
    );
    args.websocket_url = websocket_url;
    if let Some(port) = matches.value_of("port") {
        let port: u16 = port.parse().expect("can't parse port");
        args.rpc_addr = SocketAddr::from(([127, 0, 0, 1], port));
    }

    if let Some(port) = matches.value_of("subsription_port") {
        let port: u16 = port.parse().expect("can't parse subsription_port");
        args.subscription_port = SocketAddr::from(([127, 0, 0, 1], port));
    } else {
        let port = args.rpc_addr.port().saturating_add(1);
        args.subscription_port = SocketAddr::from(([127, 0, 0, 1], port));
    }
    args
}
