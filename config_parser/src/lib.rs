use proc_macro::TokenStream;
use quote::{quote, ToTokens};
use syn::{Data, Expr};

struct Arg<'a> {
    name: &'a syn::Ident,
    ty: &'a syn::Type,
    desc: Option<Expr>,
    default: Option<Expr>,
    short: Option<Expr>,
    long: Option<Expr>,
}

#[proc_macro_derive(ConfigParser, attributes(arg))]
pub fn config_parser(input: TokenStream) -> TokenStream {
    let input = syn::parse::<syn::DeriveInput>(input).unwrap();

    let name = input.ident;

    let Data::Struct(ref data) = input.data else {
        panic!("expected struct");
    };

    // get the fields and attributes
    let args = data
        .fields
        .iter()
        .map(|field| {
            let name = field.ident.as_ref().expect("expected field name");

            let mut arg = Arg {
                name,
                ty: &field.ty,
                desc: None,
                default: None,
                short: None,
                long: None,
            };

            field
                .attrs
                .iter()
                .find(|attr| attr.path().is_ident("arg"))
                .expect("expected arg attribute")
                .parse_nested_meta(|meta| {
                    let value = meta
                        .value()
                        .map(|value| value.parse::<Expr>().unwrap())
                        .ok();

                    if meta.path.is_ident("default") {
                        arg.default = Some(value.expect("expected default value"));
                    } else if meta.path.is_ident("short") {
                        arg.short = Some(value.unwrap_or_else(|| {
                            let c = arg.name.to_string().chars().next().unwrap();
                            syn::parse(quote!(#c).into()).unwrap()
                        }));
                    } else if meta.path.is_ident("long") {
                        arg.long = Some(
                            value.unwrap_or_else(|| syn::parse(quote!(#name).into()).unwrap()),
                        );
                    } else if meta.path.is_ident("desc") {
                        arg.desc = value;
                    } else {
                        panic!("unknown arg attribute");
                    }

                    Ok(())
                })
                .expect("expected arg attribute to be a list of key-value pairs");

            if arg.name == "config" && arg.default.is_none() {
                panic!("config must have a default value");
            }

            arg
        })
        .collect::<Vec<_>>();

    // default config path
    let config_default = args
        .iter()
        .find(|arg| arg.name == "config")
        .expect("config arg must be present")
        .default
        .as_ref()
        .unwrap();

    let arg_matches = args.iter().map(|arg| {
        let short = arg
            .short
            .as_ref()
            .map(|short| quote!(-#short))
            .unwrap_or_else(|| quote! {});

        let long = arg
            .long
            .as_ref()
            .map(|long| quote!(--#long))
            .unwrap_or_else(|| quote! {});

        let val = if arg.ty.into_token_stream().to_string() == "bool" {
            quote!()
        } else {
            let name = arg.name.to_string().to_uppercase();
            quote!(<#name>)
        };

        let desc = arg
            .desc
            .as_ref()
            .map(|desc| quote!(#desc))
            .unwrap_or_else(|| quote! {String::new()});

        let default = arg
            .default
            .as_ref()
            .map(|default| quote!(format!("[default : {}]",  #default)))
            .unwrap_or_else(|| quote! {String::new()});

        quote! {
            arg! (#short #long #val)
                .help(&format!("{} {}", #desc, #default))
        }
    });

    let assemble_args = args.iter().map(|arg| {
        let name = arg.name;
        let ty = arg.ty;
        let default = if let Some(ref default) = arg.default {
            quote! {
                value.unwrap_or_else(|| #default)
            }
        } else {
            quote! {
                value.unwrap_or_default()
            }
        };

        quote! {
            #name: {
                let name = stringify!(#name);
                let value = matches.remove_one::<#ty>(name);
                let value = value.or_else(|| {
                    config.as_ref().and_then(|config| {
                        config.get(name).and_then(|value| {
                            serde_json::from_value(value.clone()).ok()
                        })
                    })
                });
                #default
            }
        }
    });

    quote! {
        impl #name {
            async fn check_if_config_exists<P: AsRef<std::path::Path>>(path: P) -> anyhow::Result<()> {
                // Check if file exists
                if tokio::fs::metadata(&path).await.is_err() {
                    anyhow::bail!("Config file does not exist")
                }

                Ok(())
            }

            async fn read_config<P: AsRef<std::path::Path>>(path: P) -> anyhow::Result<serde_json::Value> {
                use tokio::io::AsyncReadExt;

                // Open the file
                let mut file = tokio::fs::File::open(path).await?;

                // Read the contents
                let mut contents = String::new();
                file.read_to_string(&mut contents).await?;

                // Parse the contents
                Ok(serde_json::from_str(&contents)?)
            }

            pub async fn parse() -> anyhow::Result<Self> {
                use clap::{arg, command, value_parser, ArgAction, Command};
                use tokio::fs::File;

                // get matches
                let mut matches = command!() // requires `cargo` feature
                    .args([
                        #(#arg_matches,)*
                    ])
                    .get_matches();

                // get config path
                let config_path: Option<String> = matches.remove_one("config");
                let config_arg_provided = config_path.is_some();
                let config_path = config_path.unwrap_or(#config_default);

                let config = if Self::check_if_config_exists(&config_path).await.is_ok() {
                    Some(Self::read_config(&config_path).await?)
                } else {
                    if config_arg_provided {
                        anyhow::bail!("Config file does not exist")
                    } else {
                        None
                    }
                };


                Ok(Self {
                    #(
                        #assemble_args,
                    )*
                })
            }
        }
    }
    .into()
}
