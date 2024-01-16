use self::tc1::Tc1;
use self::tc2::Tc2;
use self::tc3::Tc3;
use self::tc4::Tc4;

pub mod tc1;
pub mod tc2;
pub mod tc3;
pub mod tc4;

#[async_trait::async_trait]
pub trait Strategy {
    async fn execute(&self) -> anyhow::Result<Vec<serde_json::Value>>;
}

#[derive(clap::Subcommand, Debug)]
pub enum Strategies {
    Tc1(Tc1),
    Tc2(Tc2),
    Tc3(Tc3),
    Tc4(Tc4),
}

#[async_trait::async_trait]
impl Strategy for Strategies {
    async fn execute(&self) -> anyhow::Result<Vec<serde_json::Value>> {
        let res = match self {
            Strategies::Tc1(tc1) => tc1.execute().await?,
            Strategies::Tc2(tc2) => tc2.execute().await?,
            Strategies::Tc3(tc3) => tc3.execute().await?,
            Strategies::Tc4(tc4) => tc4.execute().await?,
        };

        Ok(res)
    }
}
