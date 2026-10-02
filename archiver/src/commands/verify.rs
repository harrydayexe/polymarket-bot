use chrono::NaiveDate;
use clap::Args;

use crate::cli::CommonArgs;

#[derive(Args)]
pub struct VerifyArgs {
    #[command(flatten)]
    pub common: CommonArgs,

    /// Specify the date to verify
    #[arg(short, long, value_parser = crate::commands::parse_date::parse_date)]
    date: Option<NaiveDate>,
}

pub async fn execute(args: VerifyArgs) -> anyhow::Result<()> {
    match args.date {
        None => println!("Verify..."),
        Some(d) => println!("Verify on {:?}", d),
    }
    Ok(())
}
