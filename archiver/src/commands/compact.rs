use chrono::NaiveDate;
use clap::Args;

use crate::cli::CommonArgs;

#[derive(Args)]
pub struct CompactArgs {
    #[command(flatten)]
    common: CommonArgs,

    /// Date to compact since
    #[arg(
        short,
        long,
        value_parser = crate::commands::parse_date::parse_date,
        visible_alias = "since"
    )]
    date: Option<NaiveDate>,
}

pub async fn execute(args: CompactArgs) -> anyhow::Result<()> {
    match args.date {
        None => println!("Compact..."),
        Some(d) => println!("Compacting since {:?}", d),
    }
    Ok(())
}
