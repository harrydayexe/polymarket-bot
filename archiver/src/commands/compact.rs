use chrono::NaiveDate;
use clap::Args;

#[derive(Args)]
pub struct CompactArgs {
    /// Date to compact since
    #[arg(
        short,
        long,
        value_parser = crate::commands::parse_date::parse_date,
        visible_alias = "since"
    )]
    date: Option<NaiveDate>,
}

pub fn execute(args: CompactArgs) -> anyhow::Result<()> {
    match args.date {
        None => println!("Compact..."),
        Some(d) => println!("Compacting since {:?}", d),
    }
    Ok(())
}
