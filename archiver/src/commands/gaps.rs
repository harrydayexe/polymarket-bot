use chrono::NaiveDate;
use clap::Args;

#[derive(Args)]
pub struct GapsArgs {
    /// Specify the date to check for gaps on
    #[arg(short, long, value_parser = crate::commands::parse_date::parse_date)]
    date: Option<NaiveDate>,

    /// Which tokens to check for gaps in
    #[arg(short, long, value_delimiter = ',', num_args = 1..)]
    tokens: Option<Vec<String>>,
}

pub fn execute(args: GapsArgs) -> anyhow::Result<()> {
    match args.date {
        None => println!("Gaps..."),
        Some(d) => println!("Gaps on {:?}", d),
    }
    Ok(())
}
