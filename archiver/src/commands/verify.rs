use chrono::NaiveDate;
use clap::Args;

#[derive(Args)]
pub struct VerifyArgs {
    /// Specify the date to verify
    #[arg(short, long, value_parser = crate::commands::parse_date::parse_date)]
    date: Option<NaiveDate>,
}

pub fn execute(args: VerifyArgs) -> anyhow::Result<()> {
    match args.date {
        None => println!("Verify..."),
        Some(d) => println!("Verify on {:?}", d),
    }
    Ok(())
}
