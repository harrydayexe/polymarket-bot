use clap::Args;

use crate::cli::CommonArgs;

#[derive(Args)]
pub struct SnapshotArgs {
    #[command(flatten)]
    common: CommonArgs,

    /// Snapshot everything
    #[arg(short, long, conflicts_with = "tokens")]
    all: bool,

    /// Snapshot specific tokens
    #[arg(short, long, value_delimiter = ',', num_args = 1..)]
    tokens: Option<Vec<String>>,
}

pub async fn execute(args: SnapshotArgs) -> anyhow::Result<()> {
    if args.all {
        println!("Snapshot all...");
    } else {
        match args.tokens {
            None => println!("Snapshotting..."),
            Some(v) => println!("Snapshotting tokens: {:?}", v),
        }
    }
    Ok(())
}
