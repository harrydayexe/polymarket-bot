pub mod check;
pub mod compact;
pub mod gaps;
pub mod markets;
pub mod parse_date;
pub mod run;
pub mod snapshot;
pub mod status;
pub mod verify;

use crate::cli::{Cli, Commands};

pub async fn dispatch(cli: Cli) -> anyhow::Result<()> {
    match cli.command {
        Commands::Run(args) => run::execute(args).await,
        Commands::Check(args) => check::execute(args).await,
        Commands::Status(args) => status::execute(args).await,
        Commands::Markets(args) => markets::execute(args).await,
        Commands::Snapshot(args) => snapshot::execute(args).await,
        Commands::Compact(args) => compact::execute(args).await,
        Commands::Verify(args) => verify::execute(args).await,
        Commands::Gaps(args) => gaps::execute(args).await,
    }
}
