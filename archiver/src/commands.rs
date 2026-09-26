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

pub fn dispatch(cli: Cli) -> anyhow::Result<()> {
    match cli.command {
        Commands::Run => run::execute(),
        Commands::Check => check::execute(),
        Commands::Status => status::execute(),
        Commands::Markets => markets::execute(),
        Commands::Snapshot(args) => snapshot::execute(args),
        Commands::Compact(args) => compact::execute(args),
        Commands::Verify(args) => verify::execute(args),
        Commands::Gaps(args) => gaps::execute(args),
    }
}
