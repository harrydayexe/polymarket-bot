use tokio_util::sync::CancellationToken;

use crate::cli::CommonArgs;
use crate::client::{APIClient, MarcasiteClient};
use crate::clock::{Clock, SystemClock};
use crate::config::Config;
use crate::market_registry::selection::{parse_markets, select};

/// Print the markets the registry would select right now, with counts. Changes nothing.
pub async fn execute(args: CommonArgs, token: CancellationToken) -> anyhow::Result<()> {
    let config = Config::load(&args.config)?;

    let fetch = async {
        let client = MarcasiteClient::connect(&config).await?;
        client.fetch_open_markets().await
    };
    let markets = tokio::select! {
        _ = token.cancelled() => return Ok(()),
        markets = fetch => markets?,
    };

    let fetched = markets.len();
    let (parsed, skipped) = parse_markets(markets, SystemClock.now());
    let mut selected: Vec<_> = parsed
        .iter()
        .filter(|market| select(market, &config))
        .collect();
    selected.sort_by(|a, b| a.question.cmp(&b.question));

    for market in &skipped {
        println!(
            "Skipped {}: {}",
            market
                .condition_id
                .as_ref()
                .map_or("market without condition ID".to_string(), |id| id
                    .to_string()),
            market.reason
        );
    }
    for market in &selected {
        println!("{}", market.question);
    }
    println!();
    println!(
        "{fetched} fetched, {} skipped as malformed, {} selected ({} tokens)",
        skipped.len(),
        selected.len(),
        selected.len() * 2
    );

    Ok(())
}
