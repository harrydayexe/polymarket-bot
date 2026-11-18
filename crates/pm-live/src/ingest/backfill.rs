//! Volume backfill: Data API v2 taker trades of the last 24 h, per condition, on the
//! `data_bulk` budget (D12).
//!
//! `/v2/trades` ignores `start` unless a user is given, and returns rows newest first, so a
//! batch of up to 20 conditions is paged until a row is older than the interval's start.
//! Each condition of the batch then gets one `VolumeBackfill` with the rows in
//! `[from, until)`; `complete` is false if a page failed or the page cap was hit.

use std::collections::{BTreeMap, VecDeque};

use anyhow::Result;
use marcasite::{
    data::DataClient,
    types::{ConditionId, Side},
};
use pm_core::{
    clock::SharedClock,
    normalize::{TakerSide, TxHash},
    units::{Price, Shares, UtcMicros},
};
use tokio::{sync::mpsc, task::JoinSet};
use tokio_util::sync::CancellationToken;

use crate::event::{BackfillPrint, Event, Stamped};

/// Conditions per `/v2/trades` request (the API's limit).
pub const CONDITIONS_PER_REQUEST: usize = 20;
/// Rows per page (the API's maximum).
pub const ROWS_PER_PAGE: u32 = 1000;

/// A request from the engine.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct BackfillRequest {
    pub conditions: Vec<ConditionId>,
    pub from: UtcMicros,
    pub until: UtcMicros,
}

/// The rows of one batch, by condition, and whether paging finished.
#[derive(Debug, Default)]
pub struct BatchResult {
    pub prints: BTreeMap<ConditionId, Vec<BackfillPrint>>,
    pub complete: bool,
    pub pages: u32,
    pub rows: u64,
}

fn to_print(trade: &marcasite::data::Trade) -> Option<BackfillPrint> {
    let side = match trade.side {
        Side::Buy => TakerSide::Buy,
        Side::Sell => TakerSide::Sell,
        _ => return None,
    };
    Some(BackfillPrint {
        token: trade.token_id.clone(),
        side,
        price: Price::new(trade.price).ok()?,
        size: Shares::new(trade.size).ok()?,
        tx_hash: TxHash::new(trade.transaction_hash.to_ascii_lowercase()),
        timestamp: trade.timestamp,
    })
}

/// Pages the taker trades of `conditions` (at most [CONDITIONS_PER_REQUEST]) back to `from`.
pub async fn fetch_batch(
    client: &DataClient,
    conditions: &[ConditionId],
    from: UtcMicros,
    until: UtcMicros,
    max_pages: u32,
) -> BatchResult {
    let mut result = BatchResult {
        prints: conditions.iter().map(|c| (c.clone(), Vec::new())).collect(),
        ..BatchResult::default()
    };
    let mut cursor: Option<String> = None;
    loop {
        if result.pages >= max_pages {
            tracing::warn!(pages = result.pages, "backfill page cap reached");
            return result;
        }
        let mut request = client
            .list_trades()
            .conditions(conditions.to_vec())
            .taker_only(true)
            .limit(ROWS_PER_PAGE);
        if let Some(cursor) = &cursor {
            request = request.cursor(cursor.clone());
        }
        let page = match request.send().await {
            Ok(page) => page,
            Err(error) => {
                tracing::warn!(%error, pages = result.pages, "backfill page failed");
                return result;
            }
        };
        result.pages += 1;
        let mut oldest: Option<UtcMicros> = None;
        for trade in page.items() {
            result.rows += 1;
            oldest = Some(oldest.map_or(trade.timestamp, |o| o.min(trade.timestamp)));
            if trade.timestamp < from || trade.timestamp >= until {
                continue;
            }
            let Some(prints) = result.prints.get_mut(&trade.condition_id) else {
                continue;
            };
            if let Some(print) = to_print(trade) {
                prints.push(print);
            }
        }
        let reached_start = oldest.is_some_and(|o| o < from);
        if reached_start || !page.has_more() {
            result.complete = true;
            return result;
        }
        match page.next_cursor() {
            Some(next) if cursor.as_deref() != Some(next) => cursor = Some(next.to_owned()),
            // More pages, but no way to them: what was read is not the whole interval.
            next => {
                tracing::warn!(pages = result.pages, cursor = ?next, "backfill paging stuck");
                return result;
            }
        }
    }
}

/// Serves backfill requests until cancelled or the engine is gone, with at most
/// `concurrency` batches in flight.
///
/// # Errors
///
/// Never in practice; the result type matches the other tasks'.
pub async fn backfill_task(
    client: DataClient,
    concurrency: usize,
    max_pages: u32,
    clock: SharedClock,
    out: mpsc::Sender<Stamped>,
    mut requests: mpsc::UnboundedReceiver<BackfillRequest>,
    token: CancellationToken,
) -> Result<()> {
    let concurrency = concurrency.max(1);
    // Batches waiting for a slot: only `concurrency` tasks exist at a time.
    let mut queue: VecDeque<(Vec<ConditionId>, UtcMicros, UtcMicros)> = VecDeque::new();
    let mut batches = JoinSet::new();
    loop {
        while batches.len() < concurrency
            && let Some((chunk, from, until)) = queue.pop_front()
        {
            let (client, clock, out) = (client.clone(), clock.clone(), out.clone());
            batches.spawn(async move {
                let result = fetch_batch(&client, &chunk, from, until, max_pages).await;
                tracing::debug!(
                    conditions = chunk.len(),
                    pages = result.pages,
                    rows = result.rows,
                    complete = result.complete,
                    "backfill batch done"
                );
                let now = clock.now();
                for (condition, prints) in result.prints {
                    let event = Event::VolumeBackfill {
                        condition,
                        prints,
                        complete: result.complete,
                        from,
                        until,
                    };
                    if out.send(Stamped::at(now, event)).await.is_err() {
                        return;
                    }
                }
            });
        }
        tokio::select! {
            biased;
            () = token.cancelled() => break,
            Some(done) = batches.join_next(), if !batches.is_empty() => {
                if let Err(error) = done {
                    tracing::error!(%error, "backfill batch panicked");
                }
            }
            request = requests.recv() => {
                let Some(request) = request else { break };
                for chunk in request.conditions.chunks(CONDITIONS_PER_REQUEST) {
                    queue.push_back((chunk.to_vec(), request.from, request.until));
                }
            }
        }
    }
    batches.abort_all();
    Ok(())
}

#[cfg(test)]
mod tests;
