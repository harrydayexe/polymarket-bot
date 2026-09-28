use chrono::DateTime;
use serde::Deserialize;
use serde_json::value::RawValue;

use crate::{
    market_registry::market_info::MarketInfo,
    types::{ConditionId, DecimalString, TokenId, UtcMicros},
};

/// Defines a market that was present on the page but could not be parsed.
#[derive(Debug)]
pub struct SkippedMarket {
    id: String,
    reason: String,
}

/// The page as a whole was unusable; the registry should reject this cycle.
#[derive(Debug)]
pub struct MarketPageError(pub String);

// ---- Wire format: only the Gamma fields we read. Unknown fields are ignored. ----

#[derive(Deserialize)]
#[serde(rename_all = "camelCase")]
struct GammaEvent {
    id: Option<String>,
    neg_risk: Option<bool>,
}

#[derive(Deserialize)]
struct GammaTag {
    slug: Option<String>,
}

#[derive(Deserialize)]
struct GammaFeeSchedule {
    rate: Option<Box<RawValue>>,
}

#[derive(Deserialize)]
#[serde(rename_all = "camelCase")]
struct GammaMarket {
    condition_id: Option<String>,
    question: Option<String>,
    slug: Option<String>,
    clob_token_ids: Option<String>, // JSON array encoded as a string
    outcomes: Option<String>,       // JSON array encoded as a string
    start_date: Option<String>,
    end_date: Option<String>,
    active: Option<bool>,
    closed: Option<bool>,
    accepting_orders: Option<bool>,
    liquidity: Option<Box<RawValue>>,
    order_price_min_tick_size: Option<Box<RawValue>>,
    neg_risk: Option<bool>,
    fees_enabled: Option<bool>,
    fee_type: Option<String>,
    fee_schedule: Option<GammaFeeSchedule>,
    events: Option<Vec<GammaEvent>>,
    tags: Option<Vec<GammaTag>>,
}

// ---- Public entry point ----

/// Parse one page of `GET /markets`. Well-formed binary Yes/No markets are
/// returned; anything else on the page is reported in `SkippedMarket`. Does no
/// eligibility filtering — that is `select`'s job.
pub fn parse_gamma_page(
    raw: &str,
    fetched_at: UtcMicros,
) -> Result<(Vec<MarketInfo>, Vec<SkippedMarket>), MarketPageError> {
    // Split into per-item raw JSON first, so one malformed market can't fail the page.
    let items: Vec<Box<RawValue>> = serde_json::from_str(raw)
        .map_err(|e| MarketPageError(format!("page is not a JSON list: {e}")))?;

    let mut markets = Vec::with_capacity(items.len());
    let mut skipped = Vec::new();

    for item in &items {
        match parse_market(item.get(), fetched_at) {
            Ok(m) => markets.push(m),
            Err(reason) => skipped.push(SkippedMarket {
                id: best_effort_id(item.get()),
                reason,
            }),
        }
    }
    Ok((markets, skipped))
}

fn parse_market(raw: &str, fetched_at: UtcMicros) -> Result<MarketInfo, String> {
    let m: GammaMarket = serde_json::from_str(raw).map_err(|e| format!("malformed market: {e}"))?;

    let condition_id = m
        .condition_id
        .filter(|s| !s.is_empty())
        .ok_or("missing conditionId")?;

    let token_ids = decode_embedded_list(m.clob_token_ids.as_deref(), "clobTokenIds")?;
    let outcomes = decode_embedded_list(m.outcomes.as_deref(), "outcomes")?;
    if token_ids.len() != 2 || outcomes.len() != 2 {
        return Err(format!(
            "not binary: {} outcomes, {} tokens",
            outcomes.len(),
            token_ids.len()
        ));
    }

    // Order comes from `outcomes`; never assume index 0 is Yes.
    let find = |w: &str| outcomes.iter().position(|o| o.eq_ignore_ascii_case(w));
    let (yes_i, no_i) = match (find("yes"), find("no")) {
        (Some(y), Some(n)) => (y, n),
        _ => return Err(format!("outcomes not Yes/No: {outcomes:?}")),
    };

    let tick_size = decimal_from_raw(
        m.order_price_min_tick_size.as_deref(),
        "orderPriceMinTickSize",
    )?
    .ok_or("missing orderPriceMinTickSize")?;
    let liquidity_usd = decimal_from_raw(m.liquidity.as_deref(), "liquidity")?
        .unwrap_or_else(|| DecimalString("0".to_owned()));
    let fee_rate = match &m.fee_schedule {
        Some(fs) => decimal_from_raw(fs.rate.as_deref(), "feeSchedule.rate")?,
        None => None,
    };

    let event = m.events.as_deref().and_then(<[_]>::first);

    Ok(MarketInfo {
        condition_id: ConditionId(condition_id),
        question: m.question.unwrap_or_default(),
        slug: m.slug.unwrap_or_default(),
        event_id: event.and_then(|e| e.id.clone()),
        tags: m
            .tags
            .unwrap_or_default()
            .into_iter()
            .filter_map(|t| t.slug.filter(|s| !s.is_empty()))
            .collect(),
        yes_token: TokenId(token_ids[yes_i].clone()),
        no_token: TokenId(token_ids[no_i].clone()),
        start_date: iso_to_micros(m.start_date.as_deref()),
        end_date: iso_to_micros(m.end_date.as_deref()),
        active: m.active == Some(true),
        closed: m.closed == Some(true),
        liquidity_usd,
        tick_size,
        neg_risk: m
            .neg_risk
            .or(event.and_then(|e| e.neg_risk))
            .unwrap_or(false),
        fetched_at,
        accepting_orders: m.accepting_orders == Some(true),
        fees_enabled: m.fees_enabled == Some(true),
        fee_type: m.fee_type,
        fee_rate,
    })
}

// ---- Helpers ----

/// Gamma encodes some arrays as a JSON string containing JSON: "[\"Yes\", \"No\"]".
fn decode_embedded_list(s: Option<&str>, field: &str) -> Result<Vec<String>, String> {
    let s = s.ok_or_else(|| format!("missing {field}"))?;
    serde_json::from_str(s).map_err(|e| format!("{field} is not a JSON string array: {e}"))
}

/// Take a number's literal text exactly as sent ("0.001"), never via f64.
/// Also accepts the value as a JSON string, since Gamma is inconsistent.
fn decimal_from_raw(raw: Option<&RawValue>, field: &str) -> Result<Option<DecimalString>, String> {
    let Some(raw) = raw else { return Ok(None) };
    let text = raw.get();
    let s = if text == "null" {
        return Ok(None);
    } else if text.starts_with('"') {
        serde_json::from_str::<String>(text).map_err(|e| format!("{field}: {e}"))?
    } else if text.starts_with(|c: char| c == '-' || c.is_ascii_digit()) {
        text.to_owned()
    } else {
        return Err(format!("{field}: expected a number, got {text}"));
    };
    Ok(Some(DecimalString(s)))
}

/// Unparseable dates become None rather than dropping the market.
fn iso_to_micros(s: Option<&str>) -> Option<UtcMicros> {
    let dt = DateTime::parse_from_rfc3339(s?).ok()?;
    Some(UtcMicros(dt.timestamp_micros()))
}

/// For skip reports: conditionId if present, else Gamma's id, else "?".
fn best_effort_id(raw: &str) -> String {
    let v: serde_json::Value = serde_json::from_str(raw).unwrap_or_default();
    ["conditionId", "id"]
        .iter()
        .filter_map(|k| v.get(k))
        .find(|x| !x.is_null())
        .map(|x| {
            x.as_str()
                .map(str::to_owned)
                .unwrap_or_else(|| x.to_string())
        })
        .unwrap_or_else(|| "?".to_owned())
}

#[cfg(test)]
mod tests {
    use super::*;

    const FIXTURE: &str = include_str!("../../fixtures/gamma_markets_page.json");
    const T: UtcMicros = UtcMicros(1_790_000_000_000_000);

    #[test]
    fn fixture_parses_cleanly() {
        let (markets, skipped) = parse_gamma_page(FIXTURE, T).unwrap();
        assert_eq!(markets.len(), 5);
        assert!(skipped.is_empty(), "{skipped:?}");

        let xi = &markets[0];
        assert!(!xi.neg_risk);
        assert_eq!(xi.tick_size, DecimalString("0.001".into()));
        assert_eq!(xi.fee_rate, Some(DecimalString("0.04".into())));
        assert_eq!(xi.fee_type.as_deref(), Some("politics_fees"));
        assert_eq!(xi.end_date, Some(UtcMicros(1_798_779_540_000_000)));
        assert!(xi.tags.contains(&"geopolitics".to_owned()));

        for m in &markets[1..] {
            assert!(m.neg_risk);
            assert_eq!(m.event_id.as_deref(), Some("30829"));
        }
    }

    #[test]
    fn yes_no_order_follows_outcomes() {
        let page = r#"[{"conditionId":"0x1","clobTokenIds":"[\"A\",\"B\"]",
                        "outcomes":"[\"No\",\"Yes\"]","orderPriceMinTickSize":0.01}]"#;
        let (m, _) = parse_gamma_page(page, T).unwrap();
        assert_eq!(m[0].yes_token, TokenId("B".into()));
        assert_eq!(m[0].no_token, TokenId("A".into()));
    }

    #[test]
    fn bad_markets_are_skipped_not_fatal() {
        let page = r#"[
          {"id":"1","conditionId":"0x1","clobTokenIds":"[\"A\",\"B\",\"C\"]","outcomes":"[\"X\",\"Y\",\"Z\"]","orderPriceMinTickSize":0.01},
          {"id":"2","conditionId":"0x2","clobTokenIds":"[\"A\",\"B\"]","outcomes":"[\"Up\",\"Down\"]","orderPriceMinTickSize":0.01},
          {"id":"3","conditionId":"0x3","outcomes":"[\"Yes\",\"No\"]","orderPriceMinTickSize":0.01},
          {"id":"4","conditionId":"0x4","active":"yes"},
          42
        ]"#;
        let (m, s) = parse_gamma_page(page, T).unwrap();
        assert!(m.is_empty());
        assert_eq!(s.len(), 5);
        assert_eq!(s[0].id, "0x1");
        assert_eq!(s[4].id, "?");
    }

    #[test]
    fn non_list_page_is_an_error() {
        assert!(parse_gamma_page(r#"{"error":"rate limited"}"#, T).is_err());
        assert!(parse_gamma_page("not json", T).is_err());
    }
}
