//! A print (one fill reported by the exchange) expressed as a buy of one outcome (§3.2).

use chrono::{DateTime, Utc};
use marcasite::{
    types::{ConditionId, Side, TokenId},
    ws::LastTradePriceEvent,
};
use serde::{Deserialize, Serialize};

use crate::{
    invariant,
    invariant::InvariantViolation,
    market::MarketTokens,
    units::{Price, Shares, UnitError, Usd, micros},
};

marcasite_core::string_id! {
    /// An on-chain transaction hash. Every fill of one match shares it.
    pub struct TxHash;
}

/// The two outcomes of a binary market.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, PartialOrd, Ord, Serialize, Deserialize)]
pub enum Outcome {
    Yes,
    No,
}

impl Outcome {
    pub fn other(self) -> Outcome {
        match self {
            Outcome::Yes => Outcome::No,
            Outcome::No => Outcome::Yes,
        }
    }
}

/// The taker's side of a print, as the websocket reports it.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[serde(rename_all = "UPPERCASE")]
pub enum TakerSide {
    Buy,
    Sell,
}

/// Why a print could not be normalised.
#[derive(Debug, Clone, PartialEq, thiserror::Error)]
pub enum NormaliseError {
    #[error("print is for token {token}, which is not a token of market {condition_id}")]
    UnknownToken {
        condition_id: ConditionId,
        token: TokenId,
    },
    #[error("print has side {0:?}, expected BUY or SELL")]
    Side(String),
    #[error(transparent)]
    Unit(#[from] UnitError),
    #[error(transparent)]
    Invariant(#[from] InvariantViolation),
}

/// A print as reported, on the token it was printed on.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct RawPrint {
    pub condition_id: ConditionId,
    pub token: TokenId,
    pub side: TakerSide,
    pub price: Price,
    pub size: Shares,
    pub tx_hash: Option<TxHash>,
    /// Exchange time.
    #[serde(with = "micros")]
    pub exchange_ts: DateTime<Utc>,
    /// When this process received it.
    #[serde(with = "micros")]
    pub recv_ts: DateTime<Utc>,
}

impl RawPrint {
    /// A websocket `last_trade_price` message received at `recv_ts`.
    ///
    /// # Errors
    ///
    /// If its side is neither `BUY` nor `SELL`, or its price or size is out of range.
    pub fn from_ws(
        event: &LastTradePriceEvent,
        recv_ts: DateTime<Utc>,
    ) -> Result<Self, NormaliseError> {
        let side = match &event.side {
            Side::Buy => TakerSide::Buy,
            Side::Sell => TakerSide::Sell,
            other => return Err(NormaliseError::Side(other.to_string())),
        };
        Ok(RawPrint {
            condition_id: event.market.clone(),
            token: event.asset_id.clone(),
            side,
            price: Price::new(event.price)?,
            size: Shares::new(event.size)?,
            tx_hash: event.transaction_hash.clone().map(TxHash::new),
            exchange_ts: event.timestamp,
            recv_ts,
        })
    }
}

/// A print expressed as a buy of one outcome: a `BUY` of token T at p is a buy of T at p; a
/// `SELL` of T at p is a buy of T's sibling at 1 − p. Every per-print quantity downstream
/// (price, notional, "the outcome bought") refers to the normalised outcome.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct NormalisedPrint {
    pub condition_id: ConditionId,
    /// The token the exchange reported the print on.
    pub printed_token: TokenId,
    /// The token of the outcome bought.
    pub bought_token: TokenId,
    pub outcome: Outcome,
    /// The side as reported, kept for the side-agreement check (D4).
    pub side_ws: TakerSide,
    /// Price of the outcome bought.
    pub price: Price,
    pub shares: Shares,
    /// `shares × price`, exact, in bought-outcome dollars (D2).
    pub notional: Usd,
    pub tx_hash: Option<TxHash>,
    #[serde(with = "micros")]
    pub exchange_ts: DateTime<Utc>,
    #[serde(with = "micros")]
    pub recv_ts: DateTime<Utc>,
}

impl NormalisedPrint {
    /// # Errors
    ///
    /// If the print's token is not one of `tokens`.
    pub fn new(raw: &RawPrint, tokens: &MarketTokens) -> Result<Self, NormaliseError> {
        let printed = if raw.token == tokens.yes {
            Outcome::Yes
        } else if raw.token == tokens.no {
            Outcome::No
        } else {
            return Err(NormaliseError::UnknownToken {
                condition_id: raw.condition_id.clone(),
                token: raw.token.clone(),
            });
        };
        invariant!(
            tokens.yes != tokens.no,
            Err(InvariantViolation("a market's two tokens differ").into())
        );
        let (outcome, price) = match raw.side {
            TakerSide::Buy => (printed, raw.price),
            TakerSide::Sell => (printed.other(), raw.price.complement()),
        };
        let bought_token = match outcome {
            Outcome::Yes => tokens.yes.clone(),
            Outcome::No => tokens.no.clone(),
        };
        invariant!(
            (raw.side == TakerSide::Buy) == (bought_token == raw.token),
            Err(InvariantViolation("a buy keeps its token and a sell flips it").into())
        );
        Ok(NormalisedPrint {
            condition_id: raw.condition_id.clone(),
            printed_token: raw.token.clone(),
            bought_token,
            outcome,
            side_ws: raw.side,
            price,
            shares: raw.size,
            notional: raw.size * price,
            tx_hash: raw.tx_hash.clone(),
            exchange_ts: raw.exchange_ts,
            recv_ts: raw.recv_ts,
        })
    }
}

#[cfg(test)]
mod tests {
    use rust_decimal_macros::dec;
    use serde_json::json;

    use super::*;
    use crate::test_util::t;

    fn tokens() -> MarketTokens {
        MarketTokens {
            yes: TokenId::new("1111"),
            no: TokenId::new("1222"),
        }
    }

    fn raw(token: &str, side: TakerSide, price: rust_decimal::Decimal) -> RawPrint {
        RawPrint {
            condition_id: ConditionId::new("0x01"),
            token: TokenId::new(token),
            side,
            price: Price::new(price).unwrap(),
            size: Shares::new(dec!(100)).unwrap(),
            tx_hash: Some(TxHash::new("0xabc")),
            exchange_ts: t(1000),
            recv_ts: t(1050),
        }
    }

    #[test]
    fn a_buy_stays_a_buy() {
        let p = NormalisedPrint::new(&raw("1111", TakerSide::Buy, dec!(0.2)), &tokens()).unwrap();
        assert_eq!(p.outcome, Outcome::Yes);
        assert_eq!(p.bought_token, TokenId::new("1111"));
        assert_eq!(p.price.value(), dec!(0.2));
        assert_eq!(p.notional, Usd::new(dec!(20)));
        assert_eq!(p.side_ws, TakerSide::Buy);
    }

    #[test]
    fn a_sell_of_yes_is_a_buy_of_no() {
        let p = NormalisedPrint::new(&raw("1111", TakerSide::Sell, dec!(0.82)), &tokens()).unwrap();
        assert_eq!(p.outcome, Outcome::No);
        assert_eq!(p.bought_token, TokenId::new("1222"));
        assert_eq!(p.printed_token, TokenId::new("1111"));
        assert_eq!(p.price.value(), dec!(0.18));
        assert_eq!(p.notional, Usd::new(dec!(18)));
    }

    #[test]
    fn a_sell_of_no_is_a_buy_of_yes() {
        let p = NormalisedPrint::new(&raw("1222", TakerSide::Sell, dec!(0.7)), &tokens()).unwrap();
        assert_eq!(p.outcome, Outcome::Yes);
        assert_eq!(p.price.value(), dec!(0.3));
    }

    #[test]
    fn a_token_of_another_market_is_refused() {
        let err = NormalisedPrint::new(&raw("9", TakerSide::Buy, dec!(0.5)), &tokens());
        assert!(matches!(err, Err(NormaliseError::UnknownToken { .. })));
    }

    #[test]
    fn from_a_websocket_message() {
        let event: LastTradePriceEvent = serde_json::from_value(json!({
            "asset_id": "1111", "market": "0x01", "price": "0.52", "size": "5",
            "fee_rate_bps": "0", "side": "SELL", "timestamp": "1757908892353",
            "transaction_hash": "0xabc"
        }))
        .unwrap();
        let raw = RawPrint::from_ws(&event, t(1_757_908_892_400)).unwrap();
        assert_eq!(raw.side, TakerSide::Sell);
        assert_eq!(raw.exchange_ts, t(1_757_908_892_353));
        assert_eq!(raw.tx_hash, Some(TxHash::new("0xabc")));
        let p = NormalisedPrint::new(&raw, &tokens()).unwrap();
        assert_eq!(p.price.value(), dec!(0.48));

        let mut bad = event.clone();
        bad.price = dec!(1.2);
        assert!(matches!(
            RawPrint::from_ws(&bad, t(0)),
            Err(NormaliseError::Unit(UnitError::Price(_)))
        ));
    }
}
