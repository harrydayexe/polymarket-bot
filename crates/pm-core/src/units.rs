//! The units every price, quantity and amount of money is held in (§3.1 of the Phase 1
//! spec), the logit transform and the fee curve (§3.7).
//!
//! Money is never a float: [Price], [Shares] and [Usd] wrap [Decimal]. Arithmetic is exact,
//! and a result is rounded half-even to [MONEY_DP] places only at the end of an expression,
//! with [Usd::rounded] (or [round_money]). `Shares × Price → Usd` is the only multiplication
//! between these types. Floats are for model quantities only: [Logit] and [Probability].
//!
//! Times are `DateTime<Utc>` ([UtcMicros]), serialised as integer microseconds since the
//! epoch with [micros] (or [micros_option]).

use std::{
    fmt,
    iter::Sum,
    ops::{Add, AddAssign, Mul, Neg, Sub, SubAssign},
};

use chrono::{DateTime, Utc};
use rust_decimal::{Decimal, RoundingStrategy, prelude::ToPrimitive};
use serde::{Deserialize, Serialize};

use crate::{invariant, invariant::InvariantViolation};

/// Decimal places of every stored amount, as in the archive.
pub const MONEY_DP: u32 = 6;

/// A moment, at microsecond resolution.
pub type UtcMicros = DateTime<Utc>;

/// Serde for a [UtcMicros] field as integer microseconds: `#[serde(with = "micros")]`.
pub use chrono::serde::ts_microseconds as micros;
/// Serde for an `Option<UtcMicros>` field as integer microseconds or `null`.
pub use chrono::serde::ts_microseconds_option as micros_option;

/// `value` rounded half-even to [MONEY_DP] places.
pub fn round_money(value: Decimal) -> Decimal {
    value.round_dp_with_strategy(MONEY_DP, RoundingStrategy::MidpointNearestEven)
}

/// A value outside the range of its unit, or an operation undefined for its input.
#[derive(Debug, Clone, PartialEq, thiserror::Error)]
pub enum UnitError {
    #[error("price {0} is outside [0, 1]")]
    Price(Decimal),
    #[error("share quantity {0} is negative")]
    Shares(Decimal),
    #[error("probability {0} is outside [0, 1]")]
    Probability(f64),
    #[error("logit of {0} is infinite; it needs a probability strictly between 0 and 1")]
    LogitUndefined(f64),
    #[error("invalid fee parameters or inputs: {0}")]
    Fee(String),
    #[error(transparent)]
    Invariant(#[from] InvariantViolation),
}

/// The price of one outcome share in USD, in [0, 1].
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize, Deserialize)]
#[serde(try_from = "Decimal", into = "Decimal")]
pub struct Price(Decimal);

impl Price {
    pub const ZERO: Price = Price(Decimal::ZERO);
    pub const ONE: Price = Price(Decimal::ONE);

    /// # Errors
    ///
    /// If `value` is outside [0, 1].
    pub fn new(value: Decimal) -> Result<Self, UnitError> {
        if value < Decimal::ZERO || value > Decimal::ONE {
            return Err(UnitError::Price(value));
        }
        Ok(Price(value.normalize()))
    }

    pub fn value(self) -> Decimal {
        self.0
    }

    /// `1 − p`: the price of the other outcome of a binary market, at parity.
    pub fn complement(self) -> Price {
        Price(Decimal::ONE - self.0)
    }

    /// The price read as a probability.
    pub fn to_probability(self) -> Probability {
        // In [0, 1] by construction, so the conversion cannot fail or leave the range.
        Probability(self.0.to_f64().unwrap_or(0.0).clamp(0.0, 1.0))
    }

    /// Rounded half-even to [MONEY_DP] places (a value in [0, 1] stays in it).
    pub fn rounded(self) -> Price {
        Price(round_money(self.0))
    }
}

impl TryFrom<Decimal> for Price {
    type Error = UnitError;

    fn try_from(value: Decimal) -> Result<Self, Self::Error> {
        Price::new(value)
    }
}

impl From<Price> for Decimal {
    fn from(price: Price) -> Decimal {
        price.0
    }
}

impl fmt::Display for Price {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        self.0.fmt(f)
    }
}

/// A quantity of outcome shares, ≥ 0.
#[derive(
    Debug, Clone, Copy, Default, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize, Deserialize,
)]
#[serde(try_from = "Decimal", into = "Decimal")]
pub struct Shares(Decimal);

impl Shares {
    pub const ZERO: Shares = Shares(Decimal::ZERO);

    /// # Errors
    ///
    /// If `value` is negative.
    pub fn new(value: Decimal) -> Result<Self, UnitError> {
        if value < Decimal::ZERO {
            return Err(UnitError::Shares(value));
        }
        Ok(Shares(value.normalize()))
    }

    pub fn value(self) -> Decimal {
        self.0
    }

    pub fn is_zero(self) -> bool {
        self.0.is_zero()
    }

    /// The smaller of two quantities.
    pub fn min(self, other: Shares) -> Shares {
        Shares(self.0.min(other.0))
    }

    /// `self − other`, or zero if `other` is larger.
    pub fn saturating_sub(self, other: Shares) -> Shares {
        Shares((self.0 - other.0).max(Decimal::ZERO))
    }

    pub fn rounded(self) -> Shares {
        Shares(round_money(self.0))
    }
}

impl Add for Shares {
    type Output = Shares;

    fn add(self, other: Shares) -> Shares {
        Shares(self.0 + other.0)
    }
}

impl AddAssign for Shares {
    fn add_assign(&mut self, other: Shares) {
        self.0 += other.0;
    }
}

impl Sum for Shares {
    fn sum<I: Iterator<Item = Shares>>(iter: I) -> Shares {
        iter.fold(Shares::ZERO, Add::add)
    }
}

impl TryFrom<Decimal> for Shares {
    type Error = UnitError;

    fn try_from(value: Decimal) -> Result<Self, Self::Error> {
        Shares::new(value)
    }
}

impl From<Shares> for Decimal {
    fn from(shares: Shares) -> Decimal {
        shares.0
    }
}

impl fmt::Display for Shares {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        self.0.fmt(f)
    }
}

/// An amount of money in USD, signed (P&L can be negative).
#[derive(
    Debug, Clone, Copy, Default, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize, Deserialize,
)]
#[serde(transparent)]
pub struct Usd(Decimal);

impl Usd {
    pub const ZERO: Usd = Usd(Decimal::ZERO);

    pub fn new(value: Decimal) -> Self {
        Usd(value)
    }

    pub fn value(self) -> Decimal {
        self.0
    }

    /// Rounded half-even to [MONEY_DP] places; call once, at the end of an expression.
    pub fn rounded(self) -> Usd {
        Usd(round_money(self.0))
    }
}

impl Add for Usd {
    type Output = Usd;

    fn add(self, other: Usd) -> Usd {
        Usd(self.0 + other.0)
    }
}

impl Sub for Usd {
    type Output = Usd;

    fn sub(self, other: Usd) -> Usd {
        Usd(self.0 - other.0)
    }
}

impl Neg for Usd {
    type Output = Usd;

    fn neg(self) -> Usd {
        Usd(-self.0)
    }
}

impl AddAssign for Usd {
    fn add_assign(&mut self, other: Usd) {
        self.0 += other.0;
    }
}

impl SubAssign for Usd {
    fn sub_assign(&mut self, other: Usd) {
        self.0 -= other.0;
    }
}

impl Sum for Usd {
    fn sum<I: Iterator<Item = Usd>>(iter: I) -> Usd {
        iter.fold(Usd::ZERO, Add::add)
    }
}

impl From<Usd> for Decimal {
    fn from(usd: Usd) -> Decimal {
        usd.0
    }
}

impl fmt::Display for Usd {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        self.0.fmt(f)
    }
}

impl Mul<Price> for Shares {
    type Output = Usd;

    /// Exact; round the final result with [Usd::rounded].
    fn mul(self, price: Price) -> Usd {
        Usd(self.0 * price.0)
    }
}

impl Mul<Shares> for Price {
    type Output = Usd;

    fn mul(self, shares: Shares) -> Usd {
        shares * self
    }
}

/// A model probability (an output, q*, a weight), in [0, 1].
#[derive(Debug, Clone, Copy, PartialEq, PartialOrd, Serialize, Deserialize)]
#[serde(try_from = "f64", into = "f64")]
pub struct Probability(f64);

impl Probability {
    /// # Errors
    ///
    /// If `value` is NaN or outside [0, 1].
    pub fn new(value: f64) -> Result<Self, UnitError> {
        if !(0.0..=1.0).contains(&value) {
            return Err(UnitError::Probability(value));
        }
        Ok(Probability(value))
    }

    pub fn value(self) -> f64 {
        self.0
    }
}

impl TryFrom<f64> for Probability {
    type Error = UnitError;

    fn try_from(value: f64) -> Result<Self, Self::Error> {
        Probability::new(value)
    }
}

impl From<Probability> for f64 {
    fn from(p: Probability) -> f64 {
        p.0
    }
}

/// ln(p / (1 − p)): a probability on the whole real line.
#[derive(Debug, Clone, Copy, PartialEq, PartialOrd, Serialize, Deserialize)]
#[serde(transparent)]
pub struct Logit(pub f64);

impl Logit {
    pub fn value(self) -> f64 {
        self.0
    }
}

impl Sub for Logit {
    type Output = Logit;

    fn sub(self, other: Logit) -> Logit {
        Logit(self.0 - other.0)
    }
}

/// ln(p / (1 − p)).
///
/// # Errors
///
/// If `p` is 0 or 1, where the logit is infinite.
pub fn logit(p: Probability) -> Result<Logit, UnitError> {
    let p = p.value();
    if p <= 0.0 || p >= 1.0 {
        return Err(UnitError::LogitUndefined(p));
    }
    // ln(p) − ln(1 − p) keeps precision near both ends better than ln(p / (1 − p)).
    let x = p.ln() - (-p).ln_1p();
    invariant!(
        x.is_finite(),
        Err(InvariantViolation("logit is finite").into())
    );
    invariant!(
        (x > 0.0) == (p > 0.5) || p == 0.5,
        Err(InvariantViolation("logit has the sign of p − 0.5").into())
    );
    Ok(Logit(x))
}

/// 1 / (1 + e^−x), the inverse of [logit]. Saturates to 0 or 1 for very large |x|; a NaN
/// input gives 0.5, the logit 0 it cannot be distinguished from.
pub fn inv_logit(x: Logit) -> Probability {
    let x = x.value();
    invariant!(!x.is_nan(), Probability(0.5));
    // Written so that neither branch overflows.
    let p = if x >= 0.0 {
        1.0 / (1.0 + (-x).exp())
    } else {
        let e = x.exp();
        e / (1.0 + e)
    };
    invariant!((0.0..=1.0).contains(&p), Probability(0.5));
    Probability(p)
}

/// The exponent every live market has sent so far (marcasite, 18 Nov 2026). Any other value
/// is accepted, but results computed with it are flagged `fee_exponent_unverified` (§3.7).
pub const VERIFIED_FEE_EXPONENT: u32 = 1;

/// Decimal places of a charged fee (D14): Polymarket charges fees to 5 places.
pub const FEE_DP: u32 = marcasite_core::fees::FEE_DECIMALS;

/// Where a market's fee parameters came from (§3.7, D13), in order of preference.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum FeeSource {
    /// Gamma `Market.feeSchedule`, read on every registry poll.
    Gamma,
    /// CLOB `/clob-markets/{condition_id}` `fee_details`.
    Clob,
    /// The dated table in `config/fee_schedule_history.toml`.
    Fallback,
    /// The dated table, when the market's tags match more than one fee category (as politics
    /// does: elections or not), so the highest rate in force among them was used; also the
    /// `other` row for tags that match no category.
    FallbackConservative,
}

impl FeeSource {
    pub fn as_str(self) -> &'static str {
        match self {
            FeeSource::Gamma => "gamma",
            FeeSource::Clob => "clob",
            FeeSource::Fallback => "fallback",
            FeeSource::FallbackConservative => "fallback_conservative",
        }
    }
}

/// A market's taker fee parameters, where they came from and when they were seen: stored
/// alongside every use of a fee (rule 4).
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub struct FeeParams {
    pub rate: Decimal,
    pub exponent: u32,
    pub source: FeeSource,
    #[serde(with = "micros")]
    pub seen_at: UtcMicros,
}

impl FeeParams {
    /// Whether the exponent is one no live market has been seen to send, so fees computed
    /// with it rest on marcasite's reading of the formula (open question 25).
    pub fn exponent_unverified(&self) -> bool {
        self.exponent != VERIFIED_FEE_EXPONENT
    }

    /// The same parameters with the rate and exponent compared, ignoring source and time:
    /// whether a newly seen schedule is a change (D13).
    pub fn same_curve(&self, other: &FeeParams) -> bool {
        self.rate == other.rate && self.exponent == other.exponent
    }
}

fn fee_error(err: marcasite_core::Error) -> UnitError {
    UnitError::Fee(err.to_string())
}

/// The unrounded taker fee per share bought or sold at `price` under `params`:
/// `rate × (p × (1 − p))^exponent` (§3.7), computed by [marcasite_core::fees::taker_fee].
/// Scores and gates use this value (`φ`, `q*`).
///
/// # Errors
///
/// If the rate is negative or the exponent is beyond what marcasite accepts.
pub fn fee_per_share(price: Price, params: &FeeParams) -> Result<Usd, UnitError> {
    let fee =
        marcasite_core::fees::taker_fee(Decimal::ONE, price.value(), params.rate, params.exponent)
            .map_err(fee_error)?;
    invariant!(
        fee >= Decimal::ZERO,
        Err(InvariantViolation("a taker fee is not negative").into())
    );
    invariant!(
        fee <= params.rate.max(Decimal::ZERO),
        Err(InvariantViolation("fee per share is at most the rate").into())
    );
    Ok(Usd(fee))
}

/// The fee charged for a taker fill of `shares` at `price` (paper fills): the unrounded fee
/// rounded half-even to [FEE_DP] places (D14), so anything below 0.000005 is zero.
///
/// # Errors
///
/// As [fee_per_share].
pub fn fee_charged(shares: Shares, price: Price, params: &FeeParams) -> Result<Usd, UnitError> {
    let fee = marcasite_core::fees::taker_fee(
        shares.value(),
        price.value(),
        params.rate,
        params.exponent,
    )
    .map_err(fee_error)?;
    let charged = fee.round_dp_with_strategy(FEE_DP, RoundingStrategy::MidpointNearestEven);
    invariant!(
        charged >= Decimal::ZERO,
        Err(InvariantViolation("a charged fee is not negative").into())
    );
    invariant!(
        (charged - fee).abs() <= Decimal::new(5, FEE_DP + 1),
        Err(InvariantViolation("rounding moves a fee by at most half a unit").into())
    );
    Ok(Usd(charged))
}

#[cfg(test)]
mod tests {
    use proptest::prelude::*;
    use rust_decimal_macros::dec;

    use super::*;

    fn price(d: Decimal) -> Price {
        Price::new(d).unwrap()
    }

    fn shares(d: Decimal) -> Shares {
        Shares::new(d).unwrap()
    }

    #[test]
    fn price_must_be_in_the_unit_interval() {
        assert!(Price::new(dec!(0)).is_ok());
        assert!(Price::new(dec!(1)).is_ok());
        assert_eq!(
            Price::new(dec!(1.000001)),
            Err(UnitError::Price(dec!(1.000001)))
        );
        assert_eq!(Price::new(dec!(-0.01)), Err(UnitError::Price(dec!(-0.01))));
        assert_eq!(price(dec!(0.82)).complement(), price(dec!(0.18)));
    }

    #[test]
    fn shares_must_not_be_negative() {
        assert!(Shares::new(dec!(0)).is_ok());
        assert_eq!(Shares::new(dec!(-1)), Err(UnitError::Shares(dec!(-1))));
        assert_eq!(
            shares(dec!(3)).saturating_sub(shares(dec!(5))),
            Shares::ZERO
        );
    }

    #[test]
    fn shares_times_price_is_exact_until_rounded() {
        let notional = shares(dec!(3)) * price(dec!(0.3333335));
        assert_eq!(notional.value(), dec!(1.0000005));
        // Half-even: ...0005 rounds to the even neighbour.
        assert_eq!(notional.rounded().value(), dec!(1.000000));
        assert_eq!(Usd::new(dec!(1.0000015)).rounded().value(), dec!(1.000002));
        assert_eq!(price(dec!(0.5)) * shares(dec!(10)), Usd::new(dec!(5)));
    }

    #[test]
    fn usd_is_signed() {
        let pnl = Usd::new(dec!(1.5)) - Usd::new(dec!(2));
        assert_eq!(pnl, Usd::new(dec!(-0.5)));
        assert_eq!(-pnl, Usd::new(dec!(0.5)));
        let total: Usd = [dec!(1), dec!(2), dec!(-0.5)]
            .into_iter()
            .map(Usd::new)
            .sum();
        assert_eq!(total, Usd::new(dec!(2.5)));
    }

    #[test]
    fn serialised_as_decimal_strings() {
        assert_eq!(
            serde_json::to_string(&price(dec!(0.25))).unwrap(),
            "\"0.25\""
        );
        let back: Price = serde_json::from_str("\"0.25\"").unwrap();
        assert_eq!(back, price(dec!(0.25)));
        assert!(serde_json::from_str::<Price>("\"1.5\"").is_err());
        assert!(serde_json::from_str::<Shares>("\"-1\"").is_err());
        assert_eq!(
            serde_json::to_string(&Usd::new(dec!(-2.5))).unwrap(),
            "\"-2.5\""
        );
    }

    #[test]
    fn probability_range() {
        assert!(Probability::new(0.0).is_ok());
        assert!(Probability::new(1.0).is_ok());
        assert!(Probability::new(1.1).is_err());
        assert!(Probability::new(f64::NAN).is_err());
    }

    #[test]
    fn logit_values() {
        let half = Probability::new(0.5).unwrap();
        assert_eq!(logit(half).unwrap(), Logit(0.0));
        let x = logit(Probability::new(0.75).unwrap()).unwrap();
        assert!((x.value() - 3f64.ln()).abs() < 1e-12);
        assert!(matches!(
            logit(Probability::new(0.0).unwrap()),
            Err(UnitError::LogitUndefined(_))
        ));
        assert!(logit(Probability::new(1.0).unwrap()).is_err());
        assert_eq!(inv_logit(Logit(0.0)).value(), 0.5);
        assert_eq!(inv_logit(Logit(1e6)).value(), 1.0);
        assert_eq!(inv_logit(Logit(-1e6)).value(), 0.0);
        assert_eq!(inv_logit(Logit(f64::NAN)).value(), 0.5);
    }

    #[test]
    fn price_as_probability() {
        assert_eq!(price(dec!(0.25)).to_probability().value(), 0.25);
        assert_eq!(Price::ONE.to_probability().value(), 1.0);
    }

    proptest! {
        #[test]
        fn logit_round_trip(p in 0.001f64..=0.999) {
            let q = inv_logit(logit(Probability::new(p).unwrap()).unwrap()).value();
            prop_assert!((q - p).abs() < 1e-12, "p = {p}, back = {q}");
        }

        #[test]
        fn logit_is_increasing(a in 0.001f64..=0.999, b in 0.001f64..=0.999) {
            prop_assume!(a < b);
            let la = logit(Probability::new(a).unwrap()).unwrap();
            let lb = logit(Probability::new(b).unwrap()).unwrap();
            prop_assert!(la < lb);
        }
    }

    fn params(rate: Decimal, exponent: u32) -> FeeParams {
        FeeParams {
            rate,
            exponent,
            source: FeeSource::Gamma,
            seen_at: DateTime::from_timestamp_micros(0).unwrap(),
        }
    }

    /// Polymarket's published fee table (100 shares), shown rounded to cents: politics pays
    /// 0.04 (peak $1.00 at 50¢), crypto 0.07 (peak $1.75), sports 0.05 (peak $1.25).
    #[test]
    fn fee_matches_the_published_table() {
        let fee_100 = |p: Decimal, rate: Decimal| {
            (fee_per_share(price(p), &params(rate, 1)).unwrap() * 100)
                .round_dp_with_strategy(2, RoundingStrategy::MidpointAwayFromZero)
        };
        let table = [
            // (price, politics, crypto, sports)
            (dec!(0.01), dec!(0.04), dec!(0.07), dec!(0.05)),
            (dec!(0.05), dec!(0.19), dec!(0.33), dec!(0.24)),
            (dec!(0.10), dec!(0.36), dec!(0.63), dec!(0.45)),
            (dec!(0.20), dec!(0.64), dec!(1.12), dec!(0.80)),
            (dec!(0.25), dec!(0.75), dec!(1.31), dec!(0.94)),
            (dec!(0.30), dec!(0.84), dec!(1.47), dec!(1.05)),
            (dec!(0.50), dec!(1.00), dec!(1.75), dec!(1.25)),
            (dec!(0.70), dec!(0.84), dec!(1.47), dec!(1.05)),
            (dec!(0.99), dec!(0.04), dec!(0.07), dec!(0.05)),
        ];
        for (p, politics, crypto, sports) in table {
            assert_eq!(fee_100(p, dec!(0.04)), politics, "politics at {p}");
            assert_eq!(fee_100(p, dec!(0.07)), crypto, "crypto at {p}");
            assert_eq!(fee_100(p, dec!(0.05)), sports, "sports at {p}");
        }
        // Exact before display rounding: crypto at 1¢ is 0.0693 per 100 shares.
        assert_eq!(
            fee_per_share(price(dec!(0.01)), &params(dec!(0.07), 1)).unwrap() * 100,
            dec!(0.0693)
        );
    }

    impl Mul<i64> for Usd {
        type Output = Decimal;

        fn mul(self, n: i64) -> Decimal {
            self.0 * Decimal::from(n)
        }
    }

    #[test]
    fn fee_exponent() {
        // (0.5 × 0.5)^2 × 0.02 = 0.00125.
        let squared = params(dec!(0.02), 2);
        assert_eq!(
            fee_per_share(price(dec!(0.5)), &squared).unwrap(),
            Usd::new(dec!(0.00125))
        );
        assert!(squared.exponent_unverified());
        assert!(!params(dec!(0.02), 1).exponent_unverified());
        assert!(fee_per_share(price(dec!(0.5)), &params(dec!(-0.01), 1)).is_err());
        assert!(fee_per_share(price(dec!(0.5)), &params(dec!(0.01), 99)).is_err());
        assert_eq!(
            fee_per_share(Price::ONE, &params(dec!(0.04), 1)).unwrap(),
            Usd::ZERO
        );
    }

    #[test]
    fn charged_fee_is_rounded_half_even_to_five_places() {
        let shares = |d: Decimal| Shares::new(d).unwrap();
        // 1 share at 0.5, rate 0.00002: 0.000005 exactly, a tie that rounds to even (0).
        assert_eq!(
            fee_charged(shares(dec!(1)), price(dec!(0.5)), &params(dec!(0.00002), 1)).unwrap(),
            Usd::ZERO
        );
        // 3 shares: 0.000015, a tie that rounds to even (0.00002).
        assert_eq!(
            fee_charged(shares(dec!(3)), price(dec!(0.5)), &params(dec!(0.00002), 1)).unwrap(),
            Usd::new(dec!(0.00002))
        );
        assert_eq!(
            fee_charged(shares(dec!(100)), price(dec!(0.5)), &params(dec!(0.07), 1)).unwrap(),
            Usd::new(dec!(1.75))
        );
    }

    #[test]
    fn fees_match_the_shared_fixture() {
        let path = concat!(
            env!("CARGO_MANIFEST_DIR"),
            "/../../fixtures/fees/taker_fee.json"
        );
        let fixture: serde_json::Value =
            serde_json::from_str(&std::fs::read_to_string(path).unwrap()).unwrap();
        let dec = |v: &serde_json::Value| -> Decimal { v.as_str().unwrap().parse().unwrap() };
        for case in fixture["cases"].as_array().unwrap() {
            let name = case["name"].as_str().unwrap();
            let fee_params = params(
                dec(&case["rate"]),
                case["exponent"].as_u64().unwrap() as u32,
            );
            let shares = Shares::new(dec(&case["shares"])).unwrap();
            let p = price(dec(&case["price"]));
            let per_share = fee_per_share(p, &fee_params).unwrap();
            assert_eq!(
                per_share.value() * shares.value(),
                dec(&case["fee"]),
                "{name}"
            );
            assert_eq!(
                fee_charged(shares, p, &fee_params).unwrap().value(),
                dec(&case["charged"]),
                "{name}"
            );
        }
    }

    #[test]
    fn times_serialise_as_microseconds() {
        #[derive(Serialize, Deserialize, PartialEq, Debug)]
        struct At {
            #[serde(with = "micros")]
            at: UtcMicros,
        }
        let at = At {
            at: DateTime::from_timestamp_micros(1_790_348_400_123_456).unwrap(),
        };
        let text = serde_json::to_string(&at).unwrap();
        assert_eq!(text, r#"{"at":1790348400123456}"#);
        assert_eq!(serde_json::from_str::<At>(&text).unwrap(), at);
    }
}
