//! From anonymous prints to the quantities Tier 1 measures (§3.2, §3.3 and §3.6 of the
//! Phase 1 spec):
//!
//! - [mod@print]: a print as reported ([RawPrint]) and as a buy of one outcome
//!   ([NormalisedPrint]).
//! - [grouper]: prints grouped into the sweeps that produced them ([Grouper]).
//! - [pair_volume]: the rolling 24 h volume of a market's pair of tokens ([PairVolume]).
//!
//! All three are pure. The golden cases shared with the Python research code are in
//! `fixtures/tier1/grouping.json`.

pub mod grouper;
pub mod pair_volume;
pub mod print;

pub use grouper::{GroupFlag, Grouper, GrouperConfig, GrouperStats, PrintGroup};
pub use pair_volume::PairVolume;
pub use print::{NormaliseError, NormalisedPrint, Outcome, RawPrint, TakerSide, TxHash};
