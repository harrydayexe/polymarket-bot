//! The dated table of taker fee coefficients by category (`config/fee_schedule_history.toml`,
//! §3.7 of the Phase 1 spec): the last fallback for a market's fee parameters, after Gamma's
//! `feeSchedule` and the CLOB's `fee_details`.
//!
//! The table is keyed by Polymarket's fee categories. A market is mapped to them from its
//! Gamma tag slugs ([fee_categories]); `politics` maps to both `politics_elections` and
//! `politics_other`, which the table cannot tell apart, so a politics market gets the higher
//! rate in force and [FeeSource::FallbackConservative].

use std::{collections::BTreeSet, path::Path};

use chrono::{DateTime, Utc};
use rust_decimal::Decimal;
use serde::{Deserialize, Serialize};

use crate::{
    invariant,
    invariant::InvariantViolation,
    units::{FeeParams, FeeSource, UtcMicros},
};

/// One row of the table: from `effective_from`, `category` pays `rate` with `exponent`.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct FeeRow {
    pub category: String,
    pub effective_from: DateTime<Utc>,
    pub rate: Decimal,
    pub exponent: u32,
    /// Where the row comes from (a URL).
    pub source: String,
    /// False when the docs do not pin down the row's date or rate.
    #[serde(default = "verified_default")]
    pub verified: bool,
}

fn verified_default() -> bool {
    true
}

/// Why the fee table could not be loaded.
#[derive(Debug, thiserror::Error)]
pub enum FeeTableError {
    #[error("could not read {path}: {source}")]
    Io {
        path: String,
        source: std::io::Error,
    },
    #[error("could not parse the fee table: {0}")]
    Parse(#[from] toml::de::Error),
    #[error("fee table row {index} ({category}): {problem}")]
    Row {
        index: usize,
        category: String,
        problem: &'static str,
    },
    #[error(transparent)]
    Invariant(#[from] InvariantViolation),
}

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct TableFile {
    fee: Vec<FeeRow>,
}

/// The dated fee table, rows sorted by (category, effective_from).
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct FeeTable {
    rows: Vec<FeeRow>,
}

/// Fee categories a Gamma tag slug maps to. Unknown tags map to none.
pub fn fee_categories(tag: &str) -> &'static [&'static str] {
    match tag {
        "geopolitics" => &["geopolitics"],
        "politics" => &["politics_elections", "politics_other"],
        "elections" => &["politics_elections"],
        "economics" | "economy" => &["economics"],
        "culture" | "pop-culture" => &["culture"],
        "finance" => &["finance"],
        "tech" => &["tech"],
        "sports" => &["sports"],
        _ => &[],
    }
}

impl FeeTable {
    /// Parses the table from TOML text.
    ///
    /// # Errors
    ///
    /// If the text is not a valid table, a rate is negative, or two rows of a category share
    /// an `effective_from`.
    pub fn from_toml_str(text: &str) -> Result<Self, FeeTableError> {
        let file: TableFile = toml::from_str(text)?;
        let mut rows = file.fee;
        for (index, row) in rows.iter().enumerate() {
            let problem = if row.category.is_empty() {
                Some("the category is empty")
            } else if row.rate < Decimal::ZERO {
                Some("the rate is negative")
            } else if row.exponent > marcasite_core::fees::MAX_EXPONENT {
                Some("the exponent is beyond what the fee formula accepts")
            } else {
                None
            };
            if let Some(problem) = problem {
                return Err(FeeTableError::Row {
                    index,
                    category: row.category.clone(),
                    problem,
                });
            }
        }
        // Before sorting, so the index is the row's place in the file.
        let mut seen = BTreeSet::new();
        for (index, row) in rows.iter().enumerate() {
            if !seen.insert((row.category.as_str(), row.effective_from)) {
                return Err(FeeTableError::Row {
                    index,
                    category: row.category.clone(),
                    problem: "two rows share an effective_from",
                });
            }
        }
        drop(seen);
        rows.sort_by(|a, b| {
            (a.category.as_str(), a.effective_from).cmp(&(b.category.as_str(), b.effective_from))
        });
        invariant!(
            rows.windows(2)
                .all(|p| (p[0].category.as_str(), p[0].effective_from)
                    < (p[1].category.as_str(), p[1].effective_from)),
            Err(InvariantViolation("fee rows are sorted and unique").into())
        );
        Ok(Self { rows })
    }

    /// Reads the table from a file.
    ///
    /// # Errors
    ///
    /// If the file cannot be read, or as [from_toml_str](Self::from_toml_str).
    pub fn load(path: &Path) -> Result<Self, FeeTableError> {
        let text = std::fs::read_to_string(path).map_err(|source| FeeTableError::Io {
            path: path.display().to_string(),
            source,
        })?;
        Self::from_toml_str(&text)
    }

    pub fn rows(&self) -> &[FeeRow] {
        &self.rows
    }

    /// Whether the table has any row for `category`.
    pub fn knows(&self, category: &str) -> bool {
        self.rows.iter().any(|row| row.category == category)
    }

    /// The row of `category` in force at `at`: the one with the latest `effective_from ≤ at`.
    /// `None` if the category has no row by then (no fees yet) or is not in the table.
    pub fn lookup(&self, category: &str, at: DateTime<Utc>) -> Option<&FeeRow> {
        self.rows
            .iter()
            .filter(|row| row.category == category && row.effective_from <= at)
            .max_by_key(|row| row.effective_from)
    }

    /// Fallback fee parameters for a market with Gamma tag slugs `tags` at `at` (§3.7).
    ///
    /// Every fee category the tags map to that the table knows is looked up (rate 0 before a
    /// category's first row); the highest rate wins. The source is
    /// [FeeSource::FallbackConservative] when more than one category was considered,
    /// otherwise [FeeSource::Fallback]. `None` when no tag maps to a known category.
    pub fn fallback<S: AsRef<str>>(&self, tags: &[S], at: UtcMicros) -> Option<FeeParams> {
        let mut categories: Vec<&str> = tags
            .iter()
            .flat_map(|tag| fee_categories(tag.as_ref()).iter().copied())
            .filter(|category| self.knows(category))
            .collect();
        categories.sort_unstable();
        categories.dedup();
        let (rate, exponent) = categories
            .iter()
            .map(|category| {
                self.lookup(category, at)
                    .map_or((Decimal::ZERO, 1), |row| (row.rate, row.exponent))
            })
            .max()?;
        let source = if categories.len() > 1 {
            FeeSource::FallbackConservative
        } else {
            FeeSource::Fallback
        };
        invariant!(rate >= Decimal::ZERO, None);
        Some(FeeParams {
            rate,
            exponent,
            source,
            seen_at: at,
        })
    }
}

#[cfg(test)]
mod tests {
    use std::path::PathBuf;

    use serde_json::Value;

    use super::*;

    fn repo_root() -> PathBuf {
        Path::new(env!("CARGO_MANIFEST_DIR")).join("../..")
    }

    fn table() -> FeeTable {
        FeeTable::load(&repo_root().join("config/fee_schedule_history.toml")).unwrap()
    }

    fn at(text: &str) -> DateTime<Utc> {
        text.parse().unwrap()
    }

    #[test]
    fn fallback_matches_the_shared_fixture() {
        let table = table();
        let text =
            std::fs::read_to_string(repo_root().join("fixtures/fees/fallback.json")).unwrap();
        let fixture: Value = serde_json::from_str(&text).unwrap();
        for case in fixture["cases"].as_array().unwrap() {
            let name = case["name"].as_str().unwrap();
            let tags: Vec<String> = serde_json::from_value(case["tags"].clone()).unwrap();
            let when = at(case["at"].as_str().unwrap());
            let got = table.fallback(&tags, when);
            if case["rate"].is_null() {
                assert_eq!(got, None, "{name}");
                continue;
            }
            let got = got.unwrap_or_else(|| panic!("{name}: no fallback"));
            let rate: Decimal = case["rate"].as_str().unwrap().parse().unwrap();
            assert_eq!(got.rate, rate, "{name}");
            assert_eq!(
                u64::from(got.exponent),
                case["exponent"].as_u64().unwrap(),
                "{name}"
            );
            assert_eq!(
                got.source.as_str(),
                case["source"].as_str().unwrap(),
                "{name}"
            );
            assert_eq!(got.seen_at, when, "{name}");
        }
    }

    #[test]
    fn lookup_takes_the_latest_row_in_force() {
        let table = table();
        let before = table
            .lookup("politics_other", at("2026-11-20T23:58:59.999999Z"))
            .unwrap();
        assert_eq!(before.rate, Decimal::new(4, 2));
        assert!(!before.verified);
        let after = table
            .lookup("politics_other", at("2026-11-20T23:59:00Z"))
            .unwrap();
        assert_eq!(after.rate, Decimal::new(1, 2));
        assert!(after.verified);
        assert_eq!(
            table.lookup("politics_other", at("2026-01-01T00:00:00Z")),
            None
        );
        assert_eq!(table.lookup("crypto", at("2026-12-01T00:00:00Z")), None);
    }

    #[test]
    fn bad_tables_are_refused() {
        let row = |rate: &str, from: &str| {
            format!(
                "[[fee]]\ncategory = \"x\"\neffective_from = \"{from}\"\nrate = \"{rate}\"\n\
                 exponent = 1\nsource = \"s\"\n"
            )
        };
        assert!(FeeTable::from_toml_str(&row("0.01", "2026-01-01T00:00:00Z")).is_ok());
        assert!(matches!(
            FeeTable::from_toml_str(&row("-0.01", "2026-01-01T00:00:00Z")),
            Err(FeeTableError::Row { .. })
        ));
        let twice = row("0.01", "2026-01-01T00:00:00Z") + &row("0.02", "2026-01-01T00:00:00Z");
        assert!(FeeTable::from_toml_str(&twice).is_err());
        assert!(FeeTable::from_toml_str("[[fee]]\ncategory = \"x\"\n").is_err());
    }
}
