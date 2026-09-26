use chrono::NaiveDate;

/// Parse a YYYY-MM-DD format date string into a string, or return an error
pub fn parse_date(s: &str) -> Result<NaiveDate, String> {
    NaiveDate::parse_from_str(s, "%Y-%m-%d")
        .map_err(|e| format!("invalid date '{s}' (expected YYYY-MM-DD): {e}"))
}
