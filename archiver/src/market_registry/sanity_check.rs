use crate::market_registry::not_enough_markets_error::NotEnoughMarketsError;

/// Guard against acting on a bad Gamma response.
pub fn sanity_check(
    fetched_count: u64,
    last_accepted_count: u64,
) -> Result<(), NotEnoughMarketsError> {
    if fetched_count == 0 {
        return Err(NotEnoughMarketsError {
            fetched_count,
            last_accepted_count,
            threshold: 0,
        });
    }

    if last_accepted_count == 0 {
        return Ok(());
    }

    let threshold = last_accepted_count / 2;
    match fetched_count >= threshold {
        true => Ok(()),
        false => Err(NotEnoughMarketsError {
            fetched_count,
            last_accepted_count,
            threshold,
        }),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn suspiciously_small_response() {
        assert!(sanity_check(0, 150).is_err());
        assert!(sanity_check(100, 300).is_err());
    }

    #[test]
    fn last_accepted_is_zero() {
        assert!(sanity_check(300, 0).is_ok());
        assert!(sanity_check(1, 0).is_ok());
    }

    #[test]
    fn just_above_threshold() {
        assert!(sanity_check(151, 300).is_ok());
    }

    #[test]
    fn empty_response_with_300_tracked_is_rejected() {
        assert!(sanity_check(0, 300).is_err());
    }

    #[test]
    fn rejects_140_of_300() {
        assert!(sanity_check(140, 300).is_err());
    }
}
