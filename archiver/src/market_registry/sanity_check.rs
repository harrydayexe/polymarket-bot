/// Guard against acting on a bad Gamma response.
pub fn sanity_check(fetched_count: u64, last_accepted_count: u64) -> bool {
    if fetched_count == 0 {
        return false;
    }

    if last_accepted_count == 0 {
        return true;
    }

    let threshold = last_accepted_count / 2;
    fetched_count >= threshold
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn suspiciously_small_response() {
        assert!(!sanity_check(0, 150));
        assert!(!sanity_check(100, 300));
    }

    #[test]
    fn last_accepted_is_zero() {
        assert!(sanity_check(300, 0));
        assert!(sanity_check(1, 0));
    }

    #[test]
    fn just_above_threshold() {
        assert!(sanity_check(151, 300));
    }
}
