use std::{
    collections::{HashMap, HashSet},
    time::Duration,
};

use marcasite::types::TokenId;
use tokio::time::Instant;

/// When each tracked token is next due a scheduled snapshot.
#[derive(Debug)]
pub struct Schedule {
    interval: Duration,
    due: HashMap<TokenId, Instant>,
}

impl Schedule {
    pub fn new(interval: Duration) -> Self {
        Self {
            interval,
            due: HashMap::new(),
        }
    }

    /// Starts scheduling `tokens`, spreading the newly tracked ones evenly over the next
    /// interval so their snapshots don't bunch up. The first one is due a fraction of an
    /// interval from now, the last a whole interval from now: each has just had a snapshot
    /// for its subscription. Tokens already scheduled keep their times.
    pub fn add(&mut self, tokens: &HashSet<TokenId>, now: Instant) {
        let mut new: Vec<&TokenId> = tokens
            .iter()
            .filter(|token| !self.due.contains_key(*token))
            .collect();
        new.sort();
        let count = new.len() as u32;
        for (i, token) in (1..).zip(new) {
            self.due
                .insert(token.clone(), now + self.interval * i / count);
        }
    }

    /// Stops scheduling `tokens`.
    pub fn remove(&mut self, tokens: &HashSet<TokenId>) {
        self.due.retain(|token, _| !tokens.contains(token));
    }

    /// The tokens due at `now`, earliest first, each rescheduled one interval after the time it
    /// was due (or one interval from now, if it has fallen a whole interval behind).
    pub fn take_due(&mut self, now: Instant) -> Vec<TokenId> {
        let mut due: Vec<(Instant, TokenId)> = Vec::new();
        for (token, at) in &mut self.due {
            if *at <= now {
                due.push((*at, token.clone()));
                *at += self.interval;
                if *at <= now {
                    *at = now + self.interval;
                }
            }
        }
        due.sort();
        due.into_iter().map(|(_, token)| token).collect()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    const HOUR: Duration = Duration::from_secs(3600);
    const MINUTE: Duration = Duration::from_secs(60);

    fn token(n: usize) -> TokenId {
        TokenId::new(format!("t{n:05}"))
    }

    fn tokens(ns: impl IntoIterator<Item = usize>) -> HashSet<TokenId> {
        ns.into_iter().map(token).collect()
    }

    #[test]
    fn spreads_tokens_evenly_over_the_hour() {
        let t0 = Instant::now();
        let mut schedule = Schedule::new(HOUR);
        schedule.add(&tokens(0..3600), t0);

        assert!(schedule.take_due(t0).is_empty());
        for minute in 1..=60 {
            let due = schedule.take_due(t0 + MINUTE * minute);
            assert_eq!(due.len(), 60, "minute {minute}");
        }

        // Each token once an hour: the first minute's tokens come round again.
        let again = schedule.take_due(t0 + HOUR + MINUTE);
        assert_eq!(again, (0..60).map(token).collect::<Vec<_>>());
    }

    #[test]
    fn already_scheduled_tokens_keep_their_times() {
        let t0 = Instant::now();
        let mut schedule = Schedule::new(HOUR);
        schedule.add(&tokens([1]), t0);
        schedule.add(&tokens([1, 2]), t0 + MINUTE * 30);

        // Token 1 is still due an hour after it was added.
        assert!(schedule.take_due(t0 + MINUTE * 59).is_empty());
        assert_eq!(schedule.take_due(t0 + HOUR), vec![token(1)]);
        assert_eq!(schedule.take_due(t0 + MINUTE * 90), vec![token(2)]);
    }

    #[test]
    fn removed_tokens_are_no_longer_due() {
        let t0 = Instant::now();
        let mut schedule = Schedule::new(HOUR);
        schedule.add(&tokens([1, 2]), t0);
        schedule.remove(&tokens([1]));

        assert_eq!(schedule.take_due(t0 + HOUR), vec![token(2)]);
    }

    #[test]
    fn a_token_far_behind_is_due_once_then_an_interval_later() {
        let t0 = Instant::now();
        let mut schedule = Schedule::new(HOUR);
        schedule.add(&tokens([1]), t0);

        let late = t0 + HOUR * 5;
        assert_eq!(schedule.take_due(late), vec![token(1)]);
        assert!(schedule.take_due(late + HOUR - MINUTE).is_empty());
        assert_eq!(schedule.take_due(late + HOUR), vec![token(1)]);
    }
}
