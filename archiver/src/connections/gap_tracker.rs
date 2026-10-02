// use std::collections::{HashMap, HashSet};
//
// use crate::types::{ControlBody, GapReason, TokenId, UtcMicros};
//
// /// An object describing an open gap in data.
// #[derive(Debug, Clone)]
// pub struct OpenGap {
//     start: UtcMicros,
//     reason: GapReason,
// }
//
// #[derive(Debug)]
// pub struct GapTracker {
//     open_gaps: HashMap<TokenId, OpenGap>,
// }
//
// impl GapTracker {
//     /// Open a gap for a set of tokens at a given time, for a given reason.
//     ///
//     /// # Returns
//     ///
//     /// `Some(ControlBody::GapClose)` only if the specified tokens are not already part of an open
//     /// gap.
//     pub fn open(
//         &mut self,
//         tokens: HashSet<TokenId>,
//         start: UtcMicros,
//         reason: GapReason,
//     ) -> Option<ControlBody> {
//         let missing: Vec<TokenId> = tokens
//             .iter()
//             .filter(|id| !self.open_gaps.contains_key(id))
//             .cloned()
//             .collect();
//
//         if missing.is_empty() {
//             return None;
//         }
//
//         for token in &missing {
//             self.open_gaps
//                 .insert(token.clone(), OpenGap { start, reason });
//         }
//
//         Some(ControlBody::GapOpen {
//             tokens: missing.into_iter().collect(),
//             start,
//             reason,
//         })
//     }
//
//     /// Close a gap for a set of tokens at a given time, for a given reason.
//     ///
//     /// # Returns
//     ///
//     /// `Some(ControlBody::GapClose)` only if the specified tokens are already part of an open gap.
//     pub fn close(
//         &mut self,
//         tokens: HashSet<TokenId>,
//         end: UtcMicros,
//         reason: GapReason,
//     ) -> Option<ControlBody> {
//         let found: HashSet<TokenId> = tokens
//             .into_iter()
//             .filter(|id| self.open_gaps.remove(id).is_some())
//             .collect();
//
//         if found.is_empty() {
//             return None;
//         }
//
//         Some(ControlBody::GapClose {
//             tokens: found,
//             end,
//             reason,
//         })
//     }
//
//     /// Return the number of tokens currently being tracked as in a gap.
//     pub fn open_count(&self) -> usize {
//         self.open_gaps.len()
//     }
// }
//
// #[cfg(test)]
// mod tests {
//     use super::*;
//
//     const SEC: i64 = 1_000_000;
//
//     /// Seconds after 10:00:00.
//     fn t(secs: i64) -> UtcMicros {
//         UtcMicros(secs * SEC)
//     }
//
//     fn token(n: u32) -> TokenId {
//         TokenId(format!("token-{n}"))
//     }
//
//     fn set(ids: &[u32]) -> HashSet<TokenId> {
//         ids.iter().map(|&n| token(n)).collect()
//     }
//
//     fn tracker() -> GapTracker {
//         GapTracker {
//             open_gaps: HashMap::new(),
//         }
//     }
//
//     #[test]
//     fn simple_gap() {
//         let mut tr = tracker();
//
//         let open = tr.open(set(&[1]), t(0), GapReason::ConnectionClosed);
//         let Some(ControlBody::GapOpen { tokens, start, .. }) = open else {
//             panic!("expected GapOpen, got {open:?}");
//         };
//         assert_eq!(tokens, set(&[1]));
//         assert_eq!(tr.open_count(), 1);
//
//         let close = tr.close(set(&[1]), t(7), GapReason::ConnectionClosed);
//         let Some(ControlBody::GapClose { tokens, end, .. }) = close else {
//             panic!("expected GapClose, got {close:?}");
//         };
//         assert_eq!(tokens, set(&[1]));
//         assert_eq!(tr.open_count(), 0);
//
//         // One matched pair, 7 s apart.
//         assert_eq!(end.0 - start.0, 7 * SEC);
//     }
//
//     #[test]
//     fn gap_uses_supplied_start_time() {
//         // Caller passes the last-message time (10:00:00), not the
//         // time the ConnectionClosed was noticed (10:00:30).
//         let mut tr = tracker();
//         let open = tr.open(set(&[1]), t(0), GapReason::ConnectionClosed);
//
//         let Some(ControlBody::GapOpen { start, .. }) = open else {
//             panic!("expected GapOpen");
//         };
//         assert_eq!(start, t(0));
//         assert_eq!(tr.open_gaps[&token(1)].start, t(0));
//     }
//
//     #[test]
//     fn double_open_keeps_earlier_start() {
//         let mut tr = tracker();
//         assert!(
//             tr.open(set(&[1]), t(0), GapReason::ConnectionClosed)
//                 .is_some()
//         );
//
//         let second = tr.open(set(&[1]), t(5), GapReason::ConnectionClosed);
//
//         assert!(second.is_none());
//         assert_eq!(tr.open_count(), 1);
//         assert_eq!(tr.open_gaps[&token(1)].start, t(0));
//     }
//
//     #[test]
//     fn open_only_reports_newly_opened_tokens() {
//         let mut tr = tracker();
//         tr.open(set(&[1]), t(0), GapReason::ConnectionClosed);
//
//         let open = tr.open(set(&[1, 2]), t(5), GapReason::ConnectionClosed);
//
//         let Some(ControlBody::GapOpen { tokens, start, .. }) = open else {
//             panic!("expected GapOpen");
//         };
//         assert_eq!(tokens, set(&[2]));
//         assert_eq!(start, t(5));
//         assert_eq!(tr.open_count(), 2);
//         assert_eq!(tr.open_gaps[&token(1)].start, t(0));
//     }
//
//     #[test]
//     fn close_without_open_is_ignored() {
//         let mut tr = tracker();
//
//         let close = tr.close(set(&[1]), t(7), GapReason::ConnectionClosed);
//
//         assert!(close.is_none());
//         assert_eq!(tr.open_count(), 0);
//     }
//
//     #[test]
//     fn close_only_reports_tokens_that_were_open() {
//         let mut tr = tracker();
//         tr.open(set(&[1]), t(0), GapReason::ConnectionClosed);
//
//         let close = tr.close(set(&[1, 2]), t(7), GapReason::ConnectionClosed);
//
//         let Some(ControlBody::GapClose { tokens, .. }) = close else {
//             panic!("expected GapClose");
//         };
//         assert_eq!(tokens, set(&[1]));
//         assert_eq!(tr.open_count(), 0);
//     }
//
//     #[test]
//     fn shutdown_closes_all_open_gaps_with_shutdown_reason() {
//         let mut tr = tracker();
//         tr.open(set(&[1, 2, 3]), t(0), GapReason::ConnectionClosed);
//
//         // Shutdown path: close everything currently open.
//         let all: HashSet<TokenId> = tr.open_gaps.keys().cloned().collect();
//         let close = tr.close(all, t(10), GapReason::Shutdown);
//
//         let Some(ControlBody::GapClose { tokens, reason, .. }) = close else {
//             panic!("expected GapClose");
//         };
//         assert_eq!(tokens, set(&[1, 2, 3]));
//         assert!(matches!(reason, GapReason::Shutdown));
//         assert_eq!(tr.open_count(), 0);
//     }
// }
