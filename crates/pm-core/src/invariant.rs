//! Checks of conditions that should never fail in a correct program.
//!
//! The crate's style rule is that functions assert their anomalous conditions and recover
//! explicitly when one fails, rather than panicking. [invariant!](macro@crate::invariant) does
//! both: it logs the failed condition at `error` level and returns early from the function
//! with the value the caller gives, typically an `Err` built from [InvariantViolation].

/// A condition that should always hold did not. Names the condition, for the log and for
/// whoever reads the error.
#[derive(Debug, Clone, Copy, PartialEq, Eq, thiserror::Error)]
#[error("internal invariant violated: {0}")]
pub struct InvariantViolation(pub &'static str);

/// `invariant!(condition, recovery)`: when `condition` is false, logs it and evaluates
/// `return recovery`. The condition must be free of side effects.
///
/// ```
/// use pm_core::{invariant, invariant::InvariantViolation};
///
/// fn halve(n: u32) -> Result<u32, InvariantViolation> {
///     invariant!(n % 2 == 0, Err(InvariantViolation("n is even")));
///     Ok(n / 2)
/// }
/// assert_eq!(halve(4), Ok(2));
/// assert!(halve(3).is_err());
/// ```
#[macro_export]
macro_rules! invariant {
    ($cond:expr, $recover:expr $(,)?) => {
        if !($cond) {
            ::tracing::error!(
                condition = stringify!($cond),
                file = file!(),
                line = line!(),
                "invariant violated"
            );
            return $recover;
        }
    };
}
