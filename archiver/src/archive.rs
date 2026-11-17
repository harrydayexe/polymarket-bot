//! The archive on disk: folder layout (section 8.3), the line format of every record and a
//! reader for the offline commands.

pub mod compress;

// The reading side of the archive and its format are shared, in pm-core.
pub use pm_core::archive::{envelope, layout, reader, snapshot_book};
