//! The archive on disk: folder layout (section 8.3), the line format of every record and a
//! reader for the offline commands.
//!
//! Writing (rotation, durability, compression) stays in the archiver; this module holds what
//! a reader of the archive needs, and the record types the format is defined by.

pub mod envelope;
pub mod layout;
pub mod reader;
pub mod record;
pub mod snapshot_book;
