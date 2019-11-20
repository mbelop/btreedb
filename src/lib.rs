#[macro_use]
extern crate bitflags;
extern crate btree as ffi;
extern crate errno;
extern crate libc;

#[cfg(test)]
extern crate tempdir;

pub use cursor::{Cursor, RoCursor};
pub use database::{Database, DatabaseFlags};
pub use error::{Error, ErrorKind, Result};
pub use transaction::{RoTransaction, RwTransaction, Transaction, WriteFlags};

mod cursor;
mod database;
mod entry;
mod error;
mod transaction;
