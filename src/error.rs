use std::error::Error as StdError;
use std::result;
use std::{fmt, io};

use errno;
use libc;

use ffi;

use cursor::Position;

#[derive(Clone, Eq, PartialEq)]
pub(crate) enum Op {
    Compact,          // btree_compact
    CurGet(Position), // btree_cursor_get
    CurOpen,          // btree_txn_cursor_open
    Open,             // btree_open
    Revert,           // btree_revert
    Sync,             // btree_sync
    TxnBegin,         // btree_txn_begin
    TxnCommit,        // btree_txn_commit
    TxnDel,           // btree_txn_del
    TxnGet,           // btree_txn_get
    TxnPut,           // btree_txn_put
    Other(String),
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum ErrorKind {
    AlreadyExists,
    BadHandle,
    Busy,
    InputOutput,
    InvalidArgument,
    NotFound,
    PermissionDenied,
    StaleHandle,
    Other,
}

#[derive(Clone)]
pub struct Error {
    errno: errno::Errno,
    kind: ErrorKind,
    op: Op,
}

// If the error kind is not a wildcard value, that's good enough for us,
// otherwise test errno values regardless of the context if they're set.
// If all fails, resort to comparing operation types.
impl PartialEq for Error {
    fn eq(&self, other: &Error) -> bool {
        if self.kind == ErrorKind::Other || other.kind == ErrorKind::Other {
            if self.errno.0 > 0 && other.errno.0 > 0 {
                self.errno == other.errno
            } else {
                self.op == other.op
            }
        } else {
            self.kind == other.kind
        }
    }
}

impl Eq for Error {}

impl StdError for Error {
    fn description(&self) -> &str {
        match self.op {
            _ if self.kind == ErrorKind::BadHandle => {
                "Failed to perform an operation on a bad \
                 database handle"
            }
            // StaleHandle is returned when a tombstone entry
            // is encontered during a metadata operation.
            _ if self.kind == ErrorKind::StaleHandle => {
                "Failed to perform an operation on a stale \
                 database handle"
            }
            Op::Compact => "Failed to compact the database",
            Op::CurGet(ref position) => match position {
                Position::Current => "Failed to get data at the cursor",
                Position::Exact => "Failed to get data exactly at the cursor",
                Position::First => "Failed to get the first key",
                Position::Next => "Failed to get the next key",
            },
            Op::CurOpen => "Failed to create a new cursor",
            Op::Open => "Failed to open the database",
            Op::Revert => "Failed to revert last change",
            Op::Sync => "Failed to sync the database",
            Op::TxnBegin => "Failed to start a transaction",
            Op::TxnCommit => "Failed to commit a transaction",
            Op::TxnDel => "Failed to delete a key",
            Op::TxnGet => {
                if self.kind == ErrorKind::NotFound {
                    "Key not found in the database"
                } else {
                    "Failed to retrieve value"
                }
            }
            Op::TxnPut => "Failed to store value",
            Op::Other(ref errstr) => errstr.as_str(),
        }
    }
}

impl fmt::Debug for Error {
    fn fmt(&self, fmt: &mut fmt::Formatter) -> fmt::Result {
        write!(fmt, "{}: {}", self.description(), self.errno)
    }
}

impl fmt::Display for Error {
    fn fmt(&self, fmt: &mut fmt::Formatter) -> fmt::Result {
        write!(fmt, "{}", self.description())
    }
}

impl From<Error> for ErrorKind {
    fn from(error: Error) -> Self {
        error.kind
    }
}

impl From<Error> for io::Error {
    fn from(error: Error) -> Self {
        match error.op {
            Op::Other(ref errstr) => {
                io::Error::new(io::ErrorKind::Other, errstr.clone())
            }
            _ => io::Error::from_raw_os_error(error.errno.0),
        }
    }
}

impl Error {
    pub(crate) fn new(op: Op) -> Self {
        let errno = errno::errno();
        let kind = match errno.0 {
            libc::EEXIST => ErrorKind::AlreadyExists,
            libc::EBADF => ErrorKind::BadHandle,
            libc::EBUSY => ErrorKind::Busy,
            libc::EIO => ErrorKind::InputOutput,
            libc::EINVAL => ErrorKind::InvalidArgument,
            libc::ENOENT => ErrorKind::NotFound,
            libc::EPERM => ErrorKind::PermissionDenied,
            libc::ESTALE => ErrorKind::StaleHandle,
            _ => ErrorKind::Other,
        };
        Self { errno, kind, op }
    }

    pub(crate) fn other(errstr: String) -> Self {
        Self {
            errno: errno::Errno(0),
            kind: ErrorKind::Other,
            op: Op::Other(errstr),
        }
    }

    pub fn kind(&self) -> ErrorKind {
        self.kind
    }
}

// This should be used only for the PartialEq situations
impl From<ErrorKind> for Error {
    fn from(kind: ErrorKind) -> Self {
        Self {
            errno: errno::Errno(0),
            kind: kind,
            op: Op::Other("Not an actual error".to_string()),
        }
    }
}

impl PartialEq<ErrorKind> for Error {
    fn eq(&self, other: &ErrorKind) -> bool {
        self.kind == *other
    }
}

pub type Result<T> = result::Result<T, Error>;

pub(crate) fn clear_error() {
    errno::set_errno(errno::Errno(0))
}

pub(crate) fn result_from_int(res: libc::c_int, op: Op) -> Result<()> {
    if res == ffi::BT_SUCCESS {
        return Ok(());
    } else {
        Err(Error::new(op))
    }
}

pub(crate) fn result_from_ptr<T>(res: *mut T, op: Op) -> Result<*mut T> {
    if !res.is_null() {
        Ok(res as *mut T)
    } else {
        Err(Error::new(op))
    }
}
