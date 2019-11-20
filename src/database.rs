use std::ffi::CString;
#[cfg(windows)]
use std::ffi::OsStr;
#[cfg(unix)]
use std::os::unix::ffi::OsStrExt;
use std::path::{Path, PathBuf};

use libc;

use error::{clear_error, result_from_int, result_from_ptr};
use error::{Error, Op, Result};
use ffi;
use transaction::{RoTransaction, RwTransaction};

/// An append-only database.
pub struct Database {
    handle: *mut ffi::btree,
    builder: DatabaseBuilder,
}

impl Database {
    /// Creates a new builder for specifying options for opening a database.
    pub fn new() -> DatabaseBuilder {
        DatabaseBuilder {
            flags: DatabaseFlags::empty(),
            cache_size: 0,
            path: PathBuf::new(),
            mode: 0o644,
        }
    }

    /// Returns the underlying btree database handle.
    ///
    /// The caller **must** ensure that the handle is not used after the
    /// lifetime of the database, or after the database has been closed.
    pub fn dbi(&self) -> *mut ffi::btree {
        self.handle
    }

    /// Create a read-only transaction for use with the database.
    pub fn begin_ro_txn<'db>(&'db self) -> Result<RoTransaction<'db>> {
        RoTransaction::new(self)
    }

    /// Create a read-write transaction for use with the database.
    /// This method will error out while there are any other read-write
    /// transactions open on the database.
    pub fn begin_rw_txn<'db>(&'db self) -> Result<RwTransaction<'db>> {
        RwTransaction::new(self)
    }

    pub fn reopen(&mut self) -> Result<()> {
        clear_error();
        unsafe {
            ffi::btree_close(self.handle);
        }
        let mut builder = self.builder.clone();
        let mut newdb = builder.reopen()?;
        self.handle = newdb.handle;
        newdb.handle = ::std::ptr::null_mut();
        Ok(())
    }

    /// Revert last transaction.
    pub fn revert(&self) -> Result<()> {
        clear_error();
        unsafe { result_from_int(ffi::btree_revert(self.handle), Op::Revert) }
    }

    /// Compact the database.
    ///
    /// When compaction of a database file is complete, a special marker
    /// is appended to the database file that requires the calling program
    /// to reopen the file and perform new requests against the compacted
    /// database.
    pub fn compact(&self) -> Result<()> {
        clear_error();
        unsafe { result_from_int(ffi::btree_compact(self.handle), Op::Compact) }
    }

    /// Flush data buffers to disk.
    ///
    /// Data is always written to disk when `Transaction::commit` is called,
    /// but the operating system may keep it buffered. btree always flushes
    /// the OS buffers upon commit as well, unless the database was opened
    /// with `NO_SYNC`.
    pub fn sync(&self) -> Result<()> {
        clear_error();
        unsafe { result_from_int(ffi::btree_sync(self.handle), Op::Sync) }
    }

    /// Closes the database handle. Normally unnecessary.
    ///
    /// Databases should only be closed by a single thread, and only if no
    /// other threads are going to reference the database handle or one of
    /// its cursors any further. Do not close a handle if an existing
    /// transaction has modified its database. Doing so can cause database
    /// corruption or other errors.
    pub fn close(self) {
        clear_error();
        unsafe {
            ffi::btree_close(self.handle);
        }
    }
}

impl Drop for Database {
    fn drop(&mut self) {
        clear_error();
        unsafe {
            ffi::btree_close(self.handle);
        }
    }
}

unsafe impl Sync for Database {}
unsafe impl Send for Database {}

bitflags! {
    #[doc="Database options."]
    #[derive(Default)]
    pub struct DatabaseFlags: libc::c_uint {
        #[doc="Don't flush system buffers to disk when committing"]
        #[doc="a transaction."]
        #[doc="\n\n"]
        #[doc="This optimization means a system crash can corrupt"]
        #[doc="the database or lose the last transactions if buffers"]
        #[doc="are not yet flushed to disk. The risk is governed by"]
        #[doc="how often the system flushes dirty buffers to disk"]
        #[doc="and how often `Database::sync` is called."]
        const NO_SYNC = ffi::BT_NOSYNC;

        #[doc="Open the database in read-only mode. No write"]
        #[doc="operations will be allowed."]
        const READ_ONLY = ffi::BT_RDONLY;

        #[doc="Keys are strings to be compared in reverse order,"]
        #[doc="from the end of the strings to the beginning."]
        #[doc="\n\n"]
        #[doc="By default, keys are treated as strings and compared"]
        #[doc="from the beginning to the end."]
        const REVERSE_KEY = ffi::BT_REVERSEKEY;
    }
}

#[derive(Clone)]
pub struct DatabaseBuilder {
    flags: DatabaseFlags,
    cache_size: u32,
    path: PathBuf,
    mode: u32,
}

impl DatabaseBuilder {
    /// Open an existing database or create a new one.
    ///
    /// On UNIX, the database files will be opened with 644 permissions.
    pub fn open(&mut self, path: &Path) -> Result<Database> {
        self.open_with_permissions(path, 0o644)
    }

    /// Open an existing database or create a new one with the provided
    /// UNIX permissions.
    pub fn open_with_permissions(
        &mut self,
        path: &Path,
        mode: u32,
    ) -> Result<Database> {
        self.path = path.to_path_buf();
        self.mode = mode;

        let path = match CString::new(path.as_os_str().as_bytes()) {
            Ok(path) => path,
            Err(..) => {
                let mut errstr = String::from("Invalid path");
                if let Some(path) = path.to_str() {
                    errstr += &format!(": {}", path);
                }
                return Err(Error::other(errstr));
            }
        };

        clear_error();
        let dbi = unsafe {
            result_from_ptr::<ffi::btree>(
                ffi::btree_open(
                    path.as_ptr(),
                    self.flags.bits(),
                    mode as libc::mode_t,
                ),
                Op::Open,
            )?
        };

        if self.cache_size > 0 {
            unsafe {
                ffi::btree_set_cache_size(dbi, self.cache_size);
            }
        }

        Ok(Database {
            handle: dbi,
            builder: self.clone(),
        })
    }

    pub(crate) fn reopen(&mut self) -> Result<Database> {
        let pathbuf = self.path.clone();
        let mode = self.mode;
        self.open_with_permissions(pathbuf.as_path(), mode)
    }

    /// Sets the provided options for the database.
    pub fn set_flags(&mut self, flags: DatabaseFlags) -> &mut Self {
        self.flags = flags;
        self
    }

    /// Set the cache size for database entries.
    ///
    /// The size is specified in number of pages.  Note that more than the
    /// configured number of pages may exist in the cache, as dirty pages
    /// and pages referenced by cursors are excluded from cache expiration.
    /// Cached pages are expired in a least recently used (LRU) order.
    pub fn set_cache_size(&mut self, cache_size: u32) -> &mut Self {
        self.cache_size = cache_size;
        self
    }
}

#[cfg(test)]
mod test {
    use tempdir::TempDir;

    use super::*;
    use error::ErrorKind;
    use transaction::{Transaction, WriteFlags};

    #[test]
    fn test_open() {
        let dir = TempDir::new("test").unwrap();
        let dbpath = dir.path().join("test");

        assert!(Database::new().open(&dbpath).is_ok());
    }

    #[test]
    fn test_ro_txn() {
        let dir = TempDir::new("test").unwrap();
        let dbpath = dir.path().join("test");
        let db = Database::new().open(&dbpath).unwrap();
        let txn = db.begin_ro_txn().unwrap();
        let res = txn.get(&db, &"/non-existant".to_string());
        assert!(res.is_err());
        if let Err(err) = res {
            assert_eq!(err.kind(), ErrorKind::NotFound);
        }
    }

    #[test]
    fn test_rw_txn() {
        let dir = TempDir::new("test").unwrap();
        let dbpath = dir.path().join("test");
        let db = Database::new().open(&dbpath).unwrap();
        let mut rwtxn = db.begin_rw_txn().unwrap();
        let res = rwtxn.put(
            &db,
            &"/test-key".to_string(),
            &"Some data".to_string(),
            WriteFlags::empty(),
        );
        assert!(res.is_ok());
        assert!(rwtxn.commit().is_ok());
        let rotxn = db.begin_ro_txn().unwrap();
        let res = rotxn.get(&db, &"/test-key".to_string());
        assert!(res.is_ok());
        if let Ok(data) = res {
            assert_eq!(data, "Some data".as_bytes());
        }
    }

    #[test]
    fn test_multi_rw_txn() {
        let dir = TempDir::new("test").unwrap();
        let dbpath = dir.path().join("test");
        let db = Database::new().open(&dbpath).unwrap();
        for i in 0..1000 {
            let mut rwtxn = db.begin_rw_txn().unwrap();
            assert!(rwtxn
                .put(
                    &db,
                    &format!("/test-key-{}", i),
                    &"Some data".to_string(),
                    WriteFlags::empty(),
                )
                .is_ok());
            assert!(rwtxn.commit().is_ok());
        }
        for i in 0..1000 {
            let rotxn = db.begin_ro_txn().unwrap();
            assert!(rotxn.get(&db, &format!("/test-key-{}", i)).is_ok());
        }
    }

    #[test]
    fn test_begin_txn() {
        let dir = TempDir::new("test").unwrap();
        let dbpath = dir.path().join("test");

        {
            // writable database
            let db = Database::new().open(dbpath.as_path()).unwrap();

            assert!(db.begin_rw_txn().is_ok());
            assert!(db.begin_ro_txn().is_ok());
        }

        {
            // read-only database
            let db = Database::new()
                .set_flags(DatabaseFlags::READ_ONLY)
                .open(dbpath.as_path())
                .unwrap();

            // XXX: btree doesn't error out here.
            //assert!(db.begin_rw_txn().is_err());
            assert!(db.begin_ro_txn().is_ok());
        }
    }

    #[test]
    fn test_close_database() {
        let dir = TempDir::new("test").unwrap();
        let dbpath = dir.path().join("test");
        let db = Database::new().open(dbpath.as_path()).unwrap();
        db.close();
        assert!(Database::new().open(dbpath.as_path()).is_ok());
    }

    #[test]
    fn test_sync() {
        let dir = TempDir::new("test").unwrap();
        let dbpath = dir.path().join("test");
        {
            let db = Database::new().open(dbpath.as_path()).unwrap();
            assert!(db.sync().is_ok());
        }
        {
            let db = Database::new()
                .set_flags(DatabaseFlags::READ_ONLY)
                .open(dbpath.as_path())
                .unwrap();
            // XXX: btree doesn't error out here.
            assert!(db.sync().is_ok());
        }
    }

    #[test]
    fn test_reopen() {
        let dir = TempDir::new("test").unwrap();
        let dbpath = dir.path().join("test");
        let mut db = Database::new().open(dbpath.as_path()).unwrap();
        {
            assert!(db.begin_rw_txn().is_ok());
        }
        assert!(db.reopen().is_ok());
        {
            assert!(db.begin_rw_txn().is_ok());
        }
    }

    #[test]
    fn test_compact() {
        let dir = TempDir::new("test").unwrap();
        let dbpath = dir.path().join("test");
        let mut db = Database::new().open(dbpath.as_path()).unwrap();
        {
            assert!(db.begin_rw_txn().is_ok());
        }
        assert!(db.compact().is_ok());
        {
            match db.begin_ro_txn() {
                Ok(_) => panic!("begin_rw_txn succeeded after compact"),
                Err(err) => match err.kind() {
                    ErrorKind::StaleHandle => (),
                    _ => panic!("begin_rw_txn failed with: {}", err),
                },
            }
        }
        {
            match db.begin_rw_txn() {
                Ok(_) => panic!("begin_rw_txn succeeded after compact"),
                Err(err) => match err.kind() {
                    ErrorKind::StaleHandle => (),
                    _ => panic!("begin_rw_txn failed with: {}", err),
                },
            }
        }
        assert!(db.reopen().is_ok());
        {
            assert!(db.begin_rw_txn().is_ok());
        }
    }
}
