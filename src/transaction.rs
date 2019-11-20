use std::marker::PhantomData;
use std::mem;

use libc;

use ffi;

use cursor::RoCursor;
use database::Database;
use entry::Entry;
use error::{clear_error, result_from_int, result_from_ptr};
use error::{Op, Result};

/// A database transaction.
///
/// All database operations require a transaction.
pub trait Transaction: Sized {
    /// Returns a raw pointer to the underlying btree transaction.
    ///
    /// The caller **must** ensure that the pointer is not used after
    /// the lifetime of the transaction.
    fn txn(&self) -> *mut ffi::btree_txn;

    // fn abort(self);

    /// Commits the transaction.
    ///
    /// Any pending operations will be saved.
    fn commit(self) -> Result<()> {
        unsafe {
            let res = result_from_int(
                ffi::btree_txn_commit(self.txn()),
                Op::TxnCommit,
            );
            mem::forget(self);
            res
        }
    }

    /// Gets an item from a database.
    ///
    /// This function retrieves the data associated with the given key
    /// in the database. If the item is not in the database, then an
    /// error equivalent to the `ErrorKind::NotFound` will be returned.
    fn get<K>(&self, db: &Database, key: &K) -> Result<Vec<u8>>
    where
        K: AsRef<[u8]>,
    {
        let mut keyent = Entry::from_slice(key);
        let mut dataent = Entry::new();
        unsafe {
            clear_error();
            result_from_int(
                ffi::btree_txn_get(
                    db.dbi(),
                    self.txn(),
                    keyent.inner_mut(),
                    dataent.inner_mut(),
                ),
                Op::TxnGet,
            )?;
            Ok(dataent.get_value())
        }
    }

    /// Open a new read-only cursor on the given database.
    fn open_ro_cursor<'txn>(
        &'txn self,
        db: &Database,
    ) -> Result<RoCursor<'txn>> {
        RoCursor::new(self, db)
    }
}

/// A read-only transaction.
pub struct RoTransaction<'db> {
    txn: *mut ffi::btree_txn,
    _marker: PhantomData<&'db ()>,
}

impl<'db> Drop for RoTransaction<'db> {
    fn drop(&mut self) {
        unsafe { ffi::btree_txn_abort(self.txn) }
    }
}

impl<'db> Transaction for RoTransaction<'db> {
    fn txn(&self) -> *mut ffi::btree_txn {
        self.txn
    }
}

impl<'db> RoTransaction<'db> {
    /// Creates a new read-only transaction in the given database.
    pub(crate) fn new(db: &'db Database) -> Result<RoTransaction<'db>> {
        clear_error();
        let txn = unsafe {
            result_from_ptr::<ffi::btree_txn>(
                ffi::btree_txn_begin(db.dbi(), 1),
                Op::TxnBegin,
            )?
        };
        Ok(RoTransaction {
            txn,
            _marker: PhantomData,
        })
    }
}

/// A read-write transaction.
pub struct RwTransaction<'db> {
    txn: *mut ffi::btree_txn,
    _marker: PhantomData<&'db ()>,
}

impl<'db> Drop for RwTransaction<'db> {
    fn drop(&mut self) {
        unsafe { ffi::btree_txn_abort(self.txn) }
    }
}

impl<'db> Transaction for RwTransaction<'db> {
    fn txn(&self) -> *mut ffi::btree_txn {
        self.txn
    }
}

bitflags! {
    #[doc="Write options."]
    #[derive(Default)]
    pub struct WriteFlags: libc::c_uint {
        #[doc="Insert the new item only if the key does not already "]
        #[doc="appear in the database."]
        const NO_OVERWRITE = ffi::BT_NOOVERWRITE;
    }
}

impl<'db> RwTransaction<'db> {
    /// Creates a new read-write transaction in the given database.
    pub(crate) fn new(db: &'db Database) -> Result<RwTransaction<'db>> {
        clear_error();
        let txn = unsafe {
            result_from_ptr::<ffi::btree_txn>(
                ffi::btree_txn_begin(db.dbi(), 0),
                Op::TxnBegin,
            )?
        };
        Ok(RwTransaction {
            txn,
            _marker: PhantomData,
        })
    }

    /// Stores an item into a database.
    pub fn put<K, D>(
        &mut self,
        db: &Database,
        key: &K,
        data: &D,
        flags: WriteFlags,
    ) -> Result<()>
    where
        K: AsRef<[u8]>,
        D: AsRef<[u8]>,
    {
        let mut keyent = Entry::from_slice(key);
        let mut dataent = Entry::from_slice(data);
        unsafe {
            clear_error();
            result_from_int(
                ffi::btree_txn_put(
                    db.dbi(),
                    self.txn(),
                    keyent.inner_mut(),
                    dataent.inner_mut(),
                    flags.bits(),
                ),
                Op::TxnPut,
            )
        }
    }

    /// Deletes an item from a database.
    pub fn del<K>(&mut self, db: &Database, key: &K) -> Result<()>
    where
        K: AsRef<[u8]>,
    {
        let mut keyent = Entry::from_slice(key);
        let mut dataent = Entry::new();
        unsafe {
            clear_error();
            result_from_int(
                ffi::btree_txn_del(
                    db.dbi(),
                    self.txn(),
                    keyent.inner_mut(),
                    dataent.inner_mut(),
                ),
                Op::TxnDel,
            )
        }
    }
}

#[cfg(test)]
mod test {
    use std::sync::{Arc, Barrier};
    use std::thread::{self, JoinHandle};

    use tempdir::TempDir;

    use super::*;
    use error::ErrorKind;

    #[test]
    fn test_put_get_del() {
        let dir = TempDir::new("test").unwrap();
        let dbpath = dir.path().join("test");
        let db = Database::new().open(dbpath.as_path()).unwrap();

        let mut txn = db.begin_rw_txn().unwrap();
        txn.put(&db, b"key1", b"val1", WriteFlags::empty()).unwrap();
        txn.put(&db, b"key2", b"val2", WriteFlags::empty()).unwrap();
        txn.put(&db, b"key3", b"val3", WriteFlags::empty()).unwrap();
        txn.commit().unwrap();

        let mut txn = db.begin_rw_txn().unwrap();
        assert_eq!(b"val1".to_vec(), txn.get(&db, b"key1").unwrap());
        assert_eq!(b"val2".to_vec(), txn.get(&db, b"key2").unwrap());
        assert_eq!(b"val3".to_vec(), txn.get(&db, b"key3").unwrap());
        assert_eq!(txn.get(&db, b"key"), Err(ErrorKind::NotFound.into()));

        txn.del(&db, b"key1").unwrap();
        assert_eq!(txn.get(&db, b"key1"), Err(ErrorKind::NotFound.into()));
    }

    #[test]
    fn test_concurrent_readers_single_writer() {
        let dir = TempDir::new("test").unwrap();
        let dbpath = dir.path().join("test");
        let db: Arc<Database> =
            Arc::new(Database::new().open(dbpath.as_path()).unwrap());

        let n = 10usize; // Number of concurrent readers
        let barrier = Arc::new(Barrier::new(n + 1));
        let mut threads: Vec<JoinHandle<bool>> = Vec::with_capacity(n);

        let key = b"key";
        let val = b"val";

        for _ in 0..n {
            let reader = db.clone();
            let readbar = barrier.clone();

            threads.push(thread::spawn(move || {
                {
                    let txn = reader.begin_ro_txn().unwrap();
                    assert_eq!(
                        txn.get(&reader, key),
                        Err(ErrorKind::NotFound.into())
                    );
                }
                readbar.wait();
                readbar.wait();
                {
                    let txn = reader.begin_ro_txn().unwrap();
                    txn.get(&reader, key).unwrap() == val
                }
            }));
        }

        let mut txn = db.begin_rw_txn().unwrap();
        barrier.wait();
        txn.put(&db, key, val, WriteFlags::empty()).unwrap();
        txn.commit().unwrap();
        barrier.wait();

        assert!(threads.into_iter().all(|b| b.join().unwrap()))
    }
}
