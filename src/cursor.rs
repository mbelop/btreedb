use std::marker::PhantomData;
use std::{fmt, result};

use database::Database;
use entry::Entry;
use error::{clear_error, result_from_int, result_from_ptr};
use error::{ErrorKind, Op, Result};
use ffi;
use transaction::Transaction;

#[derive(Clone, Debug, Eq, PartialEq)]
pub enum Position {
    Current,
    Exact,
    First,
    Next,
}

impl From<Position> for ffi::cursor_op {
    fn from(pos: Position) -> ffi::cursor_op {
        match pos {
            Position::Current => ffi::BT_CURSOR,
            Position::Exact => ffi::BT_CURSOR_EXACT,
            Position::First => ffi::BT_FIRST,
            Position::Next => ffi::BT_NEXT,
        }
    }
}

/// A database cursor.
pub trait Cursor<'txn> {
    /// Returns a raw pointer to the underlying btree cursor.
    ///
    /// The caller **must** ensure that the pointer is not used after the
    /// lifetime of the cursor.
    fn cursor(&self) -> *mut ffi::cursor;

    /// Retrieves a key/data pair from the cursor. Depending on the cursor
    /// position, the current key may be returned.
    fn get(
        &self,
        key: Option<&[u8]>,
        data: Option<&[u8]>,
        pos: Position,
    ) -> Result<(Option<Vec<u8>>, Vec<u8>)> {
        unsafe {
            let mut keyent =
                key.map_or(Entry::new(), |ref key| Entry::from_slice(key));
            let keyptr = keyent.as_ptr();
            let mut dataent =
                data.map_or(Entry::new(), |ref data| Entry::from_slice(data));
            clear_error();
            result_from_int(
                ffi::btree_cursor_get(
                    self.cursor(),
                    keyent.inner_mut(),
                    dataent.inner_mut(),
                    pos.clone().into(),
                ),
                Op::CurGet(pos),
            )?;
            let keyout = if keyptr != keyent.as_ptr() {
                Some(keyent.get_value())
            } else {
                None
            };
            Ok((keyout, dataent.get_value()))
        }
    }

    /// Iterate over database items. The iterator will begin with
    /// item next after the cursor, and continue until the end of
    /// the database. For new cursors, the iterator will begin with
    /// the first item in the database.
    fn iter(&mut self) -> Iter<'txn> {
        Iter::new(self.cursor(), Position::Next, Position::Next, None)
    }

    /// Iterate over database items starting from the beginning of
    /// the database.
    fn iter_start(&mut self) -> Iter<'txn> {
        Iter::new(self.cursor(), Position::First, Position::Next, None)
    }

    /// Iterate over database items starting from the given key.
    fn iter_from<K>(&mut self, key: K) -> Iter<'txn>
    where
        K: AsRef<[u8]>,
    {
        if let Err(err) = self.get(Some(key.as_ref()), None, Position::Exact) {
            if err.kind() != ErrorKind::NotFound {
                panic!("unexpected error when seeking: {}", err)
            }
        }
        Iter::new(
            self.cursor(),
            Position::Current,
            Position::Next,
            Some(key.as_ref().to_vec()),
        )
    }
}

/// A read-only cursor for navigating the items within a database.
pub struct RoCursor<'txn> {
    cursor: *mut ffi::cursor,
    _marker: PhantomData<fn() -> &'txn ()>,
}

impl<'txn> Cursor<'txn> for RoCursor<'txn> {
    fn cursor(&self) -> *mut ffi::cursor {
        self.cursor
    }
}

impl<'txn> fmt::Debug for RoCursor<'txn> {
    fn fmt(&self, f: &mut fmt::Formatter) -> result::Result<(), fmt::Error> {
        f.debug_struct("RoCursor").finish()
    }
}

impl<'txn> Drop for RoCursor<'txn> {
    fn drop(&mut self) {
        unsafe { ffi::btree_cursor_close(self.cursor) }
    }
}

impl<'txn> RoCursor<'txn> {
    /// Creates a new read-only cursor in the given database and
    /// transaction. Prefer using `Transaction::open_ro_cursor`.
    pub(crate) fn new<T>(txn: &'txn T, db: &Database) -> Result<RoCursor<'txn>>
    where
        T: Transaction,
    {
        let cursor = unsafe {
            clear_error();
            result_from_ptr::<ffi::cursor>(
                ffi::btree_txn_cursor_open(db.dbi(), txn.txn()),
                Op::CurOpen,
            )?
        };
        Ok(RoCursor {
            cursor: cursor,
            _marker: PhantomData,
        })
    }
}

/// An iterator over the values in an btree database.
pub struct Iter<'txn> {
    cursor: *mut ffi::cursor,
    from: Option<Vec<u8>>,
    curr: Position,
    next: Position,
    _marker: PhantomData<fn(&'txn ())>,
}

impl<'txn> Iter<'txn> {
    /// Creates a new iterator backed by the given cursor.
    fn new<'t>(
        cursor: *mut ffi::cursor,
        curr: Position,
        next: Position,
        from: Option<Vec<u8>>,
    ) -> Iter<'t> {
        Iter {
            cursor,
            from,
            curr,
            next,
            _marker: PhantomData,
        }
    }
}

impl<'txn> fmt::Debug for Iter<'txn> {
    fn fmt(&self, f: &mut fmt::Formatter) -> result::Result<(), fmt::Error> {
        f.debug_struct("Iter").finish()
    }
}

impl<'txn> Iterator for Iter<'txn> {
    type Item = (Vec<u8>, Vec<u8>);

    fn next(&mut self) -> Option<(Vec<u8>, Vec<u8>)> {
        let from = self.from.take();
        let mut keyent = match from {
            Some(ref key) => Entry::from_slice(key),
            None => Entry::new(),
        };
        let mut dataent = Entry::new();
        let curr = self.curr.clone();
        self.curr = self.next.clone();
        unsafe {
            clear_error();
            if let Err(err) = result_from_int(
                ffi::btree_cursor_get(
                    self.cursor,
                    keyent.inner_mut(),
                    dataent.inner_mut(),
                    curr.clone().into(),
                ),
                Op::CurGet(curr),
            ) {
                match err.kind() {
                    // EINVAL can occur when the cursor was
                    // previously seeked to a non-existent
                    // value, e.g. iter_from with a key
                    // greater than all values in the
                    // database.
                    ErrorKind::InvalidArgument | ErrorKind::NotFound => {
                        return None
                    }
                    _ => panic!(
                        "btree_cursor_get returned an unexpected error: {}",
                        err
                    ),
                }
            }
            Some((keyent.get_value(), dataent.get_value()))
        }
    }
}

#[cfg(test)]
mod test {
    use tempdir::TempDir;

    use cursor::Position;
    use database::Database;
    use transaction::WriteFlags;

    use super::*;

    #[test]
    fn test_get() {
        let dir = TempDir::new("test").unwrap();
        let dbpath = dir.path().join("test");
        let db = Database::new().open(dbpath.as_path()).unwrap();

        let mut txn = db.begin_rw_txn().unwrap();
        txn.put(&db, b"key1", b"val1", WriteFlags::empty()).unwrap();
        txn.put(&db, b"key2", b"val2", WriteFlags::empty()).unwrap();
        txn.put(&db, b"key3", b"val3", WriteFlags::empty()).unwrap();

        let cursor = txn.open_ro_cursor(&db).unwrap();
        assert_eq!(
            (Some(b"key1".to_vec()), b"val1".to_vec()),
            cursor.get(None, None, Position::First).unwrap()
        );
        assert_eq!(
            (Some(b"key1".to_vec()), b"val1".to_vec()),
            cursor
                .get(Some(&b"key1"[..]), None, Position::Current)
                .unwrap()
        );
        assert_eq!(
            (Some(b"key1".to_vec()), b"val1".to_vec()),
            cursor
                .get(Some(&b"key1"[..]), None, Position::Exact)
                .unwrap()
        );
        assert_eq!(
            (Some(b"key2".to_vec()), b"val2".to_vec()),
            cursor.get(None, None, Position::Next).unwrap()
        );
    }

    #[test]
    fn test_iter() {
        let dir = TempDir::new("test").unwrap();
        let dbpath = dir.path().join("test");
        let db = Database::new().open(dbpath.as_path()).unwrap();

        let items: Vec<(Vec<u8>, Vec<u8>)> = vec![
            (b"key1".to_vec(), b"val1".to_vec()),
            (b"key2".to_vec(), b"val2".to_vec()),
            (b"key3".to_vec(), b"val3".to_vec()),
            (b"key5".to_vec(), b"val5".to_vec()),
        ];

        {
            let mut txn = db.begin_rw_txn().unwrap();
            for &(ref key, ref data) in &items {
                txn.put(&db, key, data, WriteFlags::empty()).unwrap();
            }
            txn.commit().unwrap();
        }

        let txn = db.begin_ro_txn().unwrap();
        let mut cursor = txn.open_ro_cursor(&db).unwrap();
        assert_eq!(items, cursor.iter().collect::<Vec<_>>());

        cursor.get(Some(b"key2"), None, Position::Current).unwrap();
        assert_eq!(
            items.clone().into_iter().skip(2).collect::<Vec<_>>(),
            cursor.iter().collect::<Vec<_>>()
        );

        assert_eq!(items, cursor.iter_start().collect::<Vec<_>>());

        assert_eq!(
            items.clone().into_iter().skip(1).collect::<Vec<_>>(),
            cursor.iter_from(b"key2").collect::<Vec<_>>()
        );

        assert_eq!(
            items.clone().into_iter().skip(3).collect::<Vec<_>>(),
            cursor.iter_from(b"key4").collect::<Vec<_>>()
        );

        assert_eq!(
            vec![].into_iter().collect::<Vec<(Vec<u8>, Vec<u8>)>>(),
            cursor.iter_from(b"key6").collect::<Vec<_>>()
        );
    }

    #[test]
    fn test_iter_empty_database() {
        let dir = TempDir::new("test").unwrap();
        let dbpath = dir.path().join("test");
        let db = Database::new().open(dbpath.as_path()).unwrap();
        let txn = db.begin_ro_txn().unwrap();
        let mut cursor = txn.open_ro_cursor(&db).unwrap();

        assert_eq!(0, cursor.iter().count());
        assert_eq!(0, cursor.iter_start().count());
        assert_eq!(0, cursor.iter_from(b"foo").count());
    }

    fn match_below<P>(pat: P) -> impl Fn(&[u8]) -> bool
    where
        P: AsRef<[u8]>,
    {
        move |key: &[u8]| {
            key.len() >= pat.as_ref().len()
                && key[..pat.as_ref().len()] == pat.as_ref()[..]
        }
    }

    fn lookup_filter<F, I>(iter: I, filter: F) -> Vec<(Vec<u8>, Vec<u8>)>
    where
        F: Fn(&[u8]) -> bool,
        I: Iterator<Item = (Vec<u8>, Vec<u8>)>,
    {
        iter.filter(|(key, _)| filter(key))
            .map(|(key, val)| (key.to_vec(), val.to_vec()))
            .collect::<Vec<_>>()
    }

    #[test]
    fn test_iter_count_keys() {
        let dir = TempDir::new("test").unwrap();
        let dbpath = dir.path().join("test");
        let db = Database::new().open(dbpath.as_path()).unwrap();
        let entries = 1000;

        {
            let mut txn = db.begin_rw_txn().unwrap();
            for i in 0..entries {
                let key = format!("/r/{}", i);
                let val = format!("{}", i);
                txn.put(&db, &key, &val, WriteFlags::empty()).unwrap();
            }
            txn.commit().unwrap();
        }

        let mut counter: usize = 0;
        {
            let txn = db.begin_ro_txn().unwrap();
            let mut cur = txn.open_ro_cursor(&db).unwrap();
            for (..) in cur.iter() {
                counter += 1;
            }
        }

        assert_eq!(counter, entries);
    }

    #[test]
    fn test_iter_collect_keys() {
        let dir = TempDir::new("test").unwrap();
        let dbpath = dir.path().join("test");
        let db = Database::new().open(dbpath.as_path()).unwrap();
        let entries = 1000;

        {
            let mut txn = db.begin_rw_txn().unwrap();
            for i in 0..entries {
                let key = format!("/r/{}", i);
                let val = format!("{}", i);
                txn.put(&db, &key, &val, WriteFlags::empty()).unwrap();
            }
            txn.commit().unwrap();
        }

        let nkeys = {
            let txn = db.begin_ro_txn().unwrap();
            let mut cur = txn.open_ro_cursor(&db).unwrap();
            let iter = cur.iter();
            lookup_filter(iter, match_below(&"/r/"))
        };

        assert_eq!(nkeys.len(), entries);
    }
}
