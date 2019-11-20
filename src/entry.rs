use ffi;
use libc;
use std::{ptr, slice};

pub(crate) struct Entry {
    btval: ffi::btval,
}

impl Drop for Entry {
    fn drop(&mut self) {
        unsafe {
            ffi::btval_reset(&mut self.btval);
        }
    }
}

impl Entry {
    pub fn new() -> Self {
        Self {
            btval: ffi::btval {
                data: ptr::null_mut(),
                size: 0,
                free_data: 0,
                mp: ptr::null_mut(),
            },
        }
    }

    pub fn from_slice<D>(data: &D) -> Self
    where
        D: AsRef<[u8]>,
    {
        let data = data.as_ref();
        Self {
            btval: ffi::btval {
                data: data.as_ptr() as *mut libc::c_void,
                size: data.len() as libc::size_t,
                free_data: 0,
                mp: ptr::null_mut(),
            },
        }
    }

    #[allow(dead_code)]
    pub fn value<D>(&mut self, data: &D) -> &mut Self
    where
        D: AsRef<[u8]>,
    {
        let data = data.as_ref();
        self.btval.data = data.as_ptr() as *mut libc::c_void;
        self.btval.size = data.len() as libc::size_t;
        self
    }

    pub fn get_value(&self) -> Vec<u8> {
        let s = unsafe {
            slice::from_raw_parts(
                self.btval.data as *const u8,
                self.btval.size as usize,
            )
        };
        s.to_vec()
    }

    pub fn as_ptr(&self) -> *const u8 {
        self.btval.data as *const u8
    }

    pub fn inner_mut(&mut self) -> &mut ffi::btval {
        &mut self.btval
    }
}
