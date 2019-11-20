extern crate cc;
extern crate pkg_config;

use std::env;
use std::path::PathBuf;

fn main() {
    let mut btree: PathBuf =
        PathBuf::from(&env::var("CARGO_MANIFEST_DIR").unwrap());
    btree.push("btree");

    if !pkg_config::find_library("libbtree").is_ok() {
        println!("cargo:rustc-link-lib=crypto");
        let mut build = cc::Build::new();
        build
            .file(btree.join("btree.c"))
            .opt_level(2)
            .flag("-Wno-unused-parameter")
            .compile("libbtree.a")
    }
}
