// Copyright 2025 Johan Förberg
// SPDX-License-Identifier: MIT

use fuser::FUSE_ROOT_ID;
use libc::{EBADF, ENOENT};
use std::ffi::{OsStr, OsString};

use seafuse::*;

pub mod util;
use util::*;

#[test]
fn readdir_several_files() {
    let mut fs = SeafFuse::new(TR_BASIC.open());
    let mut entries: Vec<OsString> = fs
        .do_readdir(FUSE_ROOT_ID)
        .unwrap()
        .into_iter()
        .map(|e| e.name)
        .collect();

    entries.sort();

    assert_eq!(entries, ["somedir", "test.md"]);
}

#[test]
fn lookup_file() {
    let mut fs = SeafFuse::new(TR_BASIC.open());
    let attr = fs.do_lookup(FUSE_ROOT_ID, OsStr::new("test.md")).unwrap();

    assert_eq!(attr.size, 13);
}

#[test]
fn lookup_vs_getattr() {
    let mut fs = SeafFuse::new(TR_BASIC.open());
    let attr1 = fs.do_lookup(FUSE_ROOT_ID, OsStr::new("test.md")).unwrap();
    let attr2 = fs.do_getattr(attr1.ino).unwrap();

    assert_eq!(attr1, attr2);
}

#[test]
fn lookup_non_existent() {
    let mut fs = SeafFuse::new(TR_BASIC.open());
    let r = fs.do_lookup(FUSE_ROOT_ID, OsStr::new("doesnt_exist"));

    assert_eq!(r.unwrap_err(), ENOENT);
}

#[test]
fn read_file() {
    let mut fs = SeafFuse::new(TR_BASIC.open());
    let attr = fs.do_lookup(FUSE_ROOT_ID, OsStr::new("test.md")).unwrap();
    let fh = fs.do_open(attr.ino).unwrap();
    let data = fs.do_read(fh, 8, 4).unwrap();

    fs.do_release(fh).unwrap();

    assert_eq!(data, "test".as_bytes());
}

#[test]
fn bad_file_handle() {
    let mut fs = SeafFuse::new(TR_BASIC.open());
    let r = fs.do_read(123, 0, 1024);

    assert_eq!(r.unwrap_err(), EBADF);
}
