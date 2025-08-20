use std::collections::HashSet;
use std::io;
use std::io::{Read, Seek, SeekFrom};
use std::path::{Path, PathBuf};

use seafuse::*;

pub mod util;
use util::*;

#[test]
fn find_and_parse_commits() {
    let p = Path::new(&TR_BASIC.path).join("commits");
    let ids: Vec<String> = CommitIterator::new(&p)
        .map(|c| c.unwrap().commit_id.to_string())
        .collect();

    assert_eq!(
        ids[0..2],
        vec![
            "038cac5ffc20b13a4fac8d21e60bf01d03f8a179",
            "3437b93bb6ce178dd3041b9db1874cc731cbca19",
        ]
    );
}

#[test]
fn parse_example_fs_file() {
    let f = parse_fs_json(&TR_BASIC.path_to("fs", "e40b894880747010bf6ec384b83e578f352beed7"))
        .unwrap()
        .unwrap_file();

    let ids: Vec<_> = f.block_ids.into_iter().map(|s| s.to_string()).collect();
    assert_eq!(ids, vec!["5516c9472d25947faae16a94ee25ed8054978c85"]);
}

#[test]
fn parse_example_fs_dir() {
    let d = parse_fs_json(&TR_BASIC.path_to("fs", "ebd03d7c735be353d1c6d302e1092e69b5c5d041"))
        .unwrap()
        .unwrap_dir();
    assert_eq!(
        d.dirents[0].id.to_string(),
        "e40b894880747010bf6ec384b83e578f352beed7"
    );
}

#[test]
fn sha1_roundtrip() {
    let raw = "e40b894880747010bf6ec384b83e578f352beed7";
    let sha1 = Sha1::parse(raw).unwrap();
    println!("{sha1:?}");
    assert_eq!(sha1.to_string(), raw);
}

#[test]
fn sha1_malformed() {
    assert_eq!(Sha1::parse("1234"), None);
    assert_eq!(Sha1::parse("thisisnosha1"), None);
}

#[test]
fn walk_lib_fs() {
    let lib = TR_BASIC.open();
    let mut file_names = HashSet::new();

    for r in lib.fs_iterator() {
        let (p, de, _fs) = r.unwrap();
        file_names.insert(p.join(de.name));
    }

    assert_eq!(
        file_names,
        HashSet::from_iter(
            ["test.md", "somedir", "somedir/test2.md"]
                .into_iter()
                .map(PathBuf::from)
        )
    );
}

#[test]
fn walk_prune_directory() {
    let lib = TR_BASIC.open();

    let mut it = lib.fs_iterator();
    let (_path, de, _fs) = it.next().unwrap().unwrap();
    assert_eq!(de.name, "somedir");

    it.prune();
    let (_path, de, _fs) = it.next().unwrap().unwrap();
    assert_eq!(de.name, "test.md");

    let r = it.next();
    assert!(r.is_none());
}

#[test]
fn walk_prune_root() {
    let lib = TR_BASIC.open();
    let mut it = lib.fs_iterator();
    it.prune();

    let r = it.next();
    assert!(r.is_none());
}

#[test]
fn walk_prune_nested() {
    let lib = TR_NESTED.open();
    let mut it = lib.fs_iterator();
    let mut selected_paths: HashSet<PathBuf> = HashSet::new();
    let mut did_see_a = false;

    while let Some(x) = it.next() {
        let (path, de, _fs) = x.unwrap();
        let path_is_a = de.name == "a";
        let full_path = path.join(de.name);

        assert!(!did_see_a || !path_is_a);

        if path_is_a {
            it.prune();
            did_see_a = true;
            continue;
        }

        selected_paths.insert(full_path);
    }

    assert_eq!(selected_paths, HashSet::from(["b", "b/b.md"].map(From::from)));
}

#[test]
fn read_file_having_single_block() {
    let lib = TR_BASIC.open();
    let id = Sha1::parse("e40b894880747010bf6ec384b83e578f352beed7").unwrap();
    let f = lib.file_by_id(id).unwrap();
    let mut fr = lib.file_reader(&f).unwrap();
    let mut bytes = vec![];

    fr.read_to_end(&mut bytes).unwrap();

    assert_eq!(&bytes, b"# test\n\ntest\n");
}

#[test]
fn read_file_having_multiple_blocks() {
    let lib = TR_MULTIBLOCK.open();
    let id = Sha1::parse("e40b894880747010bf6ec384b83e578f352beed7").unwrap();
    let f = lib.file_by_id(id).unwrap();
    let mut fr = lib.file_reader(&f).unwrap();
    let mut bytes = vec![];

    fr.read_to_end(&mut bytes).unwrap();

    assert_eq!(&bytes, b"gronkadonkachonka");
}

#[test]
fn read_file_range() {
    let lib = TR_MULTIBLOCK.open();
    let id = Sha1::parse("e40b894880747010bf6ec384b83e578f352beed7").unwrap();
    let f = lib.file_by_id(id).unwrap();
    let mut fr = lib.file_reader(&f).unwrap();
    let mut bytes = [0; 7];

    fr.seek(SeekFrom::Start(5)).unwrap();
    let c = fr.read(&mut bytes).unwrap();

    assert_eq!(c, 7);
    assert_eq!(&bytes, b"adonkac");
}

#[test]
fn read_empty_range() {
    let lib = TR_MULTIBLOCK.open();
    let id = Sha1::parse("e40b894880747010bf6ec384b83e578f352beed7").unwrap();
    let f = lib.file_by_id(id).unwrap();
    let mut fr = lib.file_reader(&f).unwrap();
    let mut bytes = [];

    fr.seek(SeekFrom::Start(5)).unwrap();
    let c = fr.read(&mut bytes).unwrap();

    assert_eq!(c, 0);
}

#[test]
fn read_range_outside() {
    let lib = TR_MULTIBLOCK.open();
    let id = Sha1::parse("e40b894880747010bf6ec384b83e578f352beed7").unwrap();
    let f = lib.file_by_id(id).unwrap();
    let mut fr = lib.file_reader(&f).unwrap();
    let mut bytes = [0; 10];

    fr.seek(SeekFrom::Start(20)).unwrap();
    let c = fr.read(&mut bytes).unwrap();

    assert_eq!(c, 0);
}

#[test]
fn open_nonexistent_file() {
    let lib = TR_BASIC.open();
    let id = Sha1::parse("1234123412341234123412341234123412341234").unwrap();

    match lib.file_by_id(id) {
        Err(SeafError::IO(_, e)) => assert_eq!(e.kind(), io::ErrorKind::NotFound),
        _ => unreachable!(),
    };
}

#[test]
fn empty_root_dir() {
    let lib = TR_EMPTY_DIR.open();

    let dir = lib.load_fs(lib.head_commit.root_id).unwrap().unwrap_dir();
    assert_eq!(dir.dirents, vec![]);
}
