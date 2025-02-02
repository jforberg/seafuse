use seafrepo::*;
use std::path::{Path, PathBuf};

const TEST_REPO_PATH: &str = "tests/data/testrepo/";
const TEST_REPO_UUID: &str = "868be3a7-b357-4189-af52-304b402d9904";

#[test]
fn parse_example_commit() {
    let c = parse_commit(&path_to(
        "commits",
        "038cac5ffc20b13a4fac8d21e60bf01d03f8a179",
    ))
    .unwrap();
    assert_eq!(
        c.commit_id.to_string(),
        "038cac5ffc20b13a4fac8d21e60bf01d03f8a179"
    );
}

#[test]
fn find_and_parse_commits() {
    let p = Path::new(TEST_REPO_PATH).join("commits");
    let ids: Vec<String> = CommitIterator::new(&p)
        .map(|c| c.unwrap().commit_id.to_string())
        .collect();

    assert_eq!(
        ids,
        vec![
            "038cac5ffc20b13a4fac8d21e60bf01d03f8a179",
            "3437b93bb6ce178dd3041b9db1874cc731cbca19",
            "b075fb2acc9573f8b9546522f2c7f2221a062a29",
        ]
    );
}

#[test]
fn parse_example_fs_file() {
    let f = parse_fs(&path_to("fs", "e40b894880747010bf6ec384b83e578f352beed7"))
        .unwrap()
        .unwrap_file();

    let ids: Vec<_> = f.block_ids.into_iter().map(|s| s.to_string()).collect();
    assert_eq!(ids, vec!["5516c9472d25947faae16a94ee25ed8054978c85"]);
}

#[test]
fn parse_example_fs_dir() {
    let d = parse_fs(&path_to("fs", "ebd03d7c735be353d1c6d302e1092e69b5c5d041"))
        .unwrap()
        .unwrap_dir();
    assert_eq!(
        d.dirents[0].id.to_string(),
        "e40b894880747010bf6ec384b83e578f352beed7"
    );
}

#[test]
fn lookup_head_commit() {
    let lib = Library::new(Path::new(TEST_REPO_PATH), TEST_REPO_UUID)
        .populate()
        .unwrap();
    assert_eq!(
        lib.head_commit.unwrap().commit_id.to_string(),
        "038cac5ffc20b13a4fac8d21e60bf01d03f8a179"
    );
}

#[test]
fn sha1_roundtrip() {
    let raw = "e40b894880747010bf6ec384b83e578f352beed7";
    let sha1 = Sha1::parse(raw).unwrap();
    println!("{:?}", sha1);
    assert_eq!(sha1.to_string(), raw);
}

#[test]
fn sha1_malformed() {
    assert_eq!(Sha1::parse("1234"), None);
    assert_eq!(Sha1::parse("thisisnosha1"), None);
}

fn path_to(ty: &str, uuid: &str) -> PathBuf {
    Path::new(TEST_REPO_PATH)
        .join(ty)
        .join(TEST_REPO_UUID)
        .join(&uuid[..2])
        .join(&uuid[2..])
}
