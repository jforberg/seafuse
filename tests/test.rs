use seafrepo::*;
use std::path::Path;

const TEST_REPO: &str = "tests/data/testrepo/";

#[test]
fn parse_example_commit() {
    let p = Path::new(TEST_REPO).join(
        "commits/868be3a7-b357-4189-af52-304b402d9904/03/8cac5ffc20b13a4fac8d21e60bf01d03f8a179",
    );

    let c = parse_commit(&p).unwrap();
    assert_eq!(c.commit_id, "038cac5ffc20b13a4fac8d21e60bf01d03f8a179");
}

#[test]
fn find_and_parse_commits() {
    let p = Path::new(TEST_REPO).join("commits");
    let ids: Vec<String> = CommitIterator::new(&p)
        .map(|c| c.unwrap().commit_id)
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
