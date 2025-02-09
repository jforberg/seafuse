use std::path::Path;

use seafrepo::*;

fn main() {
    let path = Path::new("tests/data/testrepo");
    let lib = Library::new(path, "868be3a7-b357-4189-af52-304b402d9904")
        .populate()
        .unwrap();
    let head = lib.head_commit.as_ref().unwrap();
    println!("Repo name: {}", head.repo_name);
    println!("Head commit: {}", head.commit_id);
    println!("Root: {}", head.root_id);
    println!("Last modified: {}, by {}", head.ctime, head.creator_name);

    for r in lib.walk_fs() {
        let (de, fs) = r.unwrap();
        println!("{} - {:?}", de.name, fs);
    }
}
