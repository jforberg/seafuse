use std::path::Path;

use seafrepo::*;

fn main() {
    let path = Path::new("tests/data/testrepo/commits");

    for c in CommitIterator::new(path) {
        println!("{:?}", c);
    }
}
