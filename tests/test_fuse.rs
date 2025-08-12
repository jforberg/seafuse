// XXX module is declared twice, how to fix this?
mod util;

use seafuse::*;
use std::path::Path;
use tempdir::TempDir;
use util::*;

struct TestFilesystem {
    _fuse_session: fuser::BackgroundSession,
    temp_dir: TempDir,
}

impl TestFilesystem {
    pub fn mount(repo: &TestRepo) -> Self {
        let fs = SeafFuse::new(repo.open());
        let temp_dir = make_temp_dir();
        let path = temp_dir.path();

        TestFilesystem {
            _fuse_session: fuser::spawn_mount2(fs, path, &[]).unwrap(),
            temp_dir,
        }
    }

    pub fn path(&self) -> &Path {
        self.temp_dir.path()
    }
}

#[test]
fn readdir() {
    let fs = TestFilesystem::mount(&TR_BASIC);
    let mut entries = std::fs::read_dir(fs.path())
        .unwrap()
        .map(|de| {
            de.unwrap()
                .path()
                .file_name()
                .unwrap()
                .to_str()
                .unwrap()
                .to_string()
        })
        .collect::<Vec<_>>();

    entries.sort();

    assert_eq!(entries, ["somedir", "test.md"]);
}
