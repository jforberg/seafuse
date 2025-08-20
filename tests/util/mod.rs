use seafuse::*;
use std::path::{Path, PathBuf};

pub struct TestRepo {
    pub path: &'static str,
    pub uuid: &'static str,
}

impl TestRepo {
    pub fn path_to(&self, ty: &str, uuid: &str) -> PathBuf {
        Path::new(&self.path)
            .join(ty)
            .join(self.uuid)
            .join(&uuid[..2])
            .join(&uuid[2..])
    }

    pub fn open(&self) -> Library {
        Library::open(Path::new(self.path), self.uuid).unwrap()
    }
}

pub const TR_BASIC: TestRepo = TestRepo {
    path: "tests/testrepos/basic",
    uuid: "868be3a7-b357-4189-af52-304b402d9904",
};

pub const TR_MULTIBLOCK: TestRepo = TestRepo {
    path: "tests/testrepos/multiblock",
    uuid: "868be3a7-b357-4189-af52-304b402d9904",
};

pub const TR_EMPTY_DIR: TestRepo = TestRepo {
    path: "tests/testrepos/empty_dir",
    uuid: "868be3a7-b357-4189-af52-304b402d9904",
};

pub const TR_NESTED: TestRepo = TestRepo {
    path: "tests/testrepos/nested",
    uuid: "66ece1b2-55ed-414a-b0ee-2550273b0d29",
};
