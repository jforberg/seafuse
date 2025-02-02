use flate2::read::ZlibDecoder;
use serde::{Deserialize, Deserializer};
use std::{
    collections::HashSet,
    fmt,
    fmt::Debug,
    fmt::Display,
    fs,
    path::{Path, PathBuf},
};
use walkdir::WalkDir;

#[derive(Debug)]
pub struct Library {
    pub repo_path: PathBuf,
    pub uuid: String,
    pub head_commit: Option<Sha1>,
}

impl Library {
    pub fn new(repo_path: &Path, uuid: &str) -> Library {
        Library {
            repo_path: repo_path.to_owned(),
            uuid: uuid.to_owned(),
            head_commit: None,
        }
    }

    pub fn populate(mut self) -> Result<Library, SeafError> {
        // Find the HEAD commit(s). TODO improve this
        let mut all_ids = HashSet::new();
        let mut parents = HashSet::new();

        for c in CommitIterator::new(&self.obj_storage_path("commits")) {
            let c = c?;

            all_ids.insert(c.commit_id);

            if let Some(pid) = c.parent_id {
                parents.insert(pid);
            }

            if let Some(pid) = c.second_parent_id {
                parents.insert(pid);
            }
        }

        let children: Vec<&Sha1> = all_ids.difference(&parents).collect();
        match children.len() {
            0 => {}
            1 => {
                self.head_commit = Some(children[0].to_owned());
            }
            _ => {
                return Err(SeafError::MultipleHeads);
            }
        }

        Ok(self)
    }

    fn obj_storage_path(&self, ty: &str) -> PathBuf {
        self.repo_path.join(ty).join(&self.uuid)
    }
}

#[derive(Debug, Deserialize)]
pub struct Commit {
    pub commit_id: Sha1,
    pub root_id: Sha1,
    pub repo_id: String,
    pub creator_name: String,
    pub creator: String,
    pub description: String,
    pub ctime: u64,
    pub parent_id: Option<Sha1>,
    pub second_parent_id: Option<Sha1>,
    pub repo_name: String,
    pub repo_desc: String,
    pub repo_category: Option<String>,
    pub no_local_history: u32,
    pub version: u32,
}

pub fn parse_commit(filename: &Path) -> Result<Commit, SeafError> {
    let f = fs::File::open(filename)?;
    let c: Commit =
        serde_json::from_reader(f).map_err(|e| SeafError::ParseJson(filename.to_owned(), e))?;
    Ok(c)
}

#[derive(Debug)]
pub struct CommitIterator {
    it: walkdir::IntoIter,
}

impl CommitIterator {
    pub fn new(path: &Path) -> CommitIterator {
        CommitIterator {
            it: WalkDir::new(path).into_iter(),
        }
    }
}

impl Iterator for CommitIterator {
    type Item = Result<Commit, SeafError>;

    fn next(&mut self) -> Option<Self::Item> {
        for x in &mut self.it {
            match x {
                Err(e) => return Some(Err(SeafError::from(e))),
                Ok(de) => {
                    if !de.file_type().is_file() {
                        continue;
                    }

                    return Some(parse_commit(de.path()));
                }
            }
        }
        None
    }
}

#[derive(Debug, Deserialize)]
pub struct File {
    pub block_ids: Vec<Sha1>,
    pub size: u64,
    #[serde(rename(deserialize = "type"))]
    pub ty: u32,
    pub version: u32,
}

#[derive(Debug, Deserialize)]
pub struct Dir {
    pub dirents: Vec<Dirent>,
    #[serde(rename(deserialize = "type"))]
    pub ty: u32,
    pub version: u32,
}

#[derive(Debug, Deserialize)]
pub struct Dirent {
    pub id: Sha1,
    pub mode: u32,
    pub modifier: String,
    pub mtime: u64,
    pub name: String,
    pub size: u64,
}

#[derive(Debug, Deserialize)]
#[serde(untagged)]
pub enum Fs {
    File(File),
    Dir(Dir),
}

impl Fs {
    pub fn unwrap_file(self) -> File {
        if let Fs::File(f) = self {
            f
        } else {
            panic!("Expected File, have {:?}", self);
        }
    }

    pub fn unwrap_dir(self) -> Dir {
        if let Fs::Dir(d) = self {
            d
        } else {
            panic!("Expected Dir, have {:?}", self);
        }
    }
}

pub fn parse_fs(filename: &Path) -> Result<Fs, SeafError> {
    let f = fs::File::open(filename)?;
    let dec = ZlibDecoder::new(f);
    let fs: Fs =
        serde_json::from_reader(dec).map_err(|e| SeafError::ParseJson(filename.to_owned(), e))?;
    Ok(fs)
}

#[derive(Copy, Clone, PartialEq, Eq, Hash)]
pub struct Sha1 {
    words: [u32; 5],
}

impl Sha1 {
    pub fn parse(hex: &str) -> Option<Sha1> {
        let mut sha = Sha1 { words: [0; 5] };

        for i in 0..5 {
            let s = hex.get(i * 8..(i + 1) * 8)?;
            let x = u32::from_str_radix(s, 16).ok()?;
            sha.words[(5 - 1) - i] = x;
        }

        Some(sha)
    }
}

impl Display for Sha1 {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        for i in 0..5 {
            write!(f, "{:08x}", self.words[(5 - 1) - i])?;
        }
        Ok(())
    }
}

impl Debug for Sha1 {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(f, "Sha1(")?;
        Display::fmt(self, f)?;
        write!(f, ")")
    }
}

impl<'de> Deserialize<'de> for Sha1 {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: Deserializer<'de>,
    {
        let s = String::deserialize(deserializer)?;
        match Sha1::parse(&s) {
            Some(sha) => Ok(sha),
            None => Err(serde::de::Error::custom("invalid sha1 hash")),
        }
    }
}

#[derive(Debug)]
pub enum SeafError {
    IO(std::io::Error),
    ParseJson(PathBuf, serde_json::Error),
    WalkDir(walkdir::Error),
    NotImpl,
    MultipleHeads,
}

impl From<std::io::Error> for SeafError {
    fn from(e: std::io::Error) -> Self {
        Self::IO(e)
    }
}

impl From<walkdir::Error> for SeafError {
    fn from(e: walkdir::Error) -> Self {
        Self::WalkDir(e)
    }
}
