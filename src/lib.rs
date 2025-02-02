use flate2::read::ZlibDecoder;
use serde::Deserialize;
use std::{
    fs,
    path::{Path, PathBuf},
};
use walkdir::WalkDir;

#[derive(Debug, Deserialize)]
pub struct Commit {
    pub commit_id: String,
    pub root_id: String,
    pub repo_id: String,
    pub creator_name: String,
    pub creator: String,
    pub description: String,
    pub ctime: u64,
    pub parent_id: Option<String>,
    pub second_parent_id: Option<String>,
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
    pub block_ids: Vec<String>,
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
    pub id: String,
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

#[derive(Debug)]
pub enum SeafError {
    IO(std::io::Error),
    ParseJson(PathBuf, serde_json::Error),
    WalkDir(walkdir::Error),
    NotImpl,
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
