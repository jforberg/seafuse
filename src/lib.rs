use flate2::read::ZlibDecoder;
use serde::{Deserialize, Deserializer};
use std::{
    fmt,
    fmt::Debug,
    fmt::Display,
    fs,
    io::Read,
    path::{Path, PathBuf},
};
use walkdir::WalkDir;

#[derive(Debug)]
pub struct Library {
    pub repo_path: PathBuf,
    pub uuid: String,
    pub head_commit: Option<Commit>,
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
        let mut head_commit: Option<Commit> = None;

        // The head commit is assumed to be the most recent commit
        for c in CommitIterator::new(&self.obj_type_path("commits")) {
            let c = c?;

            if let Some(ref hc) = head_commit {
                if c.ctime > hc.ctime {
                    head_commit = Some(c);
                }
            } else {
                head_commit = Some(c);
            }
        }

        self.head_commit = head_commit;
        if self.head_commit.is_none() {
            return Err(SeafError::NoHeadCommit);
        }

        Ok(self)
    }

    pub fn load_fs(&self, id: Sha1) -> Result<Fs, SeafError> {
        parse_fs(&self.obj_path("fs", id))
    }

    pub fn walk_fs(&self) -> FsIterator {
        FsIterator::new(self)
    }

    fn obj_path(&self, ty: &str, id: Sha1) -> PathBuf {
        full_obj_path(&self.obj_type_path(ty), id)
    }

    fn obj_type_path(&self, ty: &str) -> PathBuf {
        self.repo_path.join(ty).join(&self.uuid)
    }

    pub fn open_file(&self, id: Sha1) -> Result<FileReader, SeafError> {
        let file = self.load_fs(id)?.try_file()?;
        Ok(FileReader::new(self.obj_type_path("blocks"), &file))
    }
}

fn full_obj_path(obj_type_path: &Path, id: Sha1) -> PathBuf {
    let id_str = id.to_string();
    obj_type_path.join(&id_str[0..2]).join(&id_str[2..])
}

#[derive(Debug)]
pub struct FsIterator<'a> {
    lib: &'a Library,
    state: FsItState,
}

#[derive(Debug)]
enum FsItState {
    Root(Sha1),
    NotRoot(FsItNrState),
}

#[derive(Debug)]
struct FsItNrState {
    stack: Vec<Dir>,
    path: PathBuf,
}

impl FsIterator<'_> {
    pub fn new(lib: &Library) -> FsIterator<'_> {
        let root_id = lib.head_commit.as_ref().unwrap().root_id;

        FsIterator {
            lib,
            state: FsItState::Root(root_id),
        }
    }

    fn next_result(&mut self) -> Result<Option<(PathBuf, Dirent, Fs)>, SeafError> {
        // TODO Too much copying is going on here, optimise
        let nr_state = match &mut self.state {
            FsItState::Root(root_id) => {
                let d = self.lib.load_fs(*root_id)?.unwrap_dir();

                self.state = FsItState::NotRoot(FsItNrState {
                    stack: vec![d],
                    path: "".into(),
                });

                match &mut self.state {
                    FsItState::NotRoot(ref mut nr_state) => nr_state,
                    _ => unreachable!(),
                }
            }
            FsItState::NotRoot(ref mut nr_state) => nr_state,
        };

        while !nr_state.stack.is_empty() {
            if let Some(de) = nr_state.stack.last_mut().unwrap().dirents.pop() {
                let fs = self.lib.load_fs(de.id)?;
                let path_before = nr_state.path.clone();

                if let Fs::Dir(ref d) = fs {
                    nr_state.stack.push(d.clone());
                    nr_state.path.push(&de.name);
                }

                return Ok(Some((path_before, de, fs)));
            }

            nr_state.stack.pop();
            nr_state.path.pop();
        }

        Ok(None)
    }
}

impl Iterator for FsIterator<'_> {
    type Item = Result<(PathBuf, Dirent, Fs), SeafError>;

    fn next(&mut self) -> Option<Self::Item> {
        self.next_result().transpose()
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

#[derive(Debug, Deserialize, Clone)]
pub struct File {
    pub block_ids: Vec<Sha1>,
    pub size: u64,
    #[serde(rename(deserialize = "type"))]
    pub ty: u32,
    pub version: u32,
}

#[derive(Debug, Deserialize, Clone)]
pub struct Dir {
    pub dirents: Vec<Dirent>,
    #[serde(rename(deserialize = "type"))]
    pub ty: u32,
    pub version: u32,
}

#[derive(Debug, Deserialize, Clone)]
pub struct Dirent {
    pub id: Sha1,
    pub mode: u32,
    pub mtime: u64,
    pub name: String,
}

#[derive(Debug, Deserialize, Clone)]
#[serde(untagged)]
pub enum Fs {
    File(File),
    Dir(Dir),
}

impl Fs {
    pub fn unwrap_file(self) -> File {
        self.try_file().unwrap()
    }

    pub fn unwrap_dir(self) -> Dir {
        self.try_dir().unwrap()
    }

    pub fn try_file(self) -> Result<File, SeafError> {
        match self {
            Fs::File(f) => Ok(f),
            _ => Err(SeafError::WrongFsType),
        }
    }

    pub fn try_dir(self) -> Result<Dir, SeafError> {
        match self {
            Fs::Dir(d) => Ok(d),
            _ => Err(SeafError::WrongFsType),
        }
    }

    pub fn type_name(self) -> &'static str {
        match self {
            Fs::Dir(_) => "Dir",
            Fs::File(_) => "File",
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
pub struct FileReader {
    block_path: PathBuf,
    block_ids: Vec<Sha1>,
    cur_file: Option<fs::File>,
}

impl FileReader {
    pub fn new(block_path: PathBuf, file: &File) -> FileReader {
        let mut fr = FileReader {
            block_path,
            block_ids: vec![],
            cur_file: None,
        };
        for b in file.block_ids.iter().rev() {
            fr.block_ids.push(*b);
        }
        fr
    }
}

impl Read for FileReader {
    fn read(&mut self, buf: &mut [u8]) -> std::io::Result<usize> {
        // We need a file to read. Either we are in the middle of reading some block, or we open
        // the next block
        let file = match self.cur_file {
            Some(ref mut f) => f,
            None => match self.block_ids.pop() {
                None => return Ok(0),
                Some(bid) => {
                    let path = full_obj_path(&self.block_path, bid);
                    let file = fs::File::open(path)?;
                    self.cur_file.insert(file)
                }
            },
        };

        let n = file.read(buf)?;
        if n > 0 {
            return Ok(n);
        }

        self.cur_file = None;
        self.read(buf)
    }
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
    NoHeadCommit,
    WrongFsType,
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
