use flate2::read::ZlibDecoder;
use serde::{Deserialize, Deserializer};
use std::{
    cmp::min,
    fmt,
    fmt::Debug,
    fmt::Display,
    fs, io,
    io::{Read, Seek, SeekFrom},
    path::{Path, PathBuf},
    rc::Rc,
};
use walkdir::WalkDir;

#[derive(Debug, Clone)]
pub struct LibraryLocation {
    pub repo_path: PathBuf,
    pub uuid: String,
}

#[derive(Debug, Clone)]
pub struct Library {
    pub location: Rc<LibraryLocation>,
    pub head_commit: Option<CommitJson>,
}

impl Library {
    pub fn open(repo_path: &Path, uuid: &str) -> Result<Library, SeafError> {
        Library::new(repo_path, uuid).populate()
    }

    fn new(repo_path: &Path, uuid: &str) -> Library {
        Library {
            location: Rc::new(LibraryLocation {
                repo_path: repo_path.to_owned(),
                uuid: uuid.to_owned(),
            }),
            head_commit: None,
        }
    }

    fn populate(mut self) -> Result<Library, SeafError> {
        let mut head_commit: Option<CommitJson> = None;

        // The head commit is assumed to be the most recent commit
        for c in CommitIterator::new(&obj_type_path(&self.location, "commits")) {
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

    pub fn load_fs(&self, id: Sha1) -> Result<FsJson, SeafError> {
        parse_fs_json(&self.obj_path("fs", id))
    }

    pub fn walk_fs(&self) -> FsIterator {
        FsIterator::new(self)
    }

    fn obj_path(&self, ty: &str, id: Sha1) -> PathBuf {
        full_obj_path(&self.location, ty, id)
    }

    pub fn file_by_json(&self, file: &FileJson) -> File {
        File {
            location: self.location.clone(),
            block_ids: file.block_ids.clone(),
        }
    }

    pub fn file_by_id(&self, id: Sha1) -> Result<File, SeafError> {
        let file = self.load_fs(id)?.try_file()?;
        Ok(File {
            location: self.location.clone(),
            block_ids: file.block_ids,
        })
    }
}

fn full_obj_path(ll: &LibraryLocation, ty: &str, id: Sha1) -> PathBuf {
    let id_str = id.to_string();
    obj_type_path(ll, ty).join(&id_str[0..2]).join(&id_str[2..])
}

fn obj_type_path(ll: &LibraryLocation, ty: &str) -> PathBuf {
    ll.repo_path.join(ty).join(&ll.uuid)
}

pub struct File {
    location: Rc<LibraryLocation>,
    block_ids: Vec<Sha1>,
}

impl File {
    pub fn to_reader(self) -> Result<FileReader, SeafError> {
        Ok(FileReader::new(self.to_block_reader()?))
    }

    fn to_block_reader(self) -> Result<FileBlockReader, SeafError> {
        FileBlockReader::from_file(self)
    }
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
    stack: Vec<DirJson>,
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

    fn next_result(&mut self) -> Result<Option<(PathBuf, DirentJson, FsJson)>, SeafError> {
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

                if let FsJson::Dir(ref d) = fs {
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
    type Item = Result<(PathBuf, DirentJson, FsJson), SeafError>;

    fn next(&mut self) -> Option<Self::Item> {
        self.next_result().transpose()
    }
}

pub fn parse_commit(filename: &Path) -> Result<CommitJson, SeafError> {
    let f = fs::File::open(filename)?;
    let c: CommitJson =
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
    type Item = Result<CommitJson, SeafError>;

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

#[derive(Debug)]
pub struct FileReader {
    block_reader: FileBlockReader,
    byte_pos: u64,
}

impl FileReader {
    fn new(block_reader: FileBlockReader) -> FileReader {
        FileReader {
            block_reader,
            byte_pos: 0,
        }
    }
}

impl Read for FileReader {
    fn read(&mut self, buf: &mut [u8]) -> std::io::Result<usize> {
        match self.block_reader.read_at_offset(self.byte_pos, buf) {
            Ok(s) => {
                self.byte_pos += s as u64;
                Ok(s)
            }
            Err(SeafError::IO(e)) => Err(e),
            Err(e) => Err(io::Error::from(e)),
        }
    }
}

impl Seek for FileReader {
    fn seek(&mut self, pos: SeekFrom) -> io::Result<u64> {
        match pos {
            SeekFrom::Start(o) => {
                self.byte_pos = o;
                Ok(self.byte_pos)
            }
            SeekFrom::End(o) => {
                let end_pos = self.block_reader.size as i64;
                let new_pos = end_pos + o;
                if new_pos < 0 {
                    return Err(From::from(io::ErrorKind::InvalidInput));
                }

                self.byte_pos = new_pos as u64;
                Ok(self.byte_pos)
            }
            SeekFrom::Current(o) => {
                let new_pos = self.byte_pos as i64 + o;
                if new_pos < 0 {
                    return Err(From::from(io::ErrorKind::InvalidInput));
                }

                self.byte_pos = new_pos as u64;
                Ok(self.byte_pos)
            }
        }
    }
}

#[derive(Debug)]
struct FileBlockReader {
    location: Rc<LibraryLocation>,
    block_ids: Vec<Sha1>,
    block_sizes: Vec<usize>,
    size: usize,
}

impl FileBlockReader {
    fn from_file(file: File) -> Result<FileBlockReader, SeafError> {
        let mut block_sizes = vec![];
        let mut size = 0;

        for id in &file.block_ids {
            let path = full_obj_path(&file.location, "blocks", *id);
            let md = fs::metadata(&path)?;
            block_sizes.push(md.len() as usize);
            size += md.len() as usize;
        }

        Ok(FileBlockReader {
            location: file.location,
            block_ids: file.block_ids,
            block_sizes,
            size,
        })
    }

    fn read_at_offset(&self, offset: u64, buf: &mut [u8]) -> Result<usize, SeafError> {
        let to_read = buf.len();
        let mut have_read = 0;

        match self.find_start_block(offset) {
            None => Ok(0),
            Some((mut block_idx, mut block_offset)) => {
                while have_read < to_read && block_idx < self.block_ids.len() {
                    let this_block_size = self.block_sizes[block_idx];
                    let to_read_this_block =
                        min(to_read - have_read, this_block_size - block_offset);
                    let file_path =
                        full_obj_path(&self.location, "blocks", self.block_ids[block_idx]);
                    let mut file = fs::File::open(file_path)?;

                    file.seek(SeekFrom::Start(block_offset as u64))?;
                    file.read_exact(&mut buf[have_read..have_read + to_read_this_block])?;

                    have_read += to_read_this_block;
                    block_idx += 1;
                    block_offset = 0;
                }

                Ok(have_read)
            }
        }
    }

    fn find_start_block(&self, offset: u64) -> Option<(usize, usize)> {
        let mut byte_idx = 0;

        for block_idx in 0..self.block_ids.len() {
            let this_size = self.block_sizes[block_idx];

            if byte_idx + this_size > offset as usize {
                return Some((block_idx, offset as usize - byte_idx));
            }

            byte_idx += this_size;
        }

        None
    }
}

#[derive(Debug, Clone, Deserialize)]
pub struct CommitJson {
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

#[derive(Debug, Deserialize, Clone)]
pub struct FileJson {
    pub block_ids: Vec<Sha1>,
    pub size: u64,
    #[serde(rename(deserialize = "type"))]
    pub ty: u32,
    pub version: u32,
}

#[derive(Debug, Deserialize, Clone)]
pub struct DirJson {
    pub dirents: Vec<DirentJson>,
    #[serde(rename(deserialize = "type"))]
    pub ty: u32,
    pub version: u32,
}

#[derive(Debug, Deserialize, Clone)]
pub struct DirentJson {
    pub id: Sha1,
    pub mode: u32,
    pub mtime: u64,
    pub name: String,
}

#[derive(Debug, Deserialize, Clone)]
#[serde(untagged)]
pub enum FsJson {
    File(FileJson),
    Dir(DirJson),
}

impl FsJson {
    pub fn unwrap_file(self) -> FileJson {
        self.try_file().unwrap()
    }

    pub fn unwrap_dir(self) -> DirJson {
        self.try_dir().unwrap()
    }

    pub fn try_file(self) -> Result<FileJson, SeafError> {
        match self {
            FsJson::File(f) => Ok(f),
            _ => Err(SeafError::WrongFsType),
        }
    }

    pub fn try_dir(self) -> Result<DirJson, SeafError> {
        match self {
            FsJson::Dir(d) => Ok(d),
            _ => Err(SeafError::WrongFsType),
        }
    }

    pub fn type_name(&self) -> &'static str {
        match self {
            FsJson::Dir(_) => "Dir",
            FsJson::File(_) => "File",
        }
    }
}

pub fn parse_fs_json(filename: &Path) -> Result<FsJson, SeafError> {
    let f = fs::File::open(filename)?;
    let dec = ZlibDecoder::new(f);
    let fs: FsJson =
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
    NoHeadCommit,
    WrongFsType,
}

impl From<std::io::Error> for SeafError {
    fn from(e: std::io::Error) -> Self {
        Self::IO(e)
    }
}

impl From<SeafError> for io::Error {
    fn from(e: SeafError) -> Self {
        Self::other(format!("{e:?}"))
    }
}

impl From<walkdir::Error> for SeafError {
    fn from(e: walkdir::Error) -> Self {
        Self::WalkDir(e)
    }
}
