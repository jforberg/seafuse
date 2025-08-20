// Copyright 2025 Johan Förberg
// SPDX-License-Identifier: MIT

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
    sync::Arc,
};
use walkdir::WalkDir;

#[derive(Debug, Clone)]
pub struct LibraryLocation {
    pub repo_path: PathBuf,
    pub uuid: String,
}

#[derive(Debug, Clone)]
pub struct Library {
    pub location: Arc<LibraryLocation>,
    pub head_commit: CommitJson,
}

impl Library {
    pub fn open(repo_path: &Path, uuid: &str) -> Result<Library, SeafError> {
        let location = Arc::new(LibraryLocation {
            repo_path: repo_path.to_path_buf(),
            uuid: uuid.to_string(),
        });
        let head_commit = find_head_commit(&location)?;

        Ok(Library {
            location,
            head_commit,
        })
    }

    pub fn open_for_commit(
        repo_path: &Path,
        uuid: &str,
        commit_id: Sha1,
    ) -> Result<Library, SeafError> {
        let location = Arc::new(LibraryLocation {
            repo_path: repo_path.to_path_buf(),
            uuid: uuid.to_string(),
        });
        let head_commit = find_commit(&location, commit_id)?;

        Ok(Library {
            location,
            head_commit,
        })
    }

    pub fn commit_iterator(&self) -> CommitIterator {
        commit_iterator(&self.location)
    }

    pub fn load_fs(&self, id: Sha1) -> Result<FsJson, SeafError> {
        if id == EMPTY_SHA1 {
            Ok(FsJson::Dir(EMPTY_DIR_JSON))
        } else {
            parse_fs_json(&self.obj_path("fs", id))
        }
    }

    pub fn fs_iterator(&self) -> FsIterator {
        FsIterator::new(self)
    }

    fn obj_path(&self, ty: &str, id: Sha1) -> PathBuf {
        full_obj_path(&self.location, ty, id)
    }

    pub fn file_by_id(&self, id: Sha1) -> Result<FileJson, SeafError> {
        self.load_fs(id)?.try_file()
    }

    pub fn file_reader(&self, file: &FileJson) -> Result<FileReader, SeafError> {
        let fbr = FileBlockReader::new(file, self.location.clone())?;
        Ok(FileReader::new(fbr))
    }
}

fn find_head_commit(ll: &LibraryLocation) -> Result<CommitJson, SeafError> {
    let mut head_commit: Option<CommitJson> = None;

    // The head commit is assumed to be the most recent commit
    for c in commit_iterator(ll) {
        let c = c?;

        if let Some(ref hc) = head_commit {
            if c.ctime > hc.ctime {
                head_commit = Some(c);
            }
        } else {
            head_commit = Some(c);
        }
    }

    head_commit.ok_or(SeafError::NoHeadCommit)
}

fn commit_iterator(ll: &LibraryLocation) -> CommitIterator {
    CommitIterator::new(&obj_type_path(ll, "commits"))
}

fn full_obj_path(ll: &LibraryLocation, ty: &str, id: Sha1) -> PathBuf {
    let id_str = id.to_string();
    obj_type_path(ll, ty).join(&id_str[0..2]).join(&id_str[2..])
}

fn obj_type_path(ll: &LibraryLocation, ty: &str) -> PathBuf {
    ll.repo_path.join(ty).join(&ll.uuid)
}

/// A cursor for walking through the filesystem
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
    /// Stack of directories "above" and including the current one. The last item is the current
    /// directory. The dirents vector of each dir is successively mutated to remove each visited
    /// dirent.
    stack: Vec<DirJson>,

    /// Path to the current directory (the last item in `stack`)
    path: PathBuf,
}

impl FsIterator<'_> {
    pub fn new(lib: &Library) -> FsIterator<'_> {
        let root_id = lib.head_commit.root_id;

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

    /// Stop walking the last/current directory and move one step up in the directory hierarchy
    pub fn prune(&mut self) {
        match &mut self.state {
            FsItState::Root(_) => self.clear(),
            FsItState::NotRoot(ref mut nr_state) => {
                nr_state.stack.pop();
                nr_state.path.pop();
            }
        }
    }

    /// Fast-forward to the end of the hierarchy.
    pub fn clear(&mut self) {
        self.state = FsItState::NotRoot(FsItNrState {
            stack: vec![],
            path: "".into(),
        });
    }
}

impl Iterator for FsIterator<'_> {
    type Item = Result<(PathBuf, DirentJson, FsJson), SeafError>;

    fn next(&mut self) -> Option<Self::Item> {
        self.next_result().transpose()
    }
}

fn find_commit(location: &LibraryLocation, id: Sha1) -> Result<CommitJson, SeafError> {
    let path = full_obj_path(location, "commits", id);
    parse_commit_file(&path)
}

fn parse_commit_file(filename: &Path) -> Result<CommitJson, SeafError> {
    let f = fs::File::open(filename).map_err(|e| SeafError::IO(filename.to_owned(), e))?;
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

                    return Some(parse_commit_file(de.path()));
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
            Err(SeafError::IO(_, e)) => Err(e),
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
    location: Arc<LibraryLocation>,
    block_ids: Vec<Sha1>,
    block_sizes: Vec<usize>,
    block_starts: Vec<usize>,
    size: usize,
}

impl FileBlockReader {
    fn new(file: &FileJson, location: Arc<LibraryLocation>) -> Result<FileBlockReader, SeafError> {
        let mut block_sizes = vec![];
        let mut block_starts = vec![];
        let mut pos = 0;

        for id in &file.block_ids {
            let path = full_obj_path(&location, "blocks", *id);
            let md = fs::metadata(&path).map_err(|e| SeafError::IO(path.to_owned(), e))?;
            let l = md.len() as usize;

            block_sizes.push(l);
            block_starts.push(pos);
            pos += l as usize;
        }

        Ok(FileBlockReader {
            location,
            block_ids: file.block_ids.clone(),
            block_sizes,
            block_starts,
            size: pos,
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

                    || -> Result<(), io::Error> {
                        let mut file = fs::File::open(&file_path)?;

                        file.seek(SeekFrom::Start(block_offset as u64))?;

                        file.read_exact(&mut buf[have_read..have_read + to_read_this_block])?;

                        Ok(())
                    }()
                    .map_err(|e| SeafError::IO(file_path.to_owned(), e))?;

                    have_read += to_read_this_block;
                    block_idx += 1;
                    block_offset = 0;
                }

                Ok(have_read)
            }
        }
    }

    fn find_start_block(&self, offset: u64) -> Option<(usize, usize)> {
        let offset = offset as usize;
        let next_block_idx = bisection::bisect_right(&self.block_starts, &offset);
        if next_block_idx == 0 {
            return None;
        }

        let block_idx = next_block_idx - 1;
        let block_start = self.block_starts[block_idx];
        assert!(offset >= block_start);

        let block_offset = offset - block_start;

        let block_size = self.block_sizes[block_idx];
        if block_offset < block_size {
            Some((block_idx, block_offset))
        } else {
            None
        }
    }
}

#[derive(Debug, Clone, Deserialize, PartialEq)]
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

#[derive(Debug, Deserialize, Clone, PartialEq)]
pub struct FileJson {
    pub block_ids: Vec<Sha1>,
    pub size: u64,
    #[serde(rename(deserialize = "type"))]
    pub ty: u32,
    pub version: u32,
}

#[derive(Debug, Deserialize, Clone, PartialEq)]
pub struct DirJson {
    pub dirents: Vec<DirentJson>,
    #[serde(rename(deserialize = "type"))]
    pub ty: u32,
    pub version: u32,
}

const EMPTY_DIR_JSON: DirJson = DirJson {
    dirents: vec![],
    ty: 0,
    version: 0,
};

#[derive(Debug, Deserialize, Clone, PartialEq)]
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
    let f = fs::File::open(filename).map_err(|e| SeafError::IO(filename.to_owned(), e))?;
    let dec = ZlibDecoder::new(f);
    let fs: FsJson =
        serde_json::from_reader(dec).map_err(|e| SeafError::ParseJson(filename.to_owned(), e))?;

    Ok(fs)
}

#[derive(Copy, Clone, PartialEq, Eq, Hash)]
pub struct Sha1 {
    words: [u32; 5],
}

const EMPTY_SHA1: Sha1 = Sha1 { words: [0; 5] };

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
    IO(PathBuf, std::io::Error),
    ParseJson(PathBuf, serde_json::Error),
    WalkDir(walkdir::Error),
    NotImpl,
    NoHeadCommit,
    WrongFsType,
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
