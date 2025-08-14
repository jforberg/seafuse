use bimap::BiMap;
use core::time::Duration;
use fuser::{
    FileAttr, FileType, Filesystem, ReplyAttr, ReplyData, ReplyDirectory, ReplyEmpty, ReplyEntry,
    ReplyOpen, Request, FUSE_ROOT_ID,
};
use libc::{c_int, EBADF, EINVAL, EIO, ENOENT, ENOTDIR};
use log::{debug, error};
use std::cmp::min;
use std::collections::HashMap;
use std::ffi::{OsStr, OsString};
use std::io::{Read, Seek, SeekFrom};
use std::time::UNIX_EPOCH;

use crate::repo::*;

const INF_TTL: Duration = Duration::new(1_000_000_000, 0);

/// Instance of a mounted seafuse filesystem
#[derive(Debug)]
pub struct SeafFuse {
    /// Description of the mounted library
    lib: Library,

    /// Mapping between inode numbers and FS hashes used by seafile
    ino_table: BiMap<u64, Sha1>,

    /// Table of currently open files, indexed by file handle
    open_file_table: HashMap<u64, OpenFile>,

    /// The next inode number to be allocated
    ino_counter: u64,

    /// The next file handle to be used
    file_handle_counter: u64,
}

/// Directory entry
#[derive(Debug, Clone)]
pub struct Dentry {
    pub ino: u64,
    pub kind: FileType,
    pub name: OsString,
}

#[derive(Debug)]
struct OpenFile {
    reader: FileReader,
}

/// Intermediate trait to make the fuse implementation testable
pub trait PreFilesystem {
    fn do_lookup(&mut self, parent_ino: u64, name: &OsStr) -> Result<FileAttr, c_int>;
    fn do_getattr(&self, ino: u64) -> Result<FileAttr, c_int>;
    fn do_readdir(&mut self, ino: u64) -> Result<Vec<Dentry>, c_int>;
    fn do_open(&mut self, ino: u64) -> Result<u64, c_int>;
    fn do_release(&mut self, fh: u64) -> Result<(), c_int>;
    fn do_read(&mut self, ino: u64, offset: i64, size: u32) -> Result<Vec<u8>, c_int>;
}

impl SeafFuse {
    pub fn new(lib: Library) -> SeafFuse {
        let root_id = lib.head_commit.as_ref().unwrap().root_id;

        SeafFuse {
            lib,
            ino_table: BiMap::from_iter([(FUSE_ROOT_ID, root_id)]),
            open_file_table: HashMap::new(),
            ino_counter: FUSE_ROOT_ID + 1,
            file_handle_counter: 1,
        }
    }

    fn lookup_attr_by_id(&mut self, id: Sha1) -> Result<FileAttr, c_int> {
        let ino = self.add_ino(id);
        self.lookup_attr_by_ino(ino)
    }

    fn lookup_attr_by_ino(&self, ino: u64) -> Result<FileAttr, c_int> {
        let id = self.lookup_id_by_ino(ino)?;
        let fs = self.lookup_fs(id)?;

        match fs {
            FsJson::Dir(_) => Ok(FileAttr {
                ino,
                size: 0,
                blocks: 0,
                atime: UNIX_EPOCH,
                mtime: UNIX_EPOCH,
                ctime: UNIX_EPOCH,
                crtime: UNIX_EPOCH,
                kind: FileType::Directory,
                perm: 0o755,
                nlink: 1,
                uid: 0,
                gid: 0,
                rdev: 0,
                blksize: 0,
                flags: 0,
            }),
            FsJson::File(f) => Ok(FileAttr {
                ino,
                size: f.size,
                blocks: 0,
                atime: UNIX_EPOCH,
                mtime: UNIX_EPOCH,
                ctime: UNIX_EPOCH,
                crtime: UNIX_EPOCH,
                kind: FileType::RegularFile,
                perm: 0o644,
                nlink: 1,
                uid: 0,
                gid: 0,
                rdev: 0,
                blksize: 0,
                flags: 0,
            }),
        }
    }

    fn lookup_id_by_ino(&self, ino: u64) -> Result<Sha1, c_int> {
        match self.ino_table.get_by_left(&ino) {
            None => {
                error!("Inode {ino} does not exist");
                Err(EIO)
            }
            Some(id) => Ok(*id),
        }
    }

    fn add_ino(&mut self, id: Sha1) -> u64 {
        match self.ino_table.get_by_right(&id) {
            Some(ino) => *ino,
            None => {
                let ino = self.ino_counter;
                self.ino_counter += 1;
                self.ino_table.insert(ino, id);
                ino
            }
        }
    }

    fn lookup_file(&self, id: Sha1) -> Result<FileJson, c_int> {
        self.lookup_fs(id)?.try_file().map_err(|e| {
            error!("Fs {id} is not a file: {e:?}");
            EINVAL
        })
    }

    fn lookup_dir(&self, id: Sha1) -> Result<DirJson, c_int> {
        self.lookup_fs(id)?.try_dir().map_err(|e| {
            error!("Fs {id} is not a dir: {e:?}");
            ENOTDIR
        })
    }

    fn lookup_fs(&self, id: Sha1) -> Result<FsJson, c_int> {
        self.lib.load_fs(id).map_err(|e| {
            error!("Failed to load Fs with id {id}: {e:?}");
            EINVAL
        })
    }

    fn get_open_file(&mut self, fh: u64) -> Result<&mut OpenFile, c_int> {
        match self.open_file_table.get_mut(&fh) {
            Some(of) => Ok(of),
            None => {
                error!("Bad file handle {fh}");
                Err(EBADF)
            }
        }
    }
}

impl PreFilesystem for SeafFuse {
    fn do_lookup(&mut self, parent_ino: u64, name: &OsStr) -> Result<FileAttr, c_int> {
        let parent_id = self.lookup_id_by_ino(parent_ino)?;
        let parent_dir = self.lookup_dir(parent_id)?;

        for de in &parent_dir.dirents {
            if Some(de.name.as_ref()) != name.to_str() {
                continue;
            }

            return self.lookup_attr_by_id(de.id);
        }

        Err(ENOENT)
    }

    fn do_getattr(&self, ino: u64) -> Result<FileAttr, c_int> {
        self.lookup_attr_by_ino(ino)
    }

    fn do_readdir(&mut self, ino: u64) -> Result<Vec<Dentry>, c_int> {
        let id = self.lookup_id_by_ino(ino)?;
        let dir = self.lookup_dir(id)?;
        let mut results = vec![];

        for de in dir.dirents {
            let de_ino = self.add_ino(de.id);
            let de_fs = self.lib.load_fs(de.id).map_err(|_e| EIO)?;

            results.push(Dentry {
                ino: de_ino,
                kind: match de_fs {
                    FsJson::Dir(_) => FileType::Directory,
                    FsJson::File(_) => FileType::RegularFile,
                },
                name: OsString::from(de.name),
            });
        }

        Ok(results)
    }

    fn do_open(&mut self, ino: u64) -> Result<u64, c_int> {
        while self.open_file_table.contains_key(&self.file_handle_counter) {
            self.file_handle_counter += 1;
        }

        let id = self.lookup_id_by_ino(ino)?;
        let file = self.lookup_file(id)?;
        let reader = self.lib.file_reader(&file).map_err(|e| {
            error!("Failed to open file {id} for reading: {e:?}");
            EIO
        })?;
        let of = OpenFile { reader };

        let fh = self.file_handle_counter;
        self.file_handle_counter += 1;

        self.open_file_table.insert(fh, of);

        debug!("Open file fh={fh} id={id}");

        Ok(fh)
    }

    fn do_release(&mut self, fh: u64) -> Result<(), c_int> {
        debug!("Close file fh={fh}");

        match self.open_file_table.remove(&fh) {
            Some(_) => Ok(()),
            None => {
                error!("Invalid file handle {fh}");
                Err(EBADF)
            }
        }
    }

    fn do_read(&mut self, fh: u64, offset: i64, size: u32) -> Result<Vec<u8>, c_int> {
        let of = self.get_open_file(fh)?;
        let mut buf = vec![0; size as usize];

        // XXX Is it correct to cast to u64 here? Does negative offset mean something?
        of.reader
            .seek(SeekFrom::Start(offset as u64))
            .map_err(|_e| EINVAL)?;
        let r = of.reader.read(&mut buf).map_err(|_e| EIO)?;

        buf.truncate(r);

        debug!("Read {} bytes: {}...", r, sample_bytes(&buf));
        Ok(buf)
    }
}

impl Filesystem for SeafFuse {
    fn access(&mut self, _req: &Request, _ino: u64, _mask: i32, reply: ReplyEmpty) {
        reply.ok();
    }

    fn lookup(&mut self, _req: &Request, parent: u64, name: &OsStr, reply: ReplyEntry) {
        match self.do_lookup(parent, name) {
            Ok(attr) => reply.entry(&INF_TTL, &attr, 0),
            Err(r) => reply.error(r),
        };
    }

    fn getattr(&mut self, _req: &Request, ino: u64, _fh: Option<u64>, reply: ReplyAttr) {
        match self.do_getattr(ino) {
            Ok(attr) => reply.attr(&INF_TTL, &attr),
            Err(r) => reply.error(r),
        };
    }

    fn readdir(
        &mut self,
        _req: &Request,
        ino: u64,
        _fh: u64,
        offset: i64,
        mut reply: ReplyDirectory,
    ) {
        match self.do_readdir(ino) {
            Ok(dentries) => {
                for (i, d) in dentries.into_iter().enumerate() {
                    let i = (i + 1) as i64;
                    if i <= offset {
                        continue;
                    }

                    if reply.add(d.ino, i, d.kind, d.name.clone()) {
                        break;
                    }
                }

                reply.ok();
            }
            Err(r) => reply.error(r),
        };
    }

    fn open(&mut self, _req: &Request<'_>, ino: u64, flags: i32, reply: ReplyOpen) {
        match self.do_open(ino) {
            Ok(fh) => reply.opened(fh, flags as u32), // XXX why is a cast needed?
            Err(r) => reply.error(r),
        }
    }

    fn release(
        &mut self,
        _req: &Request<'_>,
        _ino: u64,
        fh: u64,
        _flags: i32,
        _lock_owner: Option<u64>,
        _flush: bool,
        reply: ReplyEmpty,
    ) {
        match self.do_release(fh) {
            Ok(_) => reply.ok(),
            Err(r) => reply.error(r),
        }
    }

    fn read(
        &mut self,
        _req: &Request,
        _ino: u64,
        fh: u64,
        offset: i64,
        size: u32,
        _flags: i32,
        _lock_owner: Option<u64>,
        reply: ReplyData,
    ) {
        match self.do_read(fh, offset, size) {
            Ok(buf) => reply.data(&buf),
            Err(r) => reply.error(r),
        }
    }
}

/// Get the first few bytes of the array, formatted as string
fn sample_bytes(buf: &[u8]) -> String {
    let slice = &buf[0..min(buf.len(), 32)];
    let escaped_bytes = escape_bytes::escape(slice);
    String::from_utf8(escaped_bytes).unwrap()
}
