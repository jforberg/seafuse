use bimap::BiMap;
use core::time::Duration;
use fuser::{
    FileAttr, FileType, Filesystem, ReplyAttr, ReplyData, ReplyDirectory, ReplyEmpty, ReplyEntry,
    Request,
};
use libc::{c_int, EINVAL, EIO, ENOENT, ENOTDIR};
use log::debug;
use std::cmp::min;
use std::ffi::{OsStr, OsString};
use std::io::{Read, Seek, SeekFrom};
use std::time::UNIX_EPOCH;

use seafuse::*;

const INF_TTL: Duration = Duration::new(1_000_000_000, 0);

#[derive(Debug)]
pub struct SeafFuse {
    lib: Library,
    ino_table: BiMap<u64, Sha1>,
    ino_counter: u64,
}

#[derive(Debug, Clone)]
struct Dentry {
    ino: u64,
    kind: FileType,
    name: OsString,
}

impl SeafFuse {
    pub fn new(lib: Library) -> SeafFuse {
        let root_id = lib.head_commit.as_ref().unwrap().root_id;

        SeafFuse {
            lib,
            ino_table: BiMap::from_iter([(1, root_id)]),
            ino_counter: 2,
        }
    }

    fn do_lookup(&mut self, parent_ino: u64, name: &OsStr) -> Result<FileAttr, c_int> {
        let parent_id = *self.ino_table.get_by_left(&parent_ino).ok_or(EIO)?;
        let parent_fs = self.lib.load_fs(parent_id).map_err(|_e| EINVAL)?;
        let parent_dir = parent_fs.try_dir().map_err(|_e| ENOTDIR)?;

        for de in &parent_dir.dirents {
            if Some(de.name.as_ref()) != name.to_str() {
                continue;
            }

            return self.do_getattr_by_id(de.id);
        }

        Err(ENOENT)
    }

    fn do_getattr_by_id(&mut self, id: Sha1) -> Result<FileAttr, c_int> {
        let ino = self.lookup_or_add_ino(id);
        self.do_getattr_by_ino(ino)
    }

    fn do_getattr_by_ino(&self, ino: u64) -> Result<FileAttr, c_int> {
        let id = *self.ino_table.get_by_left(&ino).ok_or(EIO)?;
        let fs = self.lib.load_fs(id).map_err(|_e| EIO)?;

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

    fn do_readdir(&mut self, ino: u64) -> Result<Vec<Dentry>, c_int> {
        let id = *self.ino_table.get_by_left(&ino).ok_or(EIO)?;
        let fs = self.lib.load_fs(id).map_err(|_e| EIO)?;
        let d = fs.try_dir().map_err(|_e| ENOTDIR)?;
        let mut results = vec![];

        for de in d.dirents {
            let de_ino = self.lookup_or_add_ino(de.id);
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

    fn lookup_or_add_ino(&mut self, id: Sha1) -> u64 {
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

    fn do_read(&mut self, ino: u64, offset: i64, size: u32) -> Result<Vec<u8>, c_int> {
        let id = *self.ino_table.get_by_left(&ino).ok_or(EIO)?;
        let f = self.lib.file_by_id(id).map_err(|_e| EIO)?;
        let mut fr = f.to_reader().map_err(|_e| EIO)?;
        let mut buf = vec![0; size as usize];

        // XXX Is it correct to cast to u64 here? Does negative offset mean something?
        fr.seek(SeekFrom::Start(offset as u64))
            .map_err(|_e| EINVAL)?;
        let r = fr.read(&mut buf).map_err(|_e| EIO)?;

        buf.truncate(r);

        Ok(buf)
    }
}

impl Filesystem for SeafFuse {
    fn access(&mut self, _req: &Request, _ino: u64, _mask: i32, reply: ReplyEmpty) {
        reply.ok();
    }

    fn lookup(&mut self, _req: &Request, parent: u64, name: &OsStr, reply: ReplyEntry) {
        match self.do_lookup(parent, name) {
            Ok(attr) => {
                reply.entry(&INF_TTL, &attr, 0);
                0
            }
            Err(r) => {
                reply.error(r);
                r
            }
        };
    }

    fn getattr(&mut self, _req: &Request, ino: u64, _fh: Option<u64>, reply: ReplyAttr) {
        match self.do_getattr_by_ino(ino) {
            Ok(attr) => {
                reply.attr(&INF_TTL, &attr);
                0
            }
            Err(r) => {
                reply.error(r);
                r
            }
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
                0
            }
            Err(r) => {
                reply.error(r);
                r
            }
        };
    }

    fn read(
        &mut self,
        _req: &Request,
        ino: u64,
        _fh: u64,
        offset: i64,
        size: u32,
        _flags: i32,
        _lock_owner: Option<u64>,
        reply: ReplyData,
    ) {
        match self.do_read(ino, offset, size) {
            Ok(buf) => {
                debug!(
                    "read(): Reply with {} bytes: {}...",
                    buf.len(),
                    sample_bytes(&buf)
                );
                reply.data(&buf);
            }
            Err(r) => {
                reply.error(r);
            }
        }
    }
}

fn sample_bytes(buf: &[u8]) -> String {
    let slice = &buf[0..min(buf.len(), 32)];
    let escaped_bytes = escape_bytes::escape(slice);
    String::from_utf8(escaped_bytes).unwrap()
}
