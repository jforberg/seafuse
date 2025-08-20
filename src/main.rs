use chrono::{DateTime, Utc};
use clap::Parser;
use log::debug;
use simple_logger::SimpleLogger;
use std::cmp::{max, min};
use std::fs;
use std::io;
use std::io::ErrorKind;
use std::path::{Path, PathBuf};
use std::time::{Duration, UNIX_EPOCH};

use seafuse::*;

#[derive(clap::Parser, Debug)]
struct Args {
    #[clap(subcommand)]
    op: Op,

    #[arg(short = 'v', long, default_value_t = false)]
    verbose: bool,
}

#[derive(Debug, Clone, clap::Subcommand)]
enum Op {
    Extract {
        source: PathBuf,

        uuid: String,

        target: PathBuf,

        #[arg(short = 'p', long)]
        prefix: Option<PathBuf>,

        #[arg(short = 'n', long, default_value_t = false)]
        dry_run: bool,
    },
    Mount {
        source: PathBuf,

        uuid: String,

        target: PathBuf,
    },
    Stats {
        source: PathBuf,

        uuid: String,
    },
}

#[derive(Debug, PartialEq)]
enum PrefixMatch {
    Yes,
    No,
    Continue,
}

fn main() {
    let args = Args::parse();

    let log_level = if args.verbose {
        log::LevelFilter::Debug
    } else {
        log::LevelFilter::Warn
    };
    SimpleLogger::new()
        .with_level(log_level)
        .without_timestamps()
        .env()
        .init()
        .unwrap();

    match args.op {
        Op::Extract {
            source,
            uuid,
            target,
            prefix,
            dry_run,
        } => do_extract(
            &source,
            &uuid,
            &target,
            &prefix.unwrap_or("".into()),
            dry_run,
        ),
        Op::Mount {
            source,
            uuid,
            target,
        } => do_mount(&source, &uuid, &target),
        Op::Stats { source, uuid } => do_stats(&source, &uuid),
    };
}

fn do_extract(source: &Path, uuid: &str, target: &Path, prefix: &Path, dry_run: bool) {
    let lib = Library::open(source, uuid).unwrap();
    let mut file_counter = 0;
    let mut dir_counter = 0;

    fs::create_dir_all(target).expect("Failed to create target directory");

    let mut it = lib.fs_iterator();
    while let Some(r) = it.next() {
        let (p, de, fs) = r.expect("Failed to get fs entry");
        let full_path = p.join(&de.name);
        let target_path = target.join(full_path);

        match match_prefix(prefix, &p) {
            PrefixMatch::Yes => {}
            PrefixMatch::No => {
                debug!("Pruning directory {p:?}");
                it.prune(); // XXX doesn't work properly
                continue;
            }
            PrefixMatch::Continue => {
                debug!("Ignoring directory {p:?}");
                continue;
            }
        }

        debug!("Extracting {}: {}", fs.type_name(), target_path.display());

        if dry_run {
            continue;
        }

        if let Some(parent) = target_path.parent() {
            fs::create_dir_all(parent).unwrap();
        }

        match fs {
            FsJson::Dir(_) => {
                let r = match fs::create_dir(&target_path) {
                    Err(e) => {
                        if e.kind() == ErrorKind::AlreadyExists {
                            Ok(())
                        } else {
                            Err(e)
                        }
                    }
                    x => x,
                };

                r.unwrap_or_else(|e| {
                    panic!("Failed to create new directory {:?}: {:?}", &target_path, e)
                });

                dir_counter += 1;
            }
            FsJson::File(f) => {
                let mut w = fs::File::create(&target_path).unwrap_or_else(|e| {
                    panic!("Failed to create file {:?}: {:?}", &target_path, e)
                });
                let mut r = lib
                    .file_reader(&f)
                    .unwrap_or_else(|e| panic!("Failed to open file ({f:?}) for reading: {e:?}"));

                io::copy(&mut r, &mut w).expect("Failed to copy data to new file");

                file_counter += 1;
            }
        }
    }

    println!("Extracted {dir_counter} directories, {file_counter} files");
}

fn match_prefix(pref: &Path, path: &Path) -> PrefixMatch {
    let ret = if pref.as_os_str().is_empty() || path.starts_with(pref) {
        PrefixMatch::Yes
    } else if pref.starts_with(path) {
        PrefixMatch::Continue
    } else {
        PrefixMatch::No
    };

    ret
}

fn do_mount(source: &Path, uuid: &str, target: &Path) {
    let lib = Library::open(source, uuid).unwrap();
    let fs = SeafFuse::new(lib.clone());

    fuser::mount2(fs, target, &[])
        .unwrap_or_else(|e| panic!("Failed to mount {:?}: {:?}", &target, e));
}

fn do_stats(source: &Path, uuid: &str) {
    let lib = Library::open(source, uuid).unwrap();
    let head_commit_id = lib.head_commit.commit_id;
    let repo_name = &lib.head_commit.repo_name;
    println!("Head commit: {head_commit_id}");
    println!("Repo name: {repo_name}");

    let mut commit_count = 0;
    let mut min_ctime = u64::MAX;
    let mut max_ctime = 0;

    for c in lib.commit_iterator().map(|c| c.unwrap()) {
        commit_count += 1;
        min_ctime = min(min_ctime, c.ctime);
        max_ctime = max(max_ctime, c.ctime);
    }

    println!("Commit count: {commit_count}");

    let oldest_timestamp = format_unix_time(min_ctime);
    let newest_timestamp = format_unix_time(max_ctime);
    println!("Oldest timestamp: {oldest_timestamp}");
    println!("Newest timestamp: {newest_timestamp}");

    let mut file_count = 0;
    let mut dir_count = 0;
    let mut max_blocks_in_file = 0;
    let mut max_files_in_dir = 0;

    for (_p, _de, fs) in lib.fs_iterator().map(|fs| fs.unwrap()) {
        match fs {
            FsJson::File(f) => {
                file_count += 1;
                max_blocks_in_file = max(max_blocks_in_file, f.block_ids.len());
            }
            FsJson::Dir(d) => {
                dir_count += 1;
                max_files_in_dir = max(max_files_in_dir, d.dirents.len());
            }
        }
    }

    println!("File count: {file_count}");
    println!("Directory count: {dir_count}");
    println!("Max blocks in a file: {max_blocks_in_file}");
    println!("Max files in a directory: {max_files_in_dir}");
}

fn format_unix_time(t: u64) -> String {
    let st = UNIX_EPOCH + Duration::from_secs(t);
    let dt = DateTime::<Utc>::from(st);

    dt.to_rfc3339()
}

#[test]
fn test_prefix_examples() {
    assert_eq!(match_prefix(Path::new(""), Path::new("test/test2")), PrefixMatch::Yes);
    assert_eq!(match_prefix(Path::new(""), Path::new("")), PrefixMatch::Yes);
    assert_eq!(match_prefix(Path::new("test"), Path::new("")), PrefixMatch::Continue);

    assert_eq!(match_prefix(Path::new("test"), Path::new("test/test2")), PrefixMatch::Yes);
    assert_eq!(match_prefix(Path::new("test2"), Path::new("test")), PrefixMatch::No);
    assert_eq!(match_prefix(Path::new("test2"), Path::new("test/test2")), PrefixMatch::No);
    assert_eq!(match_prefix(Path::new("test/test2"), Path::new("test")), PrefixMatch::Continue);
    assert_eq!(match_prefix(Path::new("test/test2"), Path::new("test/test2")), PrefixMatch::Yes);
    assert_eq!(match_prefix(Path::new("test/test2"), Path::new("test/test")), PrefixMatch::No);
}
