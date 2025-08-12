use clap::Parser;
use log::debug;
use simple_logger::SimpleLogger;
use std::fs;
use std::io;
use std::path::PathBuf;

use seafuse::*;

use crate::fuse::*;

mod fuse;

#[derive(clap::Parser, Debug)]
struct Args {
    op: Op,

    source: PathBuf,

    uuid: String,

    target: PathBuf,

    #[arg(short = 'n', long, default_value_t = false)]
    dry_run: bool,

    #[arg(short = 'v', long, default_value_t = false)]
    verbose: bool,
}

#[derive(Debug, Clone, clap::ValueEnum)]
enum Op {
    Extract,
    Mount,
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

    let lib = Library::open(&args.source, &args.uuid).unwrap();

    match args.op {
        Op::Extract => do_extract(&args, &lib),
        Op::Mount => do_mount(&args, &lib),
    };
}

fn do_extract(args: &Args, lib: &Library) {
    let mut file_counter = 0;
    let mut dir_counter = 0;

    fs::create_dir_all(&args.target).expect("Failed to create target directory");

    for r in lib.walk_fs() {
        let (p, de, fs) = r.expect("Failed to get fs entry");

        let full_parent = args.target.join(p);
        let full_path = full_parent.join(&de.name);

        debug!("Extracted {}: {}", fs.type_name(), full_path.display());

        if args.dry_run {
            continue;
        }

        match fs {
            FsJson::Dir(_) => {
                fs::create_dir(&full_path).unwrap_or_else(|e| {
                    panic!("Failed to create new directory {:?}: {:?}", &full_path, e)
                });

                dir_counter += 1;
            }
            FsJson::File(f) => {
                let path = full_parent.join(&de.name);
                let mut w = fs::File::create_new(&path)
                    .unwrap_or_else(|e| panic!("Failed to create new file {:?}: {:?}", &path, e));
                let mut r = lib
                    .file_by_json(&f)
                    .to_reader()
                    .unwrap_or_else(|e| panic!("Failed to open file ({f:?}) for reading: {e:?}"));

                io::copy(&mut r, &mut w).expect("Failed to copy data to new file");

                file_counter += 1;
            }
        }
    }

    if args.verbose {
        println!("Extracted {dir_counter} directories, {file_counter} files");
    }
}

fn do_mount(args: &Args, lib: &Library) {
    let fs = SeafFuse::new(lib.clone());

    fuser::mount2(fs, &args.target, &[])
        .unwrap_or_else(|e| panic!("Failed to mount {:?}: {:?}", &args.target, e));
}
