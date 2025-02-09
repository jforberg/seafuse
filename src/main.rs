use clap::Parser;
use std::fs;
use std::io;
use std::path::PathBuf;

use seafrepo::*;

#[derive(clap::Parser, Debug)]
struct Args {
    source: PathBuf,

    uuid: String,

    target: PathBuf,

    #[arg(short = 'n', long, default_value_t = false)]
    dry_run: bool,

    #[arg(short = 'v', long, default_value_t = false)]
    verbose: bool,
}

fn main() {
    let args = Args::parse();

    let mut lib = Library::new(&args.source, &args.uuid);

    if args.verbose {
        println!("Looking for head commit...");
    }

    lib = lib.populate().expect("find head commit");
    let head = lib.head_commit.as_ref().unwrap();

    if args.verbose {
        println!("Repo name: {}", head.repo_name);
        println!("Head commit: {}", head.commit_id);
        println!("Root: {}", head.root_id);
        println!("Last modified: {}, by {}", head.ctime, head.creator_name);
    }

    for r in lib.walk_fs() {
        let (p, de, fs) = r.expect("get next fs entry");

        let full_parent = args.target.join(p);
        let full_path = full_parent.join(&de.name);

        if args.verbose {
            println!("{} - {}", full_path.display(), fs.type_name());
        }

        if args.dry_run {
            continue;
        }

        fs::create_dir_all(&full_parent).expect("create parent directories");
        match fs {
            Fs::Dir(_) => {
                match fs::create_dir(de.name) {
                    Ok(()) => (),
                    Err(e) if e.kind() == io::ErrorKind::AlreadyExists => (),
                    e => e.expect("create new directory"),
                };
            }
            Fs::File(f) => {
                let mut w =
                    fs::File::create_new(full_parent.join(&de.name)).expect("create new file");
                let mut r = lib.open_file(&f);

                io::copy(&mut r, &mut w).expect("copy data to new file");
            }
        }
    }
}
