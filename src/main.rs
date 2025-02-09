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

    let mut file_counter = 0;
    let mut dir_counter = 0;

    fs::create_dir_all(&args.target).expect("Failed to create target directory");

    for r in lib.walk_fs() {
        let (p, de, fs) = r.expect("Failed to get fs entry");

        let full_parent = args.target.join(p);
        let full_path = full_parent.join(&de.name);

        if args.verbose {
            println!("{} - {}", full_path.display(), fs.type_name());
        }

        if args.dry_run {
            continue;
        }

        match fs {
            Fs::Dir(_) => {
                fs::create_dir(&full_path).unwrap_or_else(|e| {
                    panic!("Failed to create new directory {:?}: {:?}", &full_path, e)
                });

                dir_counter += 1;
            }
            Fs::File(f) => {
                let path = full_parent.join(&de.name);
                let mut w = fs::File::create_new(&path)
                    .unwrap_or_else(|e| panic!("Failed to create new file {:?}: {:?}", &path, e));
                let mut r = lib.open_file(&f);

                io::copy(&mut r, &mut w).expect("Failed to copy data to new file");

                file_counter += 1;
            }
        }
    }

    println!(
        "Extracted {} directories, {} files",
        dir_counter, file_counter
    );
}
