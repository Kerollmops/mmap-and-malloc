use std::path::PathBuf;
use std::{fs, mem};

use clap::Parser;
use heed::byteorder::BE;
use heed::types::{ByteSlice, U64};
use heed::{Database, EnvOpenOptions};
use mmap_and_malloc::{DATABASE_NAME, DATABASE_SIZE};
use rand::{Fill, Rng};

#[derive(Parser)]
#[command(author, version, about, long_about = None)]
struct Opt {
    /// The path where the database is stored.
    #[arg(long, default_value = DATABASE_NAME)]
    path: PathBuf,

    /// The size of the database that will be generated.
    #[arg(long, default_value_t = DATABASE_SIZE)]
    size: usize,
}

fn main() -> anyhow::Result<()> {
    let Opt { path, size } = Opt::parse();

    fs::create_dir_all(&path)?;
    let env = EnvOpenOptions::new()
        .map_size(size * 2) // Just to make sure internal pages are taken into account
        .open(path)?;

    let mut wtxn = env.write_txn()?;
    let db: Database<U64<BE>, ByteSlice> = env.create_database(&mut wtxn, None)?;

    let mut rng = rand::thread_rng();
    let mut buffer = vec![0; 1024];
    let mut entries_size = 0;

    for i in 0.. {
        let value = random_fill_buffer(&mut rng, &mut buffer);
        db.put(&mut wtxn, &i, value)?;

        entries_size += mem::size_of_val(&i) + value.len();
        if entries_size >= size {
            break;
        }
    }

    wtxn.commit()?;

    Ok(())
}

fn random_fill_buffer<'b, R: Rng>(rng: &mut R, buffer: &'b mut [u8]) -> &'b [u8] {
    let size = rng.gen_range(0..buffer.len());
    buffer[..size].try_fill(rng).unwrap();
    &buffer[..size]
}
