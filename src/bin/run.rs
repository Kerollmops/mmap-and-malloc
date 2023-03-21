use std::path::PathBuf;
use std::{fs, mem};

use byte_unit::Byte;
use clap::{Parser, ValueEnum};
use heed::byteorder::BE;
use heed::types::{ByteSlice, U64};
use heed::{Database, EnvOpenOptions};
use mmap_and_malloc::{DATABASE_NAME, DATABASE_SIZE};
use rand::prelude::SliceRandom;
use rand::{Fill, Rng};

#[derive(Parser)]
#[command(author, version, about, long_about = None)]
struct Opt {
    /// The path where the database is stored.
    #[arg(long, default_value = DATABASE_NAME)]
    path: PathBuf,

    /// The mode used to fetch the entries.
    #[arg(value_enum, long)]
    fetch_method: FetchMethod,

    /// The size of the buffer allocated while fetching data.
    ///
    /// The buffer is filled with random data.
    #[arg(long)]
    allocate: Byte,
}

fn main() -> anyhow::Result<()> {
    let Opt { path, fetch_method, allocate } = Opt::parse();

    fs::create_dir_all(&path)?;
    let env = EnvOpenOptions::new().map_size(DATABASE_SIZE).open(path)?;

    let rtxn = env.read_txn()?;
    let db: Database<U64<BE>, ByteSlice> = env.open_database(&rtxn, None)?.unwrap();
    let mut entries_size = 0;

    // We fill the buffer with random data to be sure the OS doesn't ignore it
    // but effectively store it in the RSS.
    let mut rng = rand::thread_rng();
    let mut noise_buffer = vec![0; allocate.get_bytes().try_into().unwrap()];
    noise_buffer.as_mut_slice().try_fill(&mut rng)?;

    match fetch_method {
        FetchMethod::Iterative => {
            for result in db.iter(&rtxn)? {
                let (k, v) = result?;
                entries_size += mem::size_of_val(&k) + v.len();
            }
        }
        FetchMethod::Random => {
            let number_of_entries = db.len(&rtxn)?;
            let mut number_of_fetches = 0;

            while number_of_fetches < number_of_entries {
                let key = rng.gen_range(0..number_of_entries);
                let value = db.get(&rtxn, &key)?.unwrap();
                entries_size += mem::size_of_val(&key) + value.len();
                number_of_fetches += 1;
            }
        }
        FetchMethod::Shuffled => {
            let number_of_entries = db.len(&rtxn)?;
            let mut keys: Vec<_> = (0..number_of_entries).collect();

            keys.shuffle(&mut rng);

            for key in keys {
                let value = db.get(&rtxn, &key)?.unwrap();
                entries_size += mem::size_of_val(&key) + value.len();
            }
        }
    }

    println!("The amount of data fetched is about {} bytes.", entries_size);

    Ok(())
}

#[derive(Copy, Clone, PartialEq, Eq, PartialOrd, Ord, ValueEnum)]
enum FetchMethod {
    /// Iterate over all the entries from the lowest to the highest key.
    Iterative,
    /// Iterate over the entries randomly without any garanty to fetch all the keys
    /// but to fetch the number of keys in the database.
    Random,
    /// Iterate over all the entries randomly (keys are listed and shuffled).
    ///
    /// This mode allocate more memory to store the keys to shuffle.
    /// Those keys are represneted as i64s.
    Shuffled,
}
