use std::{fs, mem};

use heed::byteorder::BE;
use heed::types::{ByteSlice, I64};
use heed::{Database, EnvOpenOptions};
use rand::{Fill, Rng};

fn main() -> anyhow::Result<()> {
    let path = "random.mdb";
    let database_size = 5 * 1024 * 1024 * 1024; // 5GiB

    fs::create_dir_all(path)?;
    let env = EnvOpenOptions::new()
        .map_size(database_size * 2) // Just to make sure internal pages are taken into account
        .open(path)?;

    let mut wtxn = env.write_txn()?;
    let db: Database<I64<BE>, ByteSlice> = env.create_database(&mut wtxn, None)?;

    let mut rng = rand::thread_rng();
    let mut buffer = vec![0; 1024];
    let mut entries_size = 0;

    for i in 0.. {
        let value = random_fill_buffer(&mut rng, &mut buffer);
        db.put(&mut wtxn, &i, value)?;

        entries_size += mem::size_of_val(&i) + value.len();
        if entries_size >= database_size {
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
