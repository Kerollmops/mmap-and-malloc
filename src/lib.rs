use byte_unit::Byte;

pub const DATABASE_SIZE: Byte = Byte::from_bytes(5 * 1024 * 1024 * 1024); // 5GiB
pub const DATABASE_NAME: &str = "random.mdb";
