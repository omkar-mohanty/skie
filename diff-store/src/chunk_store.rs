use blake3::Hash;

use crate::Store;

pub struct HashStore {}

#[cfg(feature = "s3-storage")]
impl Store for HashStore {
    type Key = Hash;
    type Value = Vec<u8>;

    fn get(key: Self::Key) -> Option<Self::Value> {
        todo!()
    }

    fn insert(key: Self::Key, value: Self::Value) -> crate::Result<()> {
        todo!()
    }
}

#[cfg(feature = "local-storage")]
impl Store for HashStore {
    type Key = Hash;
    type Value = Vec<u8>;

    fn get(key: Self::Key) -> Option<Self::Value> {
        todo!()
    }

    fn insert(key: Self::Key, value: Self::Value) -> crate::Result<()> {
        todo!()
    }
}
