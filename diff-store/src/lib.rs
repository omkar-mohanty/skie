mod chunk_store;
mod files_store;

pub(crate) type Result<E> = std::result::Result<E, Box<dyn std::error::Error>>;
pub trait Store {
    type Key;
    type Value;

    fn insert(key: Self::Key, value: Self::Value) -> Result<()>;
    fn get(key: Self::Key) -> Option<Self::Value>;
}
