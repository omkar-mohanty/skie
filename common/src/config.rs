use directories::UserDirs;
use std::path::PathBuf;

const SKIE_SYNC_DIR_NAME: &str = "Skie";

pub fn get_default_sync_path() -> PathBuf {
    if let Some(user_dirs) = UserDirs::new() {
        // Try to get Documents first
        if let Some(docs) = user_dirs.document_dir() {
            return docs.join(SKIE_SYNC_DIR_NAME);
        }
        return user_dirs.home_dir().join(SKIE_SYNC_DIR_NAME);
    }

    // Absolute final fallback: current directory
    PathBuf::from(SKIE_SYNC_DIR_NAME)
}
