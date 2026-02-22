use anyhow::Result;
use common::*;
use crossbeam_channel::unbounded;
use notify_debouncer_full::{
    new_debouncer,
    notify::{EventKind, RecursiveMode},
};
use serde::{Deserialize, Serialize};
use std::{fs, path::PathBuf, time::Duration};
use store::ChunkConfig;

#[derive(Deserialize, Serialize)]
struct ServiceConfig {
    chunk_config: ChunkConfig,
    sync_dir: Vec<PathBuf>,
    debounce_ms: u64,
}

impl Default for ServiceConfig {
    fn default() -> Self {
        Self {
            chunk_config: ChunkConfig::default(),
            sync_dir: Vec::default(),
            debounce_ms: 500,
        }
    }
}

#[tokio::main]
async fn main() -> Result<()> {
    let app_config = {
        let sync_dir = get_default_sync_path();
        let config_dir = sync_dir.join(".config");
        let config_path = config_dir.join("config.toml");

        // 1. Ensure the sync dir and its internal .config dir exist
        if !config_dir.exists() {
            fs::create_dir_all(&config_dir)?;

            // Windows: Hide the directory itself
            #[cfg(windows)]
            {
                let mut cmd = std::process::Command::new("attrib");
                cmd.arg("+h").arg(&config_dir);
                let _ = cmd.status();
            }
        }

        if !config_path.exists() {
            let config = ServiceConfig::default();
            let config_string = toml::to_string(&config)?;
            fs::write(&config_path, config_string)?;
            config
        } else {
            let contents = fs::read(&config_path)?;
            toml::from_slice::<ServiceConfig>(&contents)?
        }
    };

    let (event_sender, event_receiver) = unbounded();

    let mut debounder = new_debouncer(
        Duration::from_millis(app_config.debounce_ms),
        None,
        event_sender,
    )
    .unwrap();

    for dir in &app_config.sync_dir {
        debounder.watch(&dir, RecursiveMode::Recursive)?;
    }

    while let Ok(events) = event_receiver.recv()? {
        //TODO Need to handle a special case where the sync directory is deleted while skie is running.
        let events_iter = events.iter().filter(|debounced_event| {
            // 1. Only care about data-changing events
            let is_valid_kind = matches!(
                debounced_event.event.kind,
                EventKind::Create(_) | EventKind::Modify(_) | EventKind::Remove(_)
            );

            // 2. Ignore anything inside the .config folder
            let is_not_internal = !debounced_event
                .event
                .paths
                .iter()
                .any(|path| path.components().any(|c| c.as_os_str() == ".config"));

            is_valid_kind && is_not_internal
        });
    }

    Ok(())
}
