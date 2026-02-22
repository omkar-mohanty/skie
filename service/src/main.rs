use anyhow::Result;
use common::*;
use crossbeam_channel::unbounded;
use notify_debouncer_full::{
    new_debouncer,
    notify::{EventKind, RecursiveMode, event::RemoveKind},
};
use std::{fs, time::Duration};

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
            let config = AppConfig::default();
            let config_string = toml::to_string(&config)?;
            fs::write(&config_path, config_string)?;
            config
        } else {
            let contents = fs::read(&config_path)?;
            toml::from_slice::<AppConfig>(&contents)?
        }
    };

    if !app_config.sync_dir.exists() {
        fs::create_dir_all(&app_config.sync_dir)?;
    }

    let context = SkieContext::new(app_config);
    let (event_sender, event_receiver) = unbounded();

    let mut debounder = new_debouncer(
        Duration::from_millis(context.app_config.debounce_ms),
        None,
        event_sender,
    )
    .unwrap();

    debounder.watch(&context.app_config.sync_dir, RecursiveMode::Recursive)?;

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

        for event in events_iter {
            // This is for an edge case when skie has started running and during operation the sync directory was removed.
            // In this weird edge case it is better to restart skie than to continue operation.
            if let EventKind::Remove(remove_kind) = &event.kind
                && let RemoveKind::Folder = remove_kind
                && event
                    .paths
                    .iter()
                    .any(|path| path.eq(&context.app_config.sync_dir))
            {
                anyhow::bail!("The sync directory was removed! Please restart the service!");
            }
        }
    }

    Ok(())
}
