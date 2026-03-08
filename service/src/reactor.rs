// SPDX-License-Identifier: GPL-3.0-or-later

use std::sync::Arc;

use anyhow::{Ok, Result};
use camino::Utf8PathBuf;
use common::FileID;
use notify_debouncer_full::notify::{EventKind, event::RemoveKind};
use store::{DataStore, Fetch, FileTableEntry};
use crate::OsEvent;

pub struct Reactor {
    store: Arc<DataStore>,
}

impl Reactor {
async fn handle_event(&self, os_event: OsEvent) -> Result<()> {
    let kind = os_event.kind;
    let paths = os_event.paths;

    use EventKind::*;
    match kind {
        Remove(remove_kind) => self.handle_remove(remove_kind, paths).await?,
        Create(create_event) => todo!(),
        Modify(modify_event) => todo!(),
        Access(access_event) => todo!(),
        Any => todo!(),
        Other => todo!(),
    }

    Ok(())
}

async fn handle_remove(&self, remove_kind: RemoveKind, paths: Vec<Utf8PathBuf>) -> Result<()> {
    use RemoveKind::*;
    let keys = self.store.fetch_many(keys);
    match remove_kind {
        Folder => self.remove_folder(paths).await?,
        _ => {
        },
    };
    Ok(())
}

async fn remove_folder(&self, keys: &[FileID], paths: Vec<Utf8PathBuf>) -> Result<()> {
    let files: Vec<FileTableEntry> = self.store.fetch_many(keys).await?;
    todo!();
    Ok(())
}
}

