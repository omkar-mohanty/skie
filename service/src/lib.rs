mod reactor;

use camino::Utf8PathBuf;
use std::{time::Instant, iter::Peekable };
use notify_debouncer_full::{notify::event::{EventKind, Event}, DebouncedEvent};

pub struct OsEvent {
    pub kind: EventKind,
    pub paths: Vec<Utf8PathBuf>,
    pub time: Instant,
}

pub fn build_events_iter<I: Iterator<Item = DebouncedEvent>>(
    mut events: Peekable<I>,
) -> Vec<OsEvent> {
    let mut os_events = vec![];
    while let Some(debounced_event) = events.next() {
        use EventKind::*;
        let DebouncedEvent { event, time } = debounced_event;
        let Event { kind, paths, .. } = event;

        let paths: Vec<Utf8PathBuf> = paths
            .into_iter()
            .map(|path| Utf8PathBuf::from_path_buf(path).unwrap())
            .collect();

        if let Remove(_) = kind {
            // Check if the NEXT event is a Create for the SAME path
            let is_atomic_update = events.peek().map_or(false, |next_event| {
                matches!(next_event.event.kind, Create(_)) && next_event.event.paths == paths // Paths must match!
            });

            if is_atomic_update {
                // Consume the paired 'Create' event from the iterator
                events.next();

                os_events.push(OsEvent {
                    kind,
                    paths,
                    time,
                });
            } else {
                // It was just a normal delete
                os_events.push(OsEvent {
                    kind,
                    paths,
                    time,
                });
            }
        } else {
            // Not a remove, map directly
            let kind = kind.into();
            os_events.push(OsEvent { kind, paths, time });
        }
    }
    os_events
}

