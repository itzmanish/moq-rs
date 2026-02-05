//! A broadcast is a collection of tracks, split into two handles: [Writer] and [Reader].
//!
//! The [Writer] can create tracks, either manually or on request.
//! It receives all requests by a [Reader] for a tracks that don't exist.
//! The simplest implementation is to close every unknown track with [ServeError::NotFound].
//!
//! A [Reader] can request tracks by name.
//! If the track already exists, it will be returned.
//! If the track doesn't exist, it will be sent to [Unknown] to be handled.
//! A [Reader] can be cloned to create multiple subscriptions.
//!
//! The broadcast is automatically closed with [ServeError::Done] when [Writer] is dropped, or all [Reader]s are dropped.
use std::{collections::HashMap, ops::Deref, sync::Arc};

use super::{ServeError, Track, TrackReader, TrackWriter};
use crate::coding::TrackNamespace;
use crate::watch::{Queue, State};

/// Full track identifier: namespace + track name
#[derive(Hash, Eq, PartialEq, Clone, Debug)]
pub struct FullTrackName {
    pub namespace: TrackNamespace,
    pub name: String,
}

/// Static information about a broadcast.
#[derive(Debug)]
pub struct Tracks {
    pub namespace: TrackNamespace,
}

impl Tracks {
    pub fn new(namespace: TrackNamespace) -> Self {
        Self { namespace }
    }

    pub fn produce(self) -> (TracksWriter, TracksRequest, TracksReader) {
        let info = Arc::new(self);
        let state = State::default().split();
        let queue = Queue::default().split();

        let writer = TracksWriter::new(state.0.clone(), info.clone());
        let request = TracksRequest::new(state.0, queue.0, info.clone());
        let reader = TracksReader::new(state.1, queue.1, info);

        (writer, request, reader)
    }
}

#[derive(Default)]
pub struct TracksState {
    tracks: HashMap<FullTrackName, TrackReader>,
}

/// Publish new tracks for a broadcast by name.
pub struct TracksWriter {
    state: State<TracksState>,
    pub info: Arc<Tracks>,
}

impl TracksWriter {
    fn new(state: State<TracksState>, info: Arc<Tracks>) -> Self {
        Self { state, info }
    }

    /// Create a new track with the given name, inserting it into the broadcast.
    /// The track will use this writer's namespace.
    /// None is returned if all [TracksReader]s have been dropped.
    pub fn create(&mut self, track: &str) -> Option<TrackWriter> {
        let (writer, reader) = Track {
            namespace: self.namespace.clone(),
            name: track.to_owned(),
        }
        .produce();

        // NOTE: We overwrite the track if it already exists.
        let full_name = FullTrackName {
            namespace: self.namespace.clone(),
            name: track.to_owned(),
        };
        self.state.lock_mut()?.tracks.insert(full_name, reader);

        Some(writer)
    }

    /// Remove a track from the broadcast by full name.
    pub fn remove(&mut self, namespace: &TrackNamespace, track_name: &str) -> Option<TrackReader> {
        let full_name = FullTrackName {
            namespace: namespace.clone(),
            name: track_name.to_owned(),
        };
        self.state.lock_mut()?.tracks.remove(&full_name)
    }

    /// Insert an existing track reader into the broadcast.
    /// Returns None if all readers have been dropped or if a track with this name already exists.
    pub fn insert(&mut self, reader: TrackReader) -> Option<()> {
        let full_name = FullTrackName {
            namespace: reader.namespace.clone(),
            name: reader.name.clone(),
        };
        let mut state = self.state.lock_mut()?;
        if state.tracks.contains_key(&full_name) {
            return None;
        }
        state.tracks.insert(full_name, reader);
        Some(())
    }
}

impl Deref for TracksWriter {
    type Target = Tracks;

    fn deref(&self) -> &Self::Target {
        &self.info
    }
}

pub struct TracksRequest {
    #[allow(dead_code)] // Avoid dropping the write side
    state: State<TracksState>,
    incoming: Option<Queue<TrackWriter>>,
    pub info: Arc<Tracks>,
}

impl TracksRequest {
    fn new(state: State<TracksState>, incoming: Queue<TrackWriter>, info: Arc<Tracks>) -> Self {
        Self {
            state,
            incoming: Some(incoming),
            info,
        }
    }

    /// Wait for a request to create a new track.
    /// None is returned if all [TracksReader]s have been dropped.
    pub async fn next(&mut self) -> Option<TrackWriter> {
        self.incoming.as_mut()?.pop().await
    }
}

impl Deref for TracksRequest {
    type Target = Tracks;

    fn deref(&self) -> &Self::Target {
        &self.info
    }
}

impl Drop for TracksRequest {
    fn drop(&mut self) {
        // Close any tracks still in the Queue
        for track in self.incoming.take().unwrap().close() {
            let _ = track.close(ServeError::not_found_ctx(
                "tracks request dropped before track handled",
            ));
        }
    }
}

/// Subscribe to a broadcast by requesting tracks.
///
/// This can be cloned to create handles.
#[derive(Clone)]
pub struct TracksReader {
    state: State<TracksState>,
    queue: Queue<TrackWriter>,
    pub info: Arc<Tracks>,
}

impl TracksReader {
    fn new(state: State<TracksState>, queue: Queue<TrackWriter>, info: Arc<Tracks>) -> Self {
        Self { state, queue, info }
    }

    /// Get a track from the broadcast by full name, if it exists.
    pub fn get_track_reader(
        &mut self,
        namespace: &TrackNamespace,
        track_name: &str,
    ) -> Option<TrackReader> {
        let state = self.state.lock();
        let full_name = FullTrackName {
            namespace: namespace.clone(),
            name: track_name.to_owned(),
        };

        if let Some(track_reader) = state.tracks.get(&full_name) {
            return Some(track_reader.clone());
        }
        None
    }

    /// Get or request a track from the broadcast by full name.
    /// The namespace parameter should be the full requested namespace, not just the announced prefix.
    /// None is returned if [TracksWriter] or [TracksRequest] cannot fufill the request.
    pub fn subscribe(
        &mut self,
        namespace: TrackNamespace,
        track_name: &str,
    ) -> Option<TrackReader> {
        let state = self.state.lock();
        let full_name = FullTrackName {
            namespace: namespace.clone(),
            name: track_name.to_owned(),
        };

        if let Some(track_reader) = state.tracks.get(&full_name) {
            return Some(track_reader.clone());
        }

        let mut state = state.into_mut()?;
        // Use the full requested namespace, not self.namespace
        let track_writer_reader = Track {
            namespace: namespace.clone(),
            name: track_name.to_owned(),
        }
        .produce();

        if self.queue.push(track_writer_reader.0).is_err() {
            return None;
        }

        // We requested the track sucessfully so we can deduplicate it by full name.
        state
            .tracks
            .insert(full_name, track_writer_reader.1.clone());

        Some(track_writer_reader.1.clone())
    }

    /// Forward an existing track writer to the upstream subscription queue.
    /// The writer will be received by [TracksRequest::next()].
    /// Returns None if the queue is closed.
    pub fn forward_upstream(&mut self, writer: TrackWriter) -> Option<()> {
        self.queue.push(writer).ok()
    }
}

impl Deref for TracksReader {
    type Target = Tracks;

    fn deref(&self) -> &Self::Target {
        &self.info
    }
}
