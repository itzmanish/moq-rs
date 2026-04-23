// SPDX-FileCopyrightText: 2024-2026 Cloudflare Inc., Luke Curley, Mike English and contributors
// SPDX-License-Identifier: MIT OR Apache-2.0

//! A stream is a stream of objects with a header, split into a [Writer] and [Reader] handle.
//!
//! A [Writer] writes an ordered stream of objects.
//! Each object can have a sequence number, allowing the reader to detect gaps objects.
//!
//! A [Reader] reads an ordered stream of objects.
//! The reader can be cloned, in which case each reader receives a copy of each object. (fanout)
//!
//! The stream is closed with [ServeError::Closed] when all writers or readers are dropped.
use std::{ops::Deref, sync::Arc};

use bytes::Bytes;

use crate::data::ObjectStatus;
use crate::serve::{GroupCache, GroupCacheReader, GroupCacheWriter};
use crate::watch::State;

use super::{ServeError, Track};

pub struct Subgroups {
    pub track: Arc<Track>,
}

/// Per-track limits applied to subgroup delivery.
///
/// These bound the memory a malicious or misbehaving upstream can consume by
/// rejecting additional objects / chunks once a subgroup hits its configured
/// cap. The defaults are intentionally generous for normal live media but
/// still prevent unbounded growth.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct SubgroupLimits {
    /// Maximum number of groups retained in the cache.
    pub max_cached_groups: usize,
    /// Maximum number of objects retained per subgroup.
    pub max_objects: usize,
    /// Maximum number of chunks retained per object.
    pub max_chunks: usize,
}

impl SubgroupLimits {
    pub const fn with_cache_depth(max_cached_groups: usize) -> Self {
        Self {
            max_cached_groups,
            max_objects: DEFAULT_MAX_OBJECTS_PER_SUBGROUP,
            max_chunks: DEFAULT_MAX_CHUNKS_PER_OBJECT,
        }
    }
}

impl Default for SubgroupLimits {
    fn default() -> Self {
        Self {
            max_cached_groups: DEFAULT_MAX_CACHED_GROUPS,
            max_objects: DEFAULT_MAX_OBJECTS_PER_SUBGROUP,
            max_chunks: DEFAULT_MAX_CHUNKS_PER_OBJECT,
        }
    }
}

impl Subgroups {
    pub fn produce(self) -> (SubgroupsWriter, SubgroupsReader) {
        self.produce_with_limits(SubgroupLimits::default())
    }

    pub fn produce_with_cache(
        self,
        max_cached_groups: usize,
    ) -> (SubgroupsWriter, SubgroupsReader) {
        self.produce_with_limits(SubgroupLimits::with_cache_depth(max_cached_groups))
    }

    pub fn produce_with_limits(self, limits: SubgroupLimits) -> (SubgroupsWriter, SubgroupsReader) {
        let (cache_writer, cache_reader) = GroupCache::produce(self.track.clone(), limits);

        let writer = SubgroupsWriter::new(cache_writer, self.track.clone());
        let reader = SubgroupsReader::new(cache_reader, self.track);

        (writer, reader)
    }
}

impl Deref for Subgroups {
    type Target = Track;

    fn deref(&self) -> &Self::Target {
        &self.track
    }
}

pub const DEFAULT_MAX_CACHED_GROUPS: usize = 16;
pub const DEFAULT_MAX_OBJECTS_PER_SUBGROUP: usize = 4096;
pub const DEFAULT_MAX_CHUNKS_PER_OBJECT: usize = 4096;

pub struct SubgroupsWriter {
    pub info: Arc<Track>,
    cache: GroupCacheWriter,
    next_subgroup_id: u64, // Not in the state to avoid a lock
    next_group_id: u64,    // Not in the state to avoid a lock
    last_group_id: u64,    // Not in the state to avoid a lock
    has_last_group: bool,
}

impl SubgroupsWriter {
    fn new(cache: GroupCacheWriter, track: Arc<Track>) -> Self {
        Self {
            info: track,
            cache,
            next_subgroup_id: 0,
            next_group_id: 0,
            last_group_id: 0,
            has_last_group: false,
        }
    }

    // Helper to increment the group by one.
    pub fn append(&mut self, priority: u8) -> Result<SubgroupWriter, ServeError> {
        let group_id;
        let subgroup_id;

        // TODO: refactor here... For now, every subgroup is mapped to a new group...
        let start_new_group = true;

        if start_new_group {
            group_id = self.next_group_id;
            subgroup_id = 0;
        } else {
            group_id = self.last_group_id;
            subgroup_id = self.next_subgroup_id;
        }

        self.create(Subgroup {
            group_id,
            subgroup_id,
            priority,
        })
    }

    pub fn create(&mut self, subgroup: Subgroup) -> Result<SubgroupWriter, ServeError> {
        if self.has_last_group
            && subgroup.group_id == self.last_group_id
            && self.next_subgroup_id != 0
            && subgroup.subgroup_id == self.next_subgroup_id - 1
        {
            return Err(ServeError::Duplicate);
        }

        let (writer, inserted) = self.cache.insert(subgroup.clone())?;

        if inserted {
            self.has_last_group = true;
            self.last_group_id = subgroup.group_id;
            // Use saturating_add to avoid panic on attacker-controlled u64 values
            // arriving from the wire (e.g. group_id == u64::MAX).
            self.next_group_id = subgroup.group_id.saturating_add(1);
            self.next_subgroup_id = subgroup.subgroup_id.saturating_add(1);
        }

        Ok(writer)
    }

    /// Close the segment with an error.
    pub fn close(self, err: ServeError) -> Result<(), ServeError> {
        self.cache.close(err)
    }
}

impl Deref for SubgroupsWriter {
    type Target = Track;

    fn deref(&self) -> &Self::Target {
        &self.info
    }
}

#[derive(Clone)]
pub struct SubgroupsReader {
    pub info: Arc<Track>,
    cache: GroupCacheReader,
}

impl SubgroupsReader {
    fn new(cache: GroupCacheReader, track_info: Arc<Track>) -> Self {
        Self {
            info: track_info,
            cache,
        }
    }

    pub async fn next(&mut self) -> Result<Option<SubgroupReader>, ServeError> {
        self.cache.next().await
    }

    // Returns the largest group/sequence
    pub fn latest(&self) -> Option<(u64, u64)> {
        self.cache.latest_group()
    }

    pub fn rewind_from(&self, start_group_id: u64) -> Self {
        Self {
            info: self.info.clone(),
            cache: self.cache.reader_from(start_group_id),
        }
    }
    pub fn available_rewind_groups(&self) -> u64 {
        self.cache.available_groups()
    }

    pub fn has_group(&self, group_id: u64) -> bool {
        self.cache.has_group(group_id)
    }

    pub fn oldest_group(&self) -> Option<u64> {
        self.cache.oldest_group()
    }

    /// Check if the subgroups writer has been closed or dropped.
    pub fn is_closed(&self) -> bool {
        self.cache.is_closed()
    }
}

impl Deref for SubgroupsReader {
    type Target = Track;

    fn deref(&self) -> &Self::Target {
        &self.info
    }
}

/// Parameters that can be specified by the user
#[derive(Debug, Clone, PartialEq)]
pub struct Subgroup {
    // The sequence number of the group within the track.
    // NOTE: These may be received out of order or with gaps.
    pub group_id: u64,

    // The sequence number of the subgroup within the group.
    // NOTE: These may be received out of order or with gaps.
    pub subgroup_id: u64,

    // The priority of the group within the track.
    pub priority: u8,
}

/// Static information about the group
#[derive(Debug, Clone, PartialEq)]
pub struct SubgroupInfo {
    pub track: Arc<Track>,

    // The sequence number of the group within the track.
    // NOTE: These may be received out of order or with gaps.
    pub group_id: u64,

    // The sequence number of the subgroup within the group.
    // NOTE: These may be received out of order or with gaps.
    pub subgroup_id: u64,

    // The priority of the group within the track.
    pub priority: u8,
}

impl SubgroupInfo {
    pub fn produce(self) -> (SubgroupWriter, SubgroupReader) {
        self.produce_with_limits(SubgroupLimits::default())
    }

    pub fn produce_with_limits(self, limits: SubgroupLimits) -> (SubgroupWriter, SubgroupReader) {
        let (writer, reader) = State::default().split();
        let info = Arc::new(self);

        let writer = SubgroupWriter::new(writer, info.clone(), limits);
        let reader = SubgroupReader::new(reader, info);

        (writer, reader)
    }
}

impl Deref for SubgroupInfo {
    type Target = Track;

    fn deref(&self) -> &Self::Target {
        &self.track
    }
}

struct SubgroupState {
    // The data that has been received thus far.
    objects: Vec<SubgroupObjectReader>,

    // Set when the writer or all readers are dropped.
    closed: Result<(), ServeError>,
}

impl Default for SubgroupState {
    fn default() -> Self {
        Self {
            objects: Vec::new(),
            closed: Ok(()),
        }
    }
}

/// Used to write data to a stream and notify readers.
pub struct SubgroupWriter {
    // Mutable stream state.
    state: State<SubgroupState>,

    // Immutable stream state.
    pub info: Arc<SubgroupInfo>,

    // The next object sequence number to use.
    next_object_id: u64,

    // Per-subgroup limits used to bound memory.
    limits: SubgroupLimits,
}

impl SubgroupWriter {
    fn new(state: State<SubgroupState>, group: Arc<SubgroupInfo>, limits: SubgroupLimits) -> Self {
        Self {
            state,
            info: group,
            next_object_id: 0,
            limits,
        }
    }

    /// Create the next object ID with the given payload.
    pub fn write(&mut self, payload: bytes::Bytes) -> Result<(), ServeError> {
        let mut object = self.create(payload.len(), None)?;
        object.write(payload)?;
        Ok(())
    }

    /// Write an object over multiple writes.
    ///
    /// BAD STUFF will happen if the size is wrong; this is an advanced feature.
    pub fn create(
        &mut self,
        size: usize,
        extension_headers: Option<crate::data::ExtensionHeaders>,
    ) -> Result<SubgroupObjectWriter, ServeError> {
        let (writer, reader) = SubgroupObject {
            group: self.info.clone(),
            object_id: self.next_object_id,
            status: ObjectStatus::NormalObject,
            size,
            extension_headers: extension_headers.unwrap_or_default(),
            limits: self.limits,
        }
        .produce();

        // Saturating to avoid overflow panic on pathological counters; the
        // per-subgroup object count cap below is the real bound.
        self.next_object_id = self.next_object_id.saturating_add(1);

        let mut state = self.state.lock_mut().ok_or(ServeError::Cancel)?;

        if state.objects.len() >= self.limits.max_objects {
            return Err(ServeError::Size);
        }

        state.objects.push(reader);

        Ok(writer)
    }

    /// Close the stream with an error.
    pub fn close(self, err: ServeError) -> Result<(), ServeError> {
        let state = self.state.lock();
        state.closed.clone()?;

        let mut state = state.into_mut().ok_or(ServeError::Cancel)?;
        state.closed = Err(err);
        Ok(())
    }

    pub fn len(&self) -> usize {
        self.state.lock().objects.len()
    }

    pub fn is_empty(&self) -> bool {
        self.len() == 0
    }
}

impl Deref for SubgroupWriter {
    type Target = SubgroupInfo;

    fn deref(&self) -> &Self::Target {
        &self.info
    }
}

/// Notified when a stream has new data available.
#[derive(Clone)]
pub struct SubgroupReader {
    // Modify the stream state.
    state: State<SubgroupState>,

    // Immutable stream state.
    pub info: Arc<SubgroupInfo>,

    // The number of chunks that we've read.
    // NOTE: Cloned readers inherit this index, but then run in parallel.
    read_index: usize,
}

impl SubgroupReader {
    fn new(state: State<SubgroupState>, subgroup: Arc<SubgroupInfo>) -> Self {
        Self {
            state,
            info: subgroup,
            read_index: 0,
        }
    }

    pub fn latest(&self) -> u64 {
        let state = self.state.lock();
        state
            .objects
            .last()
            .map(|o| o.object_id)
            .unwrap_or_default()
    }

    pub async fn read_next(&mut self) -> Result<Option<Bytes>, ServeError> {
        let object = self.next().await?;
        match object {
            Some(mut object) => Ok(Some(object.read_all().await?)),
            None => Ok(None),
        }
    }

    pub async fn next(&mut self) -> Result<Option<SubgroupObjectReader>, ServeError> {
        loop {
            {
                let state = self.state.lock();

                if self.read_index < state.objects.len() {
                    let object = state.objects[self.read_index].clone();
                    self.read_index += 1;
                    return Ok(Some(object));
                }

                state.closed.clone()?;
                match state.modified() {
                    Some(notify) => notify,
                    None => return Ok(None),
                }
            }
            .await; // Try again when the state changes
        }
    }

    pub fn pos(&self) -> usize {
        self.read_index
    }

    pub fn len(&self) -> usize {
        self.state.lock().objects.len()
    }

    pub fn is_empty(&self) -> bool {
        self.len() == 0
    }
}

impl Deref for SubgroupReader {
    type Target = SubgroupInfo;

    fn deref(&self) -> &Self::Target {
        &self.info
    }
}

/// A subset of Object, since we use the group's info.
#[derive(Clone, PartialEq, Debug)]
pub struct SubgroupObject {
    pub group: Arc<SubgroupInfo>,

    pub object_id: u64,

    // The size of the object.
    pub size: usize,

    // Object status
    pub status: ObjectStatus,

    // Extension headers (for draft-14 compliance, particularly immutable extensions)
    pub extension_headers: crate::data::ExtensionHeaders,

    // Per-object limits used to bound chunk buffering memory.
    #[doc(hidden)]
    pub limits: SubgroupLimits,
}

impl SubgroupObject {
    pub fn produce(self) -> (SubgroupObjectWriter, SubgroupObjectReader) {
        let limits = self.limits;
        let (writer, reader) = State::default().split();
        let info = Arc::new(self);

        let writer = SubgroupObjectWriter::new(writer, info.clone(), limits);
        let reader = SubgroupObjectReader::new(reader, info);

        (writer, reader)
    }
}

impl Deref for SubgroupObject {
    type Target = SubgroupInfo;

    fn deref(&self) -> &Self::Target {
        &self.group
    }
}

struct SubgroupObjectState {
    // The data that has been received thus far.
    chunks: Vec<Bytes>,

    // Set when the writer is dropped.
    closed: Result<(), ServeError>,
}

impl Default for SubgroupObjectState {
    fn default() -> Self {
        Self {
            chunks: Vec::new(),
            closed: Ok(()),
        }
    }
}

/// Used to write data to a segment and notify readers.
pub struct SubgroupObjectWriter {
    // Mutable segment state.
    state: State<SubgroupObjectState>,

    // Immutable segment state.
    pub info: Arc<SubgroupObject>,

    // The amount of promised data that has yet to be written.
    remain: usize,

    // Per-object chunk limit.
    limits: SubgroupLimits,
}

impl SubgroupObjectWriter {
    /// Create a new segment with the given info.
    fn new(
        state: State<SubgroupObjectState>,
        object: Arc<SubgroupObject>,
        limits: SubgroupLimits,
    ) -> Self {
        Self {
            state,
            remain: object.size,
            info: object,
            limits,
        }
    }

    /// Write a new chunk of bytes.
    pub fn write(&mut self, chunk: Bytes) -> Result<(), ServeError> {
        if chunk.len() > self.remain {
            return Err(ServeError::Size);
        }
        self.remain -= chunk.len();

        let mut state = self.state.lock_mut().ok_or(ServeError::Cancel)?;

        if state.chunks.len() >= self.limits.max_chunks {
            return Err(ServeError::Size);
        }

        state.chunks.push(chunk);

        Ok(())
    }

    /// Close the segment with an error.
    pub fn close(self, err: ServeError) -> Result<(), ServeError> {
        if self.remain != 0 {
            return Err(ServeError::Size);
        }

        let state = self.state.lock();
        state.closed.clone()?;

        let mut state = state.into_mut().ok_or(ServeError::Cancel)?;
        state.closed = Err(err);

        Ok(())
    }
}

impl Drop for SubgroupObjectWriter {
    fn drop(&mut self) {
        if self.remain == 0 {
            return;
        }

        if let Some(mut state) = self.state.lock_mut() {
            state.closed = Err(ServeError::Size);
        }
    }
}

impl Deref for SubgroupObjectWriter {
    type Target = SubgroupObject;

    fn deref(&self) -> &Self::Target {
        &self.info
    }
}

/// Notified when a segment has new data available.
#[derive(Clone)]
pub struct SubgroupObjectReader {
    // Modify the segment state.
    state: State<SubgroupObjectState>,

    // Immutable segment state.
    pub info: Arc<SubgroupObject>,

    // The number of chunks that we've read.
    // NOTE: Cloned readers inherit this index, but then run in parallel.
    index: usize,
}

impl SubgroupObjectReader {
    fn new(state: State<SubgroupObjectState>, object: Arc<SubgroupObject>) -> Self {
        Self {
            state,
            info: object,
            index: 0,
        }
    }

    /// Block until the next chunk of bytes is available.
    pub async fn read(&mut self) -> Result<Option<Bytes>, ServeError> {
        loop {
            {
                let state = self.state.lock();

                if self.index < state.chunks.len() {
                    let chunk = state.chunks[self.index].clone();
                    self.index += 1;
                    return Ok(Some(chunk));
                }

                state.closed.clone()?;
                match state.modified() {
                    Some(notify) => notify,
                    None => return Ok(None), // No more changes will come
                }
            }
            .await; // Try again when the state changes
        }
    }

    pub async fn read_all(&mut self) -> Result<Bytes, ServeError> {
        let mut chunks = Vec::new();
        while let Some(chunk) = self.read().await? {
            chunks.push(chunk);
        }

        Ok(Bytes::from(chunks.concat()))
    }
}

impl Deref for SubgroupObjectReader {
    type Target = SubgroupObject;

    fn deref(&self) -> &Self::Target {
        &self.info
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::coding::TrackNamespace;

    fn make_track() -> Arc<Track> {
        Arc::new(Track::new(
            TrackNamespace::from_utf8_path("test/ns"),
            "video".to_string(),
        ))
    }

    fn write_group(writer: &mut SubgroupsWriter, group_id: u64) -> SubgroupWriter {
        writer
            .create(Subgroup {
                group_id,
                subgroup_id: 0,
                priority: 0,
            })
            .unwrap()
    }

    #[tokio::test]
    async fn forward_reader_receives_new_groups() {
        let track = make_track();
        let (mut writer, mut reader) = Subgroups { track }.produce();

        let _w0 = write_group(&mut writer, 0);
        let _w1 = write_group(&mut writer, 1);

        let g = reader.next().await.unwrap().unwrap();
        assert_eq!(g.group_id, 0);

        let g = reader.next().await.unwrap().unwrap();
        assert_eq!(g.group_id, 1);
    }

    #[tokio::test]
    async fn rewind_reader_starts_from_past_group() {
        let track = make_track();
        let (mut writer, reader) = Subgroups { track }.produce();

        let _w0 = write_group(&mut writer, 0);
        let _w1 = write_group(&mut writer, 1);
        let _w2 = write_group(&mut writer, 2);
        let _w3 = write_group(&mut writer, 3);

        let mut rewound = reader.rewind_from(1);

        let g = rewound.next().await.unwrap().unwrap();
        assert_eq!(g.group_id, 1);

        let g = rewound.next().await.unwrap().unwrap();
        assert_eq!(g.group_id, 2);

        let g = rewound.next().await.unwrap().unwrap();
        assert_eq!(g.group_id, 3);
    }

    #[tokio::test]
    async fn cache_eviction_drops_oldest() {
        let track = make_track();
        let (mut writer, reader) = Subgroups { track }.produce_with_cache(3);

        let _w0 = write_group(&mut writer, 0);
        let _w1 = write_group(&mut writer, 1);
        let _w2 = write_group(&mut writer, 2);
        let _w3 = write_group(&mut writer, 3);

        assert!(!reader.has_group(0));
        assert!(reader.has_group(1));
        assert!(reader.has_group(2));
        assert!(reader.has_group(3));
        assert_eq!(reader.oldest_group(), Some(1));
    }

    #[tokio::test]
    async fn available_rewind_groups_count() {
        let track = make_track();
        let (mut writer, reader) = Subgroups { track }.produce();

        assert_eq!(reader.available_rewind_groups(), 0);

        let _w0 = write_group(&mut writer, 0);
        assert_eq!(reader.available_rewind_groups(), 0);

        let _w1 = write_group(&mut writer, 1);
        assert_eq!(reader.available_rewind_groups(), 1);

        let _w2 = write_group(&mut writer, 2);
        assert_eq!(reader.available_rewind_groups(), 2);
    }

    #[tokio::test]
    async fn rewind_and_forward_readers_independent() {
        let track = make_track();
        let (mut writer, mut forward_reader) = Subgroups { track }.produce();

        let _w0 = write_group(&mut writer, 0);
        let _w1 = write_group(&mut writer, 1);
        let _w2 = write_group(&mut writer, 2);

        let mut rewind_reader = forward_reader.rewind_from(0);

        let fg = forward_reader.next().await.unwrap().unwrap();
        assert_eq!(fg.group_id, 0);

        let rg = rewind_reader.next().await.unwrap().unwrap();
        assert_eq!(rg.group_id, 0);

        let fg = forward_reader.next().await.unwrap().unwrap();
        assert_eq!(fg.group_id, 1);

        let rg = rewind_reader.next().await.unwrap().unwrap();
        assert_eq!(rg.group_id, 1);
    }

    #[test]
    fn duplicate_group_id_returns_error() {
        let track = make_track();
        let (mut writer, _reader) = Subgroups { track }.produce();

        let _w0 = write_group(&mut writer, 0);
        let result = writer.create(Subgroup {
            group_id: 0,
            subgroup_id: 0,
            priority: 0,
        });
        assert!(matches!(result, Err(ServeError::Duplicate)));
    }

    #[test]
    fn stale_group_id_silently_accepted() {
        let track = make_track();
        let (mut writer, _reader) = Subgroups { track }.produce();

        let _w1 = write_group(&mut writer, 1);
        let result = writer.create(Subgroup {
            group_id: 0,
            subgroup_id: 0,
            priority: 0,
        });
        assert!(result.is_ok());
    }

    #[test]
    fn max_group_id_does_not_panic() {
        let track = make_track();
        let (mut writer, _reader) = Subgroups { track }.produce();

        // An attacker-controlled group_id == u64::MAX must not panic (debug)
        // or silently wrap (release). saturating_add keeps next_group_id at MAX.
        let result = writer.create(Subgroup {
            group_id: u64::MAX,
            subgroup_id: u64::MAX,
            priority: 0,
        });
        assert!(result.is_ok());
    }

    #[test]
    fn max_objects_limit_rejects_new_objects() {
        let track = make_track();
        let limits = SubgroupLimits {
            max_cached_groups: 2,
            max_objects: 2,
            max_chunks: DEFAULT_MAX_CHUNKS_PER_OBJECT,
        };
        let (mut writer, _reader) = Subgroups { track }.produce_with_limits(limits);

        let mut subgroup = writer
            .create(Subgroup {
                group_id: 0,
                subgroup_id: 0,
                priority: 0,
            })
            .unwrap();

        // Two writes should succeed.
        subgroup.write(Bytes::from_static(b"a")).unwrap();
        subgroup.write(Bytes::from_static(b"b")).unwrap();

        // Third write must be rejected with Size.
        let err = subgroup.write(Bytes::from_static(b"c")).unwrap_err();
        assert!(matches!(err, ServeError::Size));
    }

    #[test]
    fn max_chunks_limit_rejects_new_chunks() {
        let track = make_track();
        let limits = SubgroupLimits {
            max_cached_groups: 2,
            max_objects: DEFAULT_MAX_OBJECTS_PER_SUBGROUP,
            max_chunks: 2,
        };
        let (mut writer, _reader) = Subgroups { track }.produce_with_limits(limits);

        let mut subgroup = writer
            .create(Subgroup {
                group_id: 0,
                subgroup_id: 0,
                priority: 0,
            })
            .unwrap();

        // Reserve a large object we will fill chunk-by-chunk.
        let mut object = subgroup.create(6, None).unwrap();
        object.write(Bytes::from_static(b"aa")).unwrap();
        object.write(Bytes::from_static(b"bb")).unwrap();

        // Third chunk exceeds max_chunks and must be rejected.
        let err = object.write(Bytes::from_static(b"cc")).unwrap_err();
        assert!(matches!(err, ServeError::Size));
    }
}
