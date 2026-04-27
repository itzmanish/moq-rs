use std::collections::{BTreeMap, VecDeque};
use std::future::Future;
use std::pin::Pin;
use std::sync::Arc;
use std::time::Instant;

use parking_lot::RwLock;
use tokio::sync::watch;

use crate::serve::{
    ServeError, Subgroup, SubgroupInfo, SubgroupLimits, SubgroupReader, SubgroupWriter, Track,
};

// ============================================================================
// Public traits
// ============================================================================

/// A per-track cache provider.
///
/// `GroupCache` acts as a factory: `produce()` is called once per subgroup-mode
/// track and must return a fresh, independent set of writer/reader handles for
/// that track. Implementations **must not** share per-track cache state between
/// separate `produce()` calls unless that is explicitly intended.
///
/// The canonical implementation is [`DefaultGroupCache`].  Custom
/// implementations can be injected at track-creation time (see
/// [`Subgroups::produce_with_group_cache`]) or configured relay-wide via
/// [`RelayConfig::group_cache`].
pub trait GroupCache: Send + Sync + 'static {
    /// Create a fresh cache instance for one track and return its writer/reader
    /// pair.  Called exactly once per subgroup-mode track.
    fn produce(
        &self,
        track: Arc<Track>,
        limits: SubgroupLimits,
    ) -> (Box<dyn GroupCacheWriter>, Box<dyn GroupCacheReader>);
}

/// Write side of a per-track group cache.
///
/// Implementations must also handle cleanup in `Drop`: mark the cache closed
/// and wake any waiting readers so they can drain remaining data and then
/// receive `None`.
pub trait GroupCacheWriter: Send + 'static {
    /// Insert a new subgroup into the cache.
    ///
    /// Returns the subgroup writer (for the publisher to stream objects into)
    /// and a bool indicating whether the group was actually inserted (false
    /// means it was a duplicate).
    fn insert(&mut self, subgroup: Subgroup) -> Result<(SubgroupWriter, bool), ServeError>;

    /// Consume and close this cache writer with the given error, then wake all
    /// waiting readers.
    ///
    /// This uses `self: Box<Self>` because subgroup writers store cache writers
    /// as trait objects; consuming the `Box` ensures the writer is not usable
    /// after close.
    fn close(self: Box<Self>, err: ServeError) -> Result<(), ServeError>;
}

/// Read side of a per-track group cache.
pub trait GroupCacheReader: Send + 'static {
    /// Clone this reader, producing an independent cursor over the same cache.
    ///
    /// Live-mode clones start at the latest cached arrival (not the future
    /// edge) so that late-joining subscribers receive the most recent group
    /// immediately.
    fn clone_box(&self) -> Box<dyn GroupCacheReader>;

    /// Return the next subgroup from this reader.
    ///
    /// Live readers follow arrival order; replay readers walk by group-id
    /// order and automatically transition to live when they exhaust cached
    /// history.  Returns `Ok(None)` when the writer is closed/dropped and all
    /// cached data has been consumed.
    fn next<'a>(
        &'a mut self,
    ) -> Pin<Box<dyn Future<Output = Result<Option<SubgroupReader>, ServeError>> + Send + 'a>>;

    /// Create a replay reader starting at `start_group_id`.  The replay reader
    /// walks cached groups in group-id order, then hands off to live delivery
    /// when it catches up.
    fn reader_from(&self, start_group_id: u64) -> Box<dyn GroupCacheReader>;

    /// Number of cached groups available before the live edge (excludes the
    /// latest group).
    fn available_groups(&self) -> u64;

    /// Whether `group_id` is present in the cache.
    fn has_group(&self, group_id: u64) -> bool;

    /// Timestamp at which `group_id` was inserted into the cache, if present.
    fn group_created_at(&self, group_id: u64) -> Option<Instant>;

    /// `(group_id, latest_object_id)` of the newest cached group, if any.
    fn latest_group(&self) -> Option<(u64, u64)>;

    /// `group_id` of the oldest group still in the cache, if any.
    fn oldest_group(&self) -> Option<u64>;

    /// Whether the cache writer has been closed or dropped.
    fn is_closed(&self) -> bool;
}

impl Clone for Box<dyn GroupCacheReader> {
    fn clone(&self) -> Self {
        self.clone_box()
    }
}

// ============================================================================
// Default implementation
// ============================================================================

/// Default [`GroupCache`] provider.
///
/// Creates an in-memory, arrival-ordered live queue + group-id-ordered replay
/// index backed by a `RwLock` and `tokio::sync::watch` for efficient fanout.
pub struct DefaultGroupCache;

impl GroupCache for DefaultGroupCache {
    fn produce(
        &self,
        track: Arc<Track>,
        limits: SubgroupLimits,
    ) -> (Box<dyn GroupCacheWriter>, Box<dyn GroupCacheReader>) {
        let (state, rx) = CacheState::new(limits.max_cached_groups);
        let writer = DefaultGroupCacheWriter {
            track,
            state: state.clone(),
            limits,
        };
        let reader = DefaultGroupCacheReader {
            state,
            cursor: ReaderCursor::Live {
                next_arrival_seq: 0,
            },
            notify_rx: rx,
        };
        (Box::new(writer), Box::new(reader))
    }
}

// ── shared internal state ────────────────────────────────────────────────────

struct CacheState {
    data: RwLock<CacheInner>,
    notify_tx: watch::Sender<u64>,
}

impl CacheState {
    fn new(max_cached_groups: usize) -> (Arc<Self>, watch::Receiver<u64>) {
        let (tx, rx) = watch::channel(0);
        let state = Arc::new(Self {
            data: RwLock::new(CacheInner::new(max_cached_groups)),
            notify_tx: tx,
        });
        (state, rx)
    }

    fn subscribe(&self) -> watch::Receiver<u64> {
        self.notify_tx.subscribe()
    }
}

type Entry = Arc<CachedGroup>;

struct CacheInner {
    arrivals: VecDeque<Entry>,
    arrival_base_seq: u64,
    next_arrival_seq: u64,
    max_live_arrivals: usize,
    by_group: BTreeMap<u64, Entry>,
    max_replay_groups: usize,
    closed: Result<(), ServeError>,
    writer_dropped: bool,
    epoch: u64,
}

impl CacheInner {
    fn new(max_cached_groups: usize) -> Self {
        Self {
            arrivals: VecDeque::new(),
            arrival_base_seq: 0,
            next_arrival_seq: 0,
            max_live_arrivals: max_cached_groups,
            by_group: BTreeMap::new(),
            max_replay_groups: max_cached_groups,
            closed: Ok(()),
            writer_dropped: false,
            epoch: 0,
        }
    }

    fn insert(&mut self, group_id: u64, reader: SubgroupReader) -> bool {
        if self.by_group.contains_key(&group_id) {
            return false;
        }

        let entry = Arc::new(CachedGroup {
            group_id,
            arrival_seq: self.next_arrival_seq,
            reader,
            created_at: Instant::now(),
        });
        self.next_arrival_seq = self.next_arrival_seq.saturating_add(1);

        self.arrivals.push_back(entry.clone());
        self.by_group.insert(group_id, entry);

        self.evict_arrivals();
        self.evict_replay();
        true
    }

    fn evict_arrivals(&mut self) {
        if self.max_live_arrivals == 0 {
            return;
        }
        while self.arrivals.len() > self.max_live_arrivals {
            if let Some(front) = self.arrivals.pop_front() {
                self.arrival_base_seq = front.arrival_seq.saturating_add(1);
            }
        }
        if self.arrivals.is_empty() {
            self.arrival_base_seq = self.next_arrival_seq;
        }
    }

    fn evict_replay(&mut self) {
        if self.max_replay_groups == 0 {
            return;
        }
        while self.by_group.len() > self.max_replay_groups {
            self.by_group.pop_first();
        }
    }

    fn next_live_from(&self, next_arrival_seq: u64) -> Option<Entry> {
        if self.arrivals.is_empty() {
            return None;
        }
        let seq = next_arrival_seq.max(self.arrival_base_seq);
        let offset = (seq - self.arrival_base_seq) as usize;
        self.arrivals.get(offset).cloned()
    }

    fn next_replay_from(&self, next_group_id: u64) -> Option<Entry> {
        self.by_group
            .range(next_group_id..)
            .next()
            .map(|(_, e)| e.clone())
    }

    fn available_groups(&self) -> u64 {
        self.by_group.len().saturating_sub(1) as u64
    }

    fn oldest_group_id(&self) -> Option<u64> {
        self.by_group.keys().next().copied()
    }

    fn newest_group(&self) -> Option<Entry> {
        self.by_group.iter().next_back().map(|(_, e)| e.clone())
    }

    fn latest_arrival_seq(&self) -> Option<u64> {
        self.arrivals.back().map(|e| e.arrival_seq)
    }

    fn get(&self, group_id: u64) -> Option<Entry> {
        self.by_group.get(&group_id).cloned()
    }
}

struct CachedGroup {
    group_id: u64,
    arrival_seq: u64,
    reader: SubgroupReader,
    created_at: Instant,
}

// ── cursor ───────────────────────────────────────────────────────────────────

enum ReaderCursor {
    Live {
        next_arrival_seq: u64,
    },
    Replay {
        next_group_id: u64,
        max_seen_arrival_seq: u64,
    },
}

// ── DefaultGroupCacheWriter ──────────────────────────────────────────────────

pub struct DefaultGroupCacheWriter {
    track: Arc<Track>,
    state: Arc<CacheState>,
    limits: SubgroupLimits,
}

impl GroupCacheWriter for DefaultGroupCacheWriter {
    fn insert(&mut self, subgroup: Subgroup) -> Result<(SubgroupWriter, bool), ServeError> {
        let info = SubgroupInfo {
            track: self.track.clone(),
            group_id: subgroup.group_id,
            subgroup_id: subgroup.subgroup_id,
            priority: subgroup.priority,
        };

        let (writer, reader) = info.produce_with_limits(self.limits);

        let mut cache = self.state.data.write();
        cache.closed.clone()?;

        let inserted = cache.insert(subgroup.group_id, reader);

        if inserted {
            cache.epoch = cache.epoch.saturating_add(1);
            let epoch = cache.epoch;
            drop(cache);
            let _ = self.state.notify_tx.send(epoch);
        } else {
            drop(cache);
        }

        Ok((writer, inserted))
    }

    fn close(self: Box<Self>, err: ServeError) -> Result<(), ServeError> {
        let mut cache = self.state.data.write();
        cache.closed.clone()?;
        cache.closed = Err(err.clone());
        cache.writer_dropped = true;
        cache.epoch = cache.epoch.saturating_add(1);
        let epoch = cache.epoch;
        drop(cache);
        let _ = self.state.notify_tx.send(epoch);
        Ok(())
    }
}

impl Drop for DefaultGroupCacheWriter {
    fn drop(&mut self) {
        let mut cache = self.state.data.write();
        if cache.writer_dropped {
            return;
        }
        cache.writer_dropped = true;
        cache.epoch = cache.epoch.saturating_add(1);
        let epoch = cache.epoch;
        drop(cache);
        let _ = self.state.notify_tx.send(epoch);
    }
}

// ── DefaultGroupCacheReader ──────────────────────────────────────────────────

pub struct DefaultGroupCacheReader {
    state: Arc<CacheState>,
    cursor: ReaderCursor,
    notify_rx: watch::Receiver<u64>,
}

impl GroupCacheReader for DefaultGroupCacheReader {
    fn clone_box(&self) -> Box<dyn GroupCacheReader> {
        let notify_rx = self.state.subscribe();
        match self.cursor {
            ReaderCursor::Live { .. } => {
                let cache = self.state.data.read();
                let next_arrival_seq = cache.latest_arrival_seq().unwrap_or(cache.next_arrival_seq);
                Box::new(DefaultGroupCacheReader {
                    state: self.state.clone(),
                    cursor: ReaderCursor::Live { next_arrival_seq },
                    notify_rx,
                })
            }
            ReaderCursor::Replay {
                next_group_id,
                max_seen_arrival_seq,
            } => Box::new(DefaultGroupCacheReader {
                state: self.state.clone(),
                cursor: ReaderCursor::Replay {
                    next_group_id,
                    max_seen_arrival_seq,
                },
                notify_rx,
            }),
        }
    }

    fn next<'a>(
        &'a mut self,
    ) -> Pin<Box<dyn Future<Output = Result<Option<SubgroupReader>, ServeError>> + Send + 'a>> {
        Box::pin(async move {
            loop {
                let mut switch_to_live = None;

                {
                    let cache = self.state.data.read();

                    match &mut self.cursor {
                        ReaderCursor::Live { next_arrival_seq } => {
                            if let Some(entry) = cache.next_live_from(*next_arrival_seq) {
                                *next_arrival_seq = entry.arrival_seq.saturating_add(1);
                                return Ok(Some(entry.reader.clone()));
                            }
                            cache.closed.clone()?;
                            if cache.writer_dropped {
                                return Ok(None);
                            }
                        }
                        ReaderCursor::Replay {
                            next_group_id,
                            max_seen_arrival_seq,
                        } => {
                            if let Some(entry) = cache.next_replay_from(*next_group_id) {
                                *next_group_id = entry.group_id.saturating_add(1);
                                *max_seen_arrival_seq =
                                    (*max_seen_arrival_seq).max(entry.arrival_seq);
                                return Ok(Some(entry.reader.clone()));
                            }
                            let next_arrival_seq = cache
                                .arrival_base_seq
                                .max(max_seen_arrival_seq.saturating_add(1));
                            switch_to_live = Some(next_arrival_seq);
                        }
                    }
                }

                if let Some(seq) = switch_to_live {
                    self.cursor = ReaderCursor::Live {
                        next_arrival_seq: seq,
                    };
                    continue;
                }

                if self.notify_rx.changed().await.is_err() {
                    return Ok(None);
                }
            }
        })
    }

    fn reader_from(&self, start_group_id: u64) -> Box<dyn GroupCacheReader> {
        let max_seen_arrival_seq = self.state.data.read().next_arrival_seq.saturating_sub(1);
        Box::new(DefaultGroupCacheReader {
            state: self.state.clone(),
            cursor: ReaderCursor::Replay {
                next_group_id: start_group_id,
                max_seen_arrival_seq,
            },
            notify_rx: self.state.subscribe(),
        })
    }

    fn available_groups(&self) -> u64 {
        self.state.data.read().available_groups()
    }

    fn has_group(&self, group_id: u64) -> bool {
        self.state.data.read().get(group_id).is_some()
    }

    fn group_created_at(&self, group_id: u64) -> Option<Instant> {
        self.state.data.read().get(group_id).map(|g| g.created_at)
    }

    fn latest_group(&self) -> Option<(u64, u64)> {
        self.state
            .data
            .read()
            .newest_group()
            .map(|g| (g.group_id, g.reader.latest()))
    }

    fn oldest_group(&self) -> Option<u64> {
        self.state.data.read().oldest_group_id()
    }

    fn is_closed(&self) -> bool {
        let cache = self.state.data.read();
        cache.closed.is_err() || cache.writer_dropped
    }
}

// ============================================================================
// Tests
// ============================================================================

#[cfg(test)]
mod tests {
    use super::*;
    use crate::coding::TrackNamespace;
    use bytes::Bytes;

    fn demo_track() -> Arc<Track> {
        Arc::new(Track::new(
            TrackNamespace::from_utf8_path("demo/cache"),
            "video".to_string(),
        ))
    }

    fn limits_with_cache(depth: usize) -> SubgroupLimits {
        SubgroupLimits::with_cache_depth(depth)
    }

    fn make_subgroup(group_id: u64) -> Subgroup {
        Subgroup {
            group_id,
            subgroup_id: 0,
            priority: 0,
        }
    }

    fn produce(
        track: Arc<Track>,
        limits: SubgroupLimits,
    ) -> (Box<dyn GroupCacheWriter>, Box<dyn GroupCacheReader>) {
        DefaultGroupCache.produce(track, limits)
    }

    async fn drain(reader: &mut Box<dyn GroupCacheReader>, expected: &[u64]) {
        for &gid in expected {
            let subgroup = reader.next().await.expect("next ok").expect("group exists");
            assert_eq!(subgroup.group_id, gid, "unexpected group id");
        }
    }

    #[tokio::test]
    async fn live_reader_follows_arrival_order() {
        let (mut writer, mut reader) = produce(demo_track(), limits_with_cache(8));

        for &id in &[2u64, 0, 1] {
            let (mut sw, _) = writer.insert(make_subgroup(id)).expect("insert ok");
            sw.write(Bytes::from(vec![id as u8])).unwrap();
        }

        drain(&mut reader, &[2, 0, 1]).await;
    }

    #[tokio::test]
    async fn replay_reader_follows_group_order() {
        let (mut writer, reader) = produce(demo_track(), limits_with_cache(8));

        for &id in &[2u64, 0, 1] {
            let (mut sw, _) = writer.insert(make_subgroup(id)).expect("insert ok");
            sw.write(Bytes::from(vec![id as u8])).unwrap();
        }

        let mut replay = reader.reader_from(0);
        drain(&mut replay, &[0, 1, 2]).await;
    }

    #[tokio::test]
    async fn replay_reader_hands_off_to_live_for_late_arrivals() {
        let (mut writer, reader) = produce(demo_track(), limits_with_cache(8));

        for &id in &[5u64, 6] {
            let (mut sw, _) = writer.insert(make_subgroup(id)).expect("insert ok");
            sw.write(Bytes::from(vec![id as u8])).unwrap();
        }

        let mut replay = reader.reader_from(5);
        let sg = replay.next().await.expect("ok").expect("exists");
        assert_eq!(sg.group_id, 5);

        let (mut sw, _) = writer.insert(make_subgroup(3)).expect("insert ok");
        sw.write(Bytes::from_static(b"late")).unwrap();

        let sg = replay.next().await.expect("ok").expect("exists");
        assert_eq!(sg.group_id, 6);

        let sg = replay.next().await.expect("ok").expect("exists");
        assert_eq!(sg.group_id, 3);
    }

    #[tokio::test]
    async fn cloned_live_reader_starts_at_latest_cached_group() {
        let (mut writer, reader) = produce(demo_track(), limits_with_cache(8));

        for id in 0u64..2 {
            let (mut sw, _) = writer.insert(make_subgroup(id)).expect("insert ok");
            sw.write(Bytes::from(vec![id as u8])).unwrap();
        }

        let mut live = reader.clone();
        let sg = live.next().await.expect("ok").expect("exists");
        assert_eq!(sg.group_id, 1);

        let (mut sw, _) = writer.insert(make_subgroup(2)).expect("insert ok");
        sw.write(Bytes::from_static(b"live")).unwrap();

        let sg = live.next().await.expect("ok").expect("exists");
        assert_eq!(sg.group_id, 2);
    }

    #[tokio::test]
    async fn eviction_discards_oldest_replay_groups() {
        let (mut writer, reader) = produce(demo_track(), limits_with_cache(2));

        for id in 0u64..4 {
            let (mut sw, _) = writer.insert(make_subgroup(id)).expect("insert ok");
            sw.write(Bytes::from(vec![id as u8])).unwrap();
        }

        assert!(!reader.has_group(0));
        assert!(!reader.has_group(1));
        assert!(reader.has_group(2));
        assert!(reader.has_group(3));

        let mut replay = reader.reader_from(0);
        drain(&mut replay, &[2, 3]).await;
    }

    #[tokio::test]
    async fn reader_finishes_when_writer_dropped() {
        let (mut writer, mut reader) = produce(demo_track(), limits_with_cache(8));

        let (mut sw, _) = writer.insert(make_subgroup(0)).expect("insert ok");
        sw.write(Bytes::from_static(b"sample")).unwrap();

        let group = reader.next().await.expect("ok");
        assert!(group.is_some());

        drop(writer);

        let group = reader.next().await.expect("ok");
        assert!(group.is_none());
        assert!(reader.is_closed());
    }

    #[tokio::test]
    async fn custom_group_cache_produce_called_once_per_track() {
        use std::sync::atomic::{AtomicUsize, Ordering};

        struct CountingCache {
            count: Arc<AtomicUsize>,
            inner: DefaultGroupCache,
        }

        impl GroupCache for CountingCache {
            fn produce(
                &self,
                track: Arc<Track>,
                limits: SubgroupLimits,
            ) -> (Box<dyn GroupCacheWriter>, Box<dyn GroupCacheReader>) {
                self.count.fetch_add(1, Ordering::SeqCst);
                self.inner.produce(track, limits)
            }
        }

        let count = Arc::new(AtomicUsize::new(0));
        let cache = Arc::new(CountingCache {
            count: count.clone(),
            inner: DefaultGroupCache,
        });

        let track_a = demo_track();
        let track_b = Arc::new(Track::new(
            TrackNamespace::from_utf8_path("demo/cache"),
            "audio".to_string(),
        ));

        let _ = cache.produce(track_a, limits_with_cache(8));
        let _ = cache.produce(track_b, limits_with_cache(8));

        assert_eq!(count.load(Ordering::SeqCst), 2);
    }
}
