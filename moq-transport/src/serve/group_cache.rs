use std::collections::{BTreeMap, VecDeque};
use std::sync::Arc;
use std::time::Instant;

use parking_lot::RwLock;
use tokio::sync::watch;

use crate::serve::{
    ServeError, Subgroup, SubgroupInfo, SubgroupLimits, SubgroupReader, SubgroupWriter, Track,
};

/// Shared state for a per-track cache.
struct CacheState {
    data: RwLock<CacheInner>,
    notify_tx: watch::Sender<u64>,
}

impl CacheState {
    fn new(max_cached_groups: usize) -> (Arc<Self>, watch::Receiver<u64>) {
        let (tx, rx) = watch::channel(0);
        let inner = CacheInner::new(max_cached_groups);
        let state = Arc::new(Self {
            data: RwLock::new(inner),
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
        // NOTE(itzmanish): We don't allow duplicate groups to be inserted.
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
            .map(|(_, entry)| entry.clone())
    }

    fn available_groups(&self) -> u64 {
        self.by_group.len().saturating_sub(1) as u64
    }

    fn oldest_group_id(&self) -> Option<u64> {
        self.by_group.keys().next().copied()
    }

    fn newest_group(&self) -> Option<Entry> {
        self.by_group
            .iter()
            .next_back()
            .map(|(_, entry)| entry.clone())
    }

    fn latest_arrival_seq(&self) -> Option<u64> {
        self.arrivals.back().map(|entry| entry.arrival_seq)
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

pub struct GroupCache;

impl GroupCache {
    pub fn produce(
        track: Arc<Track>,
        limits: SubgroupLimits,
    ) -> (GroupCacheWriter, GroupCacheReader) {
        let (state, rx) = CacheState::new(limits.max_cached_groups);
        let writer = GroupCacheWriter {
            track,
            state: state.clone(),
            limits,
        };
        let reader = GroupCacheReader {
            state,
            cursor: ReaderCursor::Live {
                next_arrival_seq: 0,
            },
            notify_rx: rx,
        };
        (writer, reader)
    }
}

enum ReaderCursor {
    Live {
        next_arrival_seq: u64,
    },
    Replay {
        next_group_id: u64,
        max_seen_arrival_seq: u64,
    },
}

pub struct GroupCacheReader {
    state: Arc<CacheState>,
    cursor: ReaderCursor,
    notify_rx: watch::Receiver<u64>,
}

impl Clone for GroupCacheReader {
    fn clone(&self) -> Self {
        let notify_rx = self.state.subscribe();

        match self.cursor {
            ReaderCursor::Live { .. } => {
                let cache = self.state.data.read();
                let next_arrival_seq = cache.latest_arrival_seq().unwrap_or(cache.next_arrival_seq);
                Self {
                    state: self.state.clone(),
                    cursor: ReaderCursor::Live { next_arrival_seq },
                    notify_rx,
                }
            }
            ReaderCursor::Replay {
                next_group_id,
                max_seen_arrival_seq,
            } => Self {
                state: self.state.clone(),
                cursor: ReaderCursor::Replay {
                    next_group_id,
                    max_seen_arrival_seq,
                },
                notify_rx,
            },
        }
    }
}

impl GroupCacheReader {
    pub async fn next(&mut self) -> Result<Option<SubgroupReader>, ServeError> {
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
                            *max_seen_arrival_seq = (*max_seen_arrival_seq).max(entry.arrival_seq);
                            return Ok(Some(entry.reader.clone()));
                        }

                        let next_arrival_seq = cache
                            .arrival_base_seq
                            .max(max_seen_arrival_seq.saturating_add(1));
                        switch_to_live = Some(next_arrival_seq);
                    }
                }
            }

            if let Some(next_arrival_seq) = switch_to_live {
                self.cursor = ReaderCursor::Live { next_arrival_seq };
                continue;
            }

            if self.notify_rx.changed().await.is_err() {
                return Ok(None);
            }
        }
    }

    pub fn reader_from(&self, start_group_id: u64) -> GroupCacheReader {
        let max_seen_arrival_seq = self.state.data.read().next_arrival_seq.saturating_sub(1);

        GroupCacheReader {
            state: self.state.clone(),
            cursor: ReaderCursor::Replay {
                next_group_id: start_group_id,
                max_seen_arrival_seq,
            },
            notify_rx: self.state.subscribe(),
        }
    }

    pub fn available_groups(&self) -> u64 {
        self.state.data.read().available_groups()
    }

    pub fn has_group(&self, group_id: u64) -> bool {
        self.state.data.read().get(group_id).is_some()
    }

    pub fn group_created_at(&self, group_id: u64) -> Option<Instant> {
        self.state
            .data
            .read()
            .get(group_id)
            .map(|group| group.created_at)
    }

    pub fn latest_group(&self) -> Option<(u64, u64)> {
        self.state
            .data
            .read()
            .newest_group()
            .map(|group| (group.group_id, group.reader.latest()))
    }

    pub fn oldest_group(&self) -> Option<u64> {
        self.state.data.read().oldest_group_id()
    }

    pub fn is_closed(&self) -> bool {
        let cache = self.state.data.read();
        cache.closed.is_err() || cache.writer_dropped
    }
}

pub struct GroupCacheWriter {
    track: Arc<Track>,
    state: Arc<CacheState>,
    limits: SubgroupLimits,
}

impl GroupCacheWriter {
    pub fn insert(&mut self, subgroup: Subgroup) -> Result<(SubgroupWriter, bool), ServeError> {
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

    pub fn close(self, err: ServeError) -> Result<(), ServeError> {
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

impl Drop for GroupCacheWriter {
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

    async fn drain(reader: &mut GroupCacheReader, expected: &[u64]) {
        for &gid in expected {
            let subgroup = reader.next().await.expect("next ok").expect("group exists");
            assert_eq!(subgroup.group_id, gid, "unexpected group id");
        }
    }

    #[tokio::test]
    async fn live_reader_follows_arrival_order() {
        let (mut writer, mut reader) = GroupCache::produce(demo_track(), limits_with_cache(8));

        for &id in &[2, 0, 1] {
            let subgroup = make_subgroup(id);
            let (mut subgroup_writer, _) = writer.insert(subgroup).expect("insert ok");
            subgroup_writer.write(Bytes::from(vec![id as u8])).unwrap();
        }

        drain(&mut reader, &[2, 0, 1]).await;
    }

    #[tokio::test]
    async fn replay_reader_follows_group_order() {
        let (mut writer, reader) = GroupCache::produce(demo_track(), limits_with_cache(8));

        for &id in &[2, 0, 1] {
            let subgroup = make_subgroup(id);
            let (mut subgroup_writer, _) = writer.insert(subgroup).expect("insert ok");
            subgroup_writer.write(Bytes::from(vec![id as u8])).unwrap();
        }

        let mut replay = reader.reader_from(0);
        drain(&mut replay, &[0, 1, 2]).await;
    }

    #[tokio::test]
    async fn replay_reader_hands_off_to_live_for_late_arrivals() {
        let (mut writer, reader) = GroupCache::produce(demo_track(), limits_with_cache(8));

        for &id in &[5, 6] {
            let subgroup = make_subgroup(id);
            let (mut subgroup_writer, _) = writer.insert(subgroup).expect("insert ok");
            subgroup_writer.write(Bytes::from(vec![id as u8])).unwrap();
        }

        let mut replay = reader.reader_from(5);

        let subgroup = replay.next().await.expect("next ok").expect("group exists");
        assert_eq!(subgroup.group_id, 5);

        let subgroup = make_subgroup(3);
        let (mut subgroup_writer, _) = writer.insert(subgroup).expect("insert ok");
        subgroup_writer.write(Bytes::from_static(b"late")).unwrap();

        let subgroup = replay.next().await.expect("next ok").expect("group exists");
        assert_eq!(subgroup.group_id, 6);

        let subgroup = replay.next().await.expect("next ok").expect("group exists");
        assert_eq!(subgroup.group_id, 3);
    }

    #[tokio::test]
    async fn cloned_live_reader_starts_at_latest_cached_group() {
        let (mut writer, reader) = GroupCache::produce(demo_track(), limits_with_cache(8));

        for id in 0..2 {
            let subgroup = make_subgroup(id);
            let (mut subgroup_writer, _) = writer.insert(subgroup).expect("insert ok");
            subgroup_writer.write(Bytes::from(vec![id as u8])).unwrap();
        }

        let mut live = reader.clone();

        // A fresh live clone should receive the latest cached arrival once.
        let subgroup = live.next().await.expect("next ok").expect("group exists");
        assert_eq!(subgroup.group_id, 1);

        let subgroup = make_subgroup(2);
        let (mut subgroup_writer, _) = writer.insert(subgroup).expect("insert ok");
        subgroup_writer.write(Bytes::from_static(b"live")).unwrap();

        let subgroup = live.next().await.expect("next ok").expect("group exists");
        assert_eq!(subgroup.group_id, 2);
    }

    #[tokio::test]
    async fn eviction_discards_oldest_replay_groups() {
        let (mut writer, reader) = GroupCache::produce(demo_track(), limits_with_cache(2));

        for id in 0..4 {
            let subgroup = make_subgroup(id);
            let (mut subgroup_writer, _) = writer.insert(subgroup).expect("insert ok");
            subgroup_writer.write(Bytes::from(vec![id as u8])).unwrap();
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
        let (mut writer, mut reader) = GroupCache::produce(demo_track(), limits_with_cache(8));

        let subgroup = make_subgroup(0);
        let (mut subgroup_writer, _) = writer.insert(subgroup).expect("insert ok");
        subgroup_writer
            .write(Bytes::from_static(b"sample"))
            .unwrap();

        let group = reader.next().await.expect("next ok");
        assert!(group.is_some());

        drop(writer);

        let group = reader.next().await.expect("next ok");
        assert!(group.is_none());
        assert!(reader.is_closed());
    }
}
