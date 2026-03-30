use std::collections::VecDeque;
use std::sync::{Arc, RwLock};
use std::time::Instant;

use tokio::sync::watch;

use crate::serve::{ServeError, Subgroup, SubgroupInfo, SubgroupReader, SubgroupWriter, Track};

/// Shared state for a per-track cache.
struct CacheState {
    data: RwLock<CacheInner>,
    notify_tx: watch::Sender<u64>,
}

impl CacheState {
    fn new(max_depth: usize) -> (Arc<Self>, watch::Receiver<u64>) {
        let (tx, rx) = watch::channel(0);
        let inner = CacheInner::new(max_depth);
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

struct CacheInner {
    slots: VecDeque<Option<CachedGroup>>,
    base_group_id: u64,
    count: usize,
    max_depth: usize,
    closed: Result<(), ServeError>,
    epoch: u64,
}

impl CacheInner {
    fn new(max_depth: usize) -> Self {
        Self {
            slots: VecDeque::new(),
            base_group_id: 0,
            count: 0,
            max_depth,
            closed: Ok(()),
            epoch: 0,
        }
    }

    fn max_spread(&self) -> usize {
        if self.max_depth == 0 {
            usize::MAX
        } else {
            self.max_depth.saturating_mul(2)
        }
    }

    fn insert(&mut self, group: CachedGroup) -> bool {
        if self.slots.is_empty() {
            self.base_group_id = group.group_id;
            self.slots.push_back(Some(group));
            self.count = 1;
            self.evict_if_needed();
            return true;
        }

        if group.group_id < self.base_group_id {
            let gap = (self.base_group_id - group.group_id) as usize;
            if gap + self.slots.len() > self.max_spread() {
                return false;
            }
            for _ in 0..gap {
                self.slots.push_front(None);
            }
            self.base_group_id = group.group_id;
            if let Some(slot) = self.slots.front_mut() {
                if slot.is_some() {
                    return false;
                }
                *slot = Some(group);
                self.count += 1;
                self.evict_if_needed();
                return true;
            }
            return false;
        }

        let offset = (group.group_id - self.base_group_id) as usize;
        if offset + 1 > self.max_spread() {
            return false;
        }

        if offset >= self.slots.len() {
            self.slots.resize_with(offset + 1, || None);
        }

        if self.slots[offset].is_some() {
            return false;
        }

        self.slots[offset] = Some(group);
        self.count += 1;
        self.evict_if_needed();
        true
    }

    fn evict_if_needed(&mut self) {
        if self.max_depth == 0 {
            return;
        }

        while self.count > self.max_depth {
            if let Some(front) = self.slots.pop_front() {
                if front.is_some() {
                    self.count -= 1;
                }
                self.base_group_id = self.base_group_id.saturating_add(1);
            } else {
                break;
            }
        }

        while let Some(None) = self.slots.front() {
            self.slots.pop_front();
            self.base_group_id = self.base_group_id.saturating_add(1);
        }
    }

    fn next_from(&self, cursor: u64) -> Option<&CachedGroup> {
        if self.slots.is_empty() {
            return None;
        }

        let start = if cursor <= self.base_group_id {
            0
        } else {
            (cursor - self.base_group_id) as usize
        };

        for slot in self.slots.iter().skip(start) {
            if let Some(group) = slot.as_ref() {
                if group.group_id >= cursor {
                    return Some(group);
                }
            }
        }
        None
    }

    fn get(&self, group_id: u64) -> Option<&CachedGroup> {
        if self.slots.is_empty() || group_id < self.base_group_id {
            return None;
        }
        let offset = (group_id - self.base_group_id) as usize;
        self.slots.get(offset).and_then(|slot| slot.as_ref())
    }

    fn available_groups(&self) -> u64 {
        self.count.saturating_sub(1) as u64
    }

    fn oldest_group_id(&self) -> Option<u64> {
        self.slots
            .iter()
            .find_map(|slot| slot.as_ref().map(|group| group.group_id))
    }

    fn newest_group(&self) -> Option<&CachedGroup> {
        self.slots.iter().rev().find_map(|slot| slot.as_ref())
    }
}

#[derive(Clone)]
struct CachedGroup {
    group_id: u64,
    reader: SubgroupReader,
    created_at: Instant,
}

pub struct GroupCache;

impl GroupCache {
    pub fn produce(track: Arc<Track>, max_depth: usize) -> (GroupCacheWriter, GroupCacheReader) {
        let (state, rx) = CacheState::new(max_depth);
        let writer = GroupCacheWriter {
            track,
            state: state.clone(),
        };
        let reader = GroupCacheReader {
            state,
            cursor: 0,
            notify_rx: rx,
        };
        (writer, reader)
    }
}

#[derive(Clone)]
pub struct GroupCacheReader {
    state: Arc<CacheState>,
    cursor: u64,
    notify_rx: watch::Receiver<u64>,
}

impl GroupCacheReader {
    pub async fn next(&mut self) -> Result<Option<SubgroupReader>, ServeError> {
        loop {
            {
                let cache = self.state.data.read().unwrap();
                if let Some(group) = cache.next_from(self.cursor) {
                    self.cursor = group.group_id.saturating_add(1);
                    return Ok(Some(group.reader.clone()));
                }

                cache.closed.clone()?;
            }

            if self.notify_rx.changed().await.is_err() {
                return Ok(None);
            }
        }
    }

    pub fn reader_from(&self, start_group_id: u64) -> GroupCacheReader {
        GroupCacheReader {
            state: self.state.clone(),
            cursor: start_group_id,
            notify_rx: self.state.subscribe(),
        }
    }

    pub fn available_groups(&self) -> u64 {
        let cache = self.state.data.read().unwrap();
        cache.available_groups()
    }

    pub fn has_group(&self, group_id: u64) -> bool {
        let cache = self.state.data.read().unwrap();
        cache.get(group_id).is_some()
    }

    pub fn group_created_at(&self, group_id: u64) -> Option<Instant> {
        let cache = self.state.data.read().unwrap();
        cache.get(group_id).map(|group| group.created_at)
    }

    pub fn latest_group(&self) -> Option<(u64, u64)> {
        let cache = self.state.data.read().unwrap();
        cache
            .newest_group()
            .map(|group| (group.group_id, group.reader.latest()))
    }

    pub fn oldest_group(&self) -> Option<u64> {
        let cache = self.state.data.read().unwrap();
        cache.oldest_group_id()
    }
}

pub struct GroupCacheWriter {
    track: Arc<Track>,
    state: Arc<CacheState>,
}

impl GroupCacheWriter {
    pub fn insert(&mut self, subgroup: Subgroup) -> Result<(SubgroupWriter, bool), ServeError> {
        let info = SubgroupInfo {
            track: self.track.clone(),
            group_id: subgroup.group_id,
            subgroup_id: subgroup.subgroup_id,
            priority: subgroup.priority,
        };

        let (writer, reader) = info.produce();

        let mut cache = self.state.data.write().unwrap();
        cache.closed.clone()?;

        let inserted = cache.insert(CachedGroup {
            group_id: subgroup.group_id,
            reader,
            created_at: Instant::now(),
        });

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
        let mut cache = self.state.data.write().unwrap();
        cache.closed.clone()?;
        cache.closed = Err(err.clone());
        cache.epoch = cache.epoch.saturating_add(1);
        let epoch = cache.epoch;
        drop(cache);
        let _ = self.state.notify_tx.send(epoch);
        Ok(())
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
    async fn ordered_reads_follow_insert_sequence() {
        let (mut writer, mut reader) = GroupCache::produce(demo_track(), 8);

        for id in 0..4 {
            let subgroup = make_subgroup(id);
            let (mut subgroup_writer, _) = writer.insert(subgroup).expect("insert ok");
            subgroup_writer
                .write(Bytes::from_static(b"sample"))
                .unwrap();
        }

        drain(&mut reader, &[0, 1, 2, 3]).await;
    }

    #[tokio::test]
    async fn out_of_order_inserts_are_read_in_order() {
        let (mut writer, mut reader) = GroupCache::produce(demo_track(), 8);

        for &id in &[2, 0, 1] {
            let subgroup = make_subgroup(id);
            let (mut subgroup_writer, _) = writer.insert(subgroup).expect("insert ok");
            subgroup_writer.write(Bytes::from(vec![id as u8])).unwrap();
        }

        drain(&mut reader, &[0, 1, 2]).await;
    }

    #[tokio::test]
    async fn eviction_discards_oldest_groups() {
        let (mut writer, mut reader) = GroupCache::produce(demo_track(), 2);

        for id in 0..4 {
            let subgroup = make_subgroup(id);
            let (mut subgroup_writer, _) = writer.insert(subgroup).expect("insert ok");
            subgroup_writer.write(Bytes::from(vec![id as u8])).unwrap();
        }

        // Cache depth 2: only groups 2 and 3 should remain.
        assert!(!reader.has_group(0));
        assert!(!reader.has_group(1));
        assert!(reader.has_group(2));
        assert!(reader.has_group(3));

        drain(&mut reader, &[2, 3]).await;
    }
}
