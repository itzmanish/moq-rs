// SPDX-FileCopyrightText: 2024-2026 Cloudflare Inc., Luke Curley, Mike English and contributors
// SPDX-License-Identifier: MIT OR Apache-2.0

use std::collections::hash_map;
use std::collections::HashMap;

use std::sync::{Arc, Mutex};

use moq_transport::{
    coding::TrackNamespace,
    serve::{ServeError, TracksReader},
};

use crate::metrics::GaugeGuard;

/// Scope key for the outer level of the two-level registry.
///
/// An empty string (`""`) represents the global/unscoped bucket. All unscoped
/// connections share this bucket — any publisher without a scope can be reached
/// by any subscriber without a scope. This is the default behavior for backward
/// compatibility with pre-scope deployments.
///
/// We use `String` rather than `Option<String>` so that `HashMap::get` can
/// accept a `&str` via the `Borrow` trait, avoiding a heap allocation on
/// every lookup in `retrieve()`.
type ScopeKey = String;

/// The scope key used for unscoped (global) registrations.
const UNSCOPED: &str = "";

/// Registry of local tracks, indexed by (scope, namespace).
///
/// Uses a two-level map so that `retrieve()` only scans namespaces within
/// the matching scope, rather than iterating all namespaces across all scopes.
#[derive(Clone)]
pub struct Locals {
    lookup: Arc<Mutex<HashMap<ScopeKey, HashMap<TrackNamespace, TracksReader>>>>,
}

impl Default for Locals {
    fn default() -> Self {
        Self::new()
    }
}

/// Local tracks registry.
impl Locals {
    pub fn new() -> Self {
        Self {
            lookup: Default::default(),
        }
    }

    /// Register new local tracks.
    ///
    /// `scope` is the resolved scope identity from `Coordinator::resolve_scope()`,
    /// or `None` for unscoped sessions. Registrations are keyed by `(scope, namespace)`,
    /// so the same namespace in different scopes routes independently.
    pub async fn register(
        &mut self,
        scope: Option<&str>,
        tracks: TracksReader,
    ) -> anyhow::Result<Registration> {
        let namespace = tracks.namespace.clone();
        let scope_key = scope.unwrap_or(UNSCOPED).to_string();

        // Insert the tracks into the scope bucket
        let mut lookup = self.lookup.lock().unwrap();
        let bucket = lookup.entry(scope_key.clone()).or_default();
        match bucket.entry(namespace.clone()) {
            hash_map::Entry::Vacant(entry) => entry.insert(tracks),
            hash_map::Entry::Occupied(_) => return Err(ServeError::Duplicate.into()),
        };

        let registration = Registration {
            locals: self.clone(),
            scope_key,
            namespace,
            _gauge_guard: GaugeGuard::new("moq_relay_announced_namespaces"),
        };

        Ok(registration)
    }

    /// Retrieve local tracks by namespace using hierarchical prefix matching.
    /// Returns the TracksReader for the longest matching namespace prefix.
    ///
    /// `scope` is the resolved scope identity from `Coordinator::resolve_scope()`,
    /// or `None` for unscoped sessions. When `scope` is `None`, only tracks
    /// registered without a scope (the global/unscoped bucket) are searched.
    pub fn retrieve(
        &self,
        scope: Option<&str>,
        namespace: &TrackNamespace,
    ) -> Option<TracksReader> {
        let lookup = self.lookup.lock().unwrap();

        // Look up the scope bucket directly — O(1), zero allocation.
        // HashMap<String, V>::get accepts &str via Borrow<str>.
        let bucket = lookup.get(scope.unwrap_or(UNSCOPED))?;

        // Find the longest matching prefix within this scope
        let mut best_match: Option<TracksReader> = None;
        let mut best_len = 0;

        for (registered_ns, tracks) in bucket.iter() {
            // Check if registered_ns is a prefix of namespace
            if namespace.fields.len() >= registered_ns.fields.len() {
                let is_prefix = registered_ns
                    .fields
                    .iter()
                    .zip(namespace.fields.iter())
                    .all(|(a, b)| a == b);

                if is_prefix && registered_ns.fields.len() > best_len {
                    best_match = Some(tracks.clone());
                    best_len = registered_ns.fields.len();
                }
            }
        }

        best_match
    }
}

pub struct Registration {
    locals: Locals,
    scope_key: ScopeKey,
    namespace: TrackNamespace,
    /// Gauge guard for tracking announced namespaces - decrements on drop
    _gauge_guard: GaugeGuard,
}

/// Deregister local tracks on drop.
impl Drop for Registration {
    fn drop(&mut self) {
        let ns = self.namespace.to_utf8_path();
        let scope = if self.scope_key.is_empty() {
            "<unscoped>"
        } else {
            &self.scope_key
        };
        tracing::debug!(namespace = %ns, scope = %scope, "deregistering namespace from locals");

        let mut lookup = self.locals.lookup.lock().unwrap();
        if let Some(bucket) = lookup.get_mut(self.scope_key.as_str()) {
            bucket.remove(&self.namespace);
            // Clean up empty scope buckets to avoid memory leaks
            if bucket.is_empty() {
                lookup.remove(self.scope_key.as_str());
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::NamespaceRegistration;
    use moq_transport::serve::{Subgroup, TrackReaderMode, Tracks};
    use std::sync::atomic::{AtomicBool, Ordering};

    fn namespace(path: &str) -> TrackNamespace {
        TrackNamespace::from_utf8_path(path)
    }

    struct DropFlag(Arc<AtomicBool>);

    impl Drop for DropFlag {
        fn drop(&mut self) {
            self.0.store(true, Ordering::SeqCst);
        }
    }

    #[tokio::test]
    async fn publisher_teardown_unregisters_namespace_and_drops_tracks_reader_when_idle() {
        let mut locals = Locals::new();
        let ns = namespace("teardown/idle");

        let (_, request, reader) = Tracks::new(ns.clone()).produce();
        let reader_info = Arc::downgrade(&reader.info);

        let registration = locals.register(None, reader.clone()).await.unwrap();
        let coordinator_dropped = Arc::new(AtomicBool::new(false));
        let coordinator_registration =
            NamespaceRegistration::new(DropFlag(coordinator_dropped.clone()));

        assert!(locals.retrieve(None, &ns).is_some());

        drop(registration);
        drop(coordinator_registration);
        drop(request);
        drop(reader);

        assert!(locals.retrieve(None, &ns).is_none());
        assert!(coordinator_dropped.load(Ordering::SeqCst));
        assert!(reader_info.upgrade().is_none());
    }

    #[tokio::test]
    async fn publisher_teardown_unregisters_namespace_but_existing_subscriber_drains_cache() {
        let mut locals = Locals::new();
        let ns = namespace("teardown/active");
        let track_name = "0.mp4";

        let (_, mut request, mut reader) = Tracks::new(ns.clone()).produce();
        let reader_info = Arc::downgrade(&reader.info);

        let registration = locals.register(None, reader.clone()).await.unwrap();
        let coordinator_dropped = Arc::new(AtomicBool::new(false));
        let coordinator_registration =
            NamespaceRegistration::new(DropFlag(coordinator_dropped.clone()));

        let track_reader = reader.subscribe(ns.clone(), track_name).unwrap();
        let track_writer = request.next().await.unwrap();
        let mut subgroups_writer = track_writer.subgroups().unwrap();
        let mut subgroup_writer = subgroups_writer
            .create(Subgroup {
                group_id: 0,
                subgroup_id: 0,
                priority: 0,
            })
            .unwrap();

        subgroup_writer.write(b"init-a".to_vec().into()).unwrap();
        subgroup_writer.write(b"init-b".to_vec().into()).unwrap();

        let mut subgroups_reader = match track_reader.mode().await.unwrap() {
            TrackReaderMode::Subgroups(reader) => reader,
            _ => panic!("unexpected track mode"),
        };

        drop(subgroup_writer);
        drop(subgroups_writer);
        drop(registration);
        drop(coordinator_registration);
        drop(request);
        drop(reader);

        assert!(locals.retrieve(None, &ns).is_none());
        assert!(coordinator_dropped.load(Ordering::SeqCst));
        assert!(reader_info.upgrade().is_none());

        let mut subgroup = subgroups_reader.next().await.unwrap().unwrap();
        assert_eq!(subgroup.group_id, 0);

        let object = subgroup.read_next().await.unwrap().unwrap();
        assert_eq!(object.as_ref(), b"init-a");

        let object = subgroup.read_next().await.unwrap().unwrap();
        assert_eq!(object.as_ref(), b"init-b");

        assert!(subgroup.read_next().await.unwrap().is_none());
        assert!(subgroups_reader.next().await.unwrap().is_none());
    }

    #[tokio::test]
    async fn dropping_subgroup_handles_keeps_cache_alive_until_track_reader_drops() {
        let mut locals = Locals::new();
        let ns = namespace("teardown/drop-order");
        let track_name = "0.mp4";

        let (_, mut request, mut reader) = Tracks::new(ns.clone()).produce();

        let registration = locals.register(None, reader.clone()).await.unwrap();
        let coordinator_registration = NamespaceRegistration::new(());

        let track_reader = reader.subscribe(ns.clone(), track_name).unwrap();
        let track_info = Arc::downgrade(&track_reader.info);

        let track_writer = request.next().await.unwrap();
        let mut subgroups_writer = track_writer.subgroups().unwrap();
        let mut subgroup_writer = subgroups_writer
            .create(Subgroup {
                group_id: 0,
                subgroup_id: 0,
                priority: 0,
            })
            .unwrap();

        subgroup_writer.write(b"payload".to_vec().into()).unwrap();

        let mut subgroups_reader = match track_reader.mode().await.unwrap() {
            TrackReaderMode::Subgroups(reader) => reader,
            _ => panic!("unexpected track mode"),
        };

        let mut subgroup = subgroups_reader.next().await.unwrap().unwrap();
        let subgroup_info = Arc::downgrade(&subgroup.info);

        drop(subgroup_writer);
        drop(subgroups_writer);
        drop(registration);
        drop(coordinator_registration);
        drop(request);
        drop(reader);

        let object = subgroup.read_next().await.unwrap().unwrap();
        assert_eq!(object.as_ref(), b"payload");
        assert!(subgroup.read_next().await.unwrap().is_none());

        drop(subgroup);
        drop(subgroups_reader);

        // The namespace-level ownership is gone, but the subscriber-held TrackReader
        // should still keep the cached subgroup alive.
        assert!(locals.retrieve(None, &ns).is_none());
        assert!(subgroup_info.upgrade().is_some());
        assert!(track_info.upgrade().is_some());

        // A fresh subgroup reader cloned from the still-active TrackReader can still
        // reach the cached subgroup once more.
        let mut reopened = match track_reader.mode().await.unwrap() {
            TrackReaderMode::Subgroups(reader) => reader,
            _ => panic!("unexpected track mode"),
        };
        let subgroup = reopened.next().await.unwrap().unwrap();
        assert_eq!(subgroup.group_id, 0);
        drop(subgroup);
        drop(reopened);

        drop(track_reader);

        assert!(subgroup_info.upgrade().is_none());
        assert!(track_info.upgrade().is_none());
    }
}
