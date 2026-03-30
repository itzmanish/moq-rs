# Lingering Subscriber Feature

## Product Requirements Document (PRD) & Architecture Design

**Version:** 1.0  
**Date:** December 2024  
**Status:** Draft

---

## Table of Contents

1. [Executive Summary](#executive-summary)
2. [Problem Statement](#problem-statement)
3. [Goals & Non-Goals](#goals--non-goals)
4. [User Stories](#user-stories)
5. [Functional Requirements](#functional-requirements)
6. [Architecture Design](#architecture-design)
7. [Detailed Design](#detailed-design)
8. [Edge Cases & Failure Modes](#edge-cases--failure-modes)
9. [Security Considerations](#security-considerations)
10. [Performance Considerations](#performance-considerations)
11. [Configuration & Tunables](#configuration--tunables)
12. [Migration & Compatibility](#migration--compatibility)
13. [Testing Strategy](#testing-strategy)
14. [Open Questions](#open-questions)

---

## Executive Summary

The **Lingering Subscriber** feature allows subscribers to express interest in tracks that may not yet exist, with the relay holding these subscriptions in a pending state until a matching publisher announces and publishes the requested track. This enables "subscriber-first" workflows where consumers can connect before producers, improving resilience in distributed live streaming scenarios.

---

## Terms and Definition

**Publisher**: A publisher is an entity which pushes track in a namespace. Publisher is always associated with the track and not the namespace directly.

**Subscriber**: A subscriber is an entity which pulls track from a namespace.

---

## Problem Statement

### Current Behavior

When a subscriber requests a track via `SUBSCRIBE`:

1. The relay checks local tracks (`Locals::retrieve`)
2. If not found locally, checks remote relays via coordinator (`RemoteManager::subscribe`)
3. **If neither exists, immediately returns `SUBSCRIBE_ERROR` with `NotFound`**

This creates a **race condition** where:

-   Subscribers must connect **after** publishers
-   Network partitions or publisher restarts cause subscriber disconnections
-   Load balancers cannot route subscribers to relays before publishers announce

---

## Goals & Non-Goals

### Goals

1. **Subscriber Resilience**: Subscribers can connect before publishers without errors
2. **Bounded Resources**: Prevent unbounded memory growth from abandoned subscriptions
3. **Configurable Behavior**: Allow opt-in/opt-out per subscription and relay-wide policies
4. **Observable**: Expose metrics and logs for debugging lingering subscriptions
5. **Backward Compatible**: Existing clients work without modification

### Non-Goals

1. **Persistent Subscriptions**: Subscriptions don't survive relay restarts
2. **Guaranteed Delivery**: No buffering of missed data while waiting
3. **Subscription Prioritization**: All lingering subscriptions treated equally (v1)
4. **Transport reconnection**: If publisher is closed the subscriber also gets closed, it's upto client to reconnect and re-subscribe.

---

## User Stories

### US-1: Early Viewer

> As a viewer, I want to open a stream URL before the streamer goes live, so that I automatically see the stream when it starts without refreshing.

### US-2: Relay Operator

> As a relay operator, I want to limit how long subscriptions can linger and how many can be pending, so that I can prevent resource exhaustion.

### US-3: Publisher

> As a publisher, I want subscribers to be waiting when I announce my track, so that they receive my content from the first frame.

### US-4: Application Developer

> As an application developer, I want to explicitly opt-in or opt-out of lingering behavior per subscription, so that I can control the user experience.

---

## Functional Requirements

### FR-1: Lingering Subscription State

| Requirement | Description                                                                                                                               |
| ----------- | ----------------------------------------------------------------------------------------------------------------------------------------- |
| FR-1.1      | When a SUBSCRIBE arrives for a non-existent track, the relay MAY hold it in a "lingering" state instead of immediately returning an error |
| FR-1.2      | Lingering subscriptions MUST be associated with a specific namespace and track name                                                       |
| FR-1.3      | Lingering subscriptions MUST have a configurable timeout after which they are closed with an error                                        |
| FR-1.4      | Lingering subscriptions MUST be closed if the subscriber disconnects                                                                      |

### FR-2: Publisher Arrival Handling

| Requirement | Description                                                                                                       |
| ----------- | ----------------------------------------------------------------------------------------------------------------- |
| FR-2.1      | When a publisher announces a namespace, all lingering subscriptions for tracks in that namespace MUST be notified |
| FR-2.2      | When a specific track becomes available, matching lingering subscriptions MUST be activated and receive data      |
| FR-2.3      | The order of activation SHOULD follow FIFO (first subscriber to linger gets served first)                         |
| FR-2.4      | If multiple tracks match (e.g., wildcard subscriptions), the most specific match wins                             |

### FR-3: Resource Management

| Requirement | Description                                                                                         |
| ----------- | --------------------------------------------------------------------------------------------------- |
| FR-3.1      | The relay MUST enforce a maximum number of lingering subscriptions per namespace                    |
| FR-3.2      | The relay MUST enforce a maximum total number of lingering subscriptions                            |
| FR-3.3      | When limits are exceeded, the oldest lingering subscription SHOULD be evicted (configurable policy) |
| FR-3.4      | The relay SHOULD expose metrics for lingering subscription counts and wait times                    |

### FR-4: Subscriber Control

| Requirement | Description                                                                             |
| ----------- | --------------------------------------------------------------------------------------- |
| FR-4.1      | Subscribers MAY indicate willingness to linger via a subscription parameter             |
| FR-4.2      | Subscribers MAY specify a custom linger timeout in the subscription request             |
| FR-4.3      | Subscribers MAY cancel a lingering subscription at any time via UNSUBSCRIBE             |
| FR-4.4      | The relay MAY reject lingering requests based on policy (returning immediate NOT_FOUND) |

### FR-5: Publisher Departure Handling

| Requirement | Description                                                                                                          |
| ----------- | -------------------------------------------------------------------------------------------------------------------- |
| FR-5.1      | When a publisher closes a track, active subscriptions DOES NOT transition to lingering state                         |
| FR-5.2      | When a publisher's namespace is unregistered, lingering subscriptions for that namespace gets cleared out            |
| FR-5.3      | A "grace period" configuration is out of scope for publisher reconnection before subscribers are notified of closure |

---

## Architecture Design

### High-Level Architecture

```
┌─────────────────────────────────────────────────────────────────────┐
│                            RELAY                                     │
│  ┌─────────────┐    ┌──────────────────┐    ┌─────────────────────┐ │
│  │  Producer   │    │  LingeringStore  │    │     Consumer        │ │
│  │             │◄───┤                  │◄───┤                     │ │
│  │ (serves     │    │  - pending_subs  │    │ (receives announces)│ │
│  │  subscribers)│   │  - timeouts      │    │                     │ │
│  └──────┬──────┘    │  - limits        │    └──────────┬──────────┘ │
│         │           └────────┬─────────┘               │            │
│         │                    │                         │            │
│         ▼                    ▼                         ▼            │
│  ┌─────────────┐    ┌──────────────────┐    ┌─────────────────────┐ │
│  │   Locals    │◄───┤  NamespaceWatch  │◄───┤    Coordinator      │ │
│  │             │    │                  │    │                     │ │
│  └─────────────┘    └──────────────────┘    └─────────────────────┘ │
└─────────────────────────────────────────────────────────────────────┘

                    ┌─────────────┐
        SUBSCRIBE   │             │   PUBLISH_NAMESPACE
      ────────────► │   Session   │ ◄────────────────────
                    │             │
                    └─────────────┘
```

### Component Responsibilities

| Component          | Responsibility                                                                                             |
| ------------------ | ---------------------------------------------------------------------------------------------------------- |
| **LingeringStore** | Stores pending subscriptions, manages timeouts and limits, notifies on matches                             |
| **NamespaceWatch** | Watches for namespace/track changes, triggers LingeringStore notifications                                 |
| **Producer**       | Routes subscriptions to LingeringStore when track not found                                                |
| **Consumer**       | Notifies NamespaceWatch when publishers announce/depart                                                    |
| **Coordinator**    | Extended to support namespace change subscriptions and to notify on remote relay publisher announce/depart |

### State Machine

```
                         ┌─────────────┐
                         │   INITIAL    │
                         └─────┬──────┘
                                │ SUBSCRIBE received
                                ▼
                    ┌───────────────────────┐
                    │    TRACK_LOOKUP         │
                    └──────────┬───────────┘
                       found /  │  \ not found
                            /   │   \
                           ▼    │    ▼
              ┌─────────────┐  │   ┌─────────────────┐
              │   ACTIVE     │  │   │ LINGER_ELIGIBLE   │
              └─────────────┘  │   └────────┬────────┘
                                │     can    │  cannot
                                │    linger  │  linger
                                │      /     │     \
                                │     ▼      │      ▼
                                │ ┌──────────┴─┐ ┌─────────┐
                                │ │  LINGERING  │ │ ERRORED │
                                │ └─────┬──────┘ └─────────┘
                                │        │
                    ┌──────────┴───────┴─────────┐
                    │                              │
            track   │  timeout/    publisher       │ unsubscribe
           appears  │  evicted     departs         │
                ▼   ▼              ▼               ▼
           ┌────────┐         ┌────────┐      ┌────────┐
           │ ACTIVE  │         │ERRORED  │      │ CLOSED │
           └────��──┘         └────────┘      └────────┘
```

---

## Detailed Design

### 1. LingeringStore Structure

```rust
/// Configuration for lingering behavior
#[derive(Clone, Debug)]
pub struct LingeringConfig {
    /// Whether lingering is enabled at all
    pub enabled: bool,

    /// Default timeout for lingering subscriptions
    pub default_timeout: Duration,

    /// Maximum timeout a subscriber can request
    pub max_timeout: Duration,

    /// Maximum lingering subscriptions per namespace
    pub max_per_namespace: usize,

    /// Maximum total lingering subscriptions
    pub max_total: usize,

    /// Eviction policy when limits are reached
    pub eviction_policy: EvictionPolicy,
}

#[derive(Clone, Debug)]
pub enum EvictionPolicy {
    /// Reject new lingering subscriptions
    RejectNew,
    /// Evict oldest lingering subscription
    EvictOldest,
    /// Evict subscription closest to timeout
    EvictNearestTimeout,
}

/// A pending subscription waiting for its track
pub struct LingeringSubscription {
    /// The subscription to fulfill when track arrives
    pub subscribed: Subscribed,

    /// Namespace being waited on
    pub namespace: TrackNamespace,

    /// Track name being waited on
    pub track_name: String,

    /// When this subscription was created
    pub created_at: Instant,

    /// When this subscription will timeout
    pub expires_at: Instant,

    /// Priority for eviction decisions
    pub priority: u8,
}

/// The store for lingering subscriptions
pub struct LingeringStore {
    config: LingeringConfig,

    /// Subscriptions indexed by (namespace, track_name)
    by_track: HashMap<(TrackNamespace, String), VecDeque<LingeringSubscription>>,

    /// Subscriptions indexed by namespace only (for namespace-level notifications)
    by_namespace: HashMap<TrackNamespace, HashSet<String>>,

    /// Total count for limit enforcement
    total_count: usize,

    /// Timeout heap for efficient expiration
    timeout_heap: BinaryHeap<Reverse<(Instant, (TrackNamespace, String), u64)>>,

    /// Metrics
    metrics: LingeringMetrics,
}

impl LingeringStore {
    /// Add a subscription to linger
    pub fn add(&mut self, sub: LingeringSubscription) -> Result<(), LingeringError>;

    /// Called when a track becomes available
    pub fn notify_track_available(
        &mut self,
        namespace: &TrackNamespace,
        track_name: &str
    ) -> Vec<LingeringSubscription>;

    /// Called when a namespace is announced
    pub fn notify_namespace_announced(
        &mut self,
        namespace: &TrackNamespace
    ) -> Vec<(String, Vec<LingeringSubscription>)>;

    /// Remove a specific subscription (e.g., on UNSUBSCRIBE)
    pub fn remove(&mut self, id: u64) -> Option<LingeringSubscription>;

    /// Process expired subscriptions
    pub fn expire_timeouts(&mut self) -> Vec<LingeringSubscription>;

    /// Get current metrics
    pub fn metrics(&self) -> &LingeringMetrics;
}
```

### 2. Wire Protocol Extension

Add a new subscription parameter to indicate lingering preference:

```rust
// In moq-transport/src/message/subscribe.rs

/// Extended subscription parameters for lingering
pub mod subscribe_params {
    /// Parameter key for linger mode
    pub const LINGER_MODE: u64 = 0x4C494E47; // "LING" in ASCII

    /// Parameter key for linger timeout (in milliseconds)
    pub const LINGER_TIMEOUT: u64 = 0x4C54494D; // "LTIM" in ASCII
}

#[derive(Clone, Debug, Default)]
pub enum LingerMode {
    /// Do not linger, return error immediately if track not found
    #[default]
    Disabled,
    /// Linger until timeout, using relay default timeout
    Enabled,
    /// Linger with custom timeout
    EnabledWithTimeout(Duration),
}
```

### 3. Modified Producer Flow

```rust
// In moq-relay-ietf/src/producer.rs

impl Producer {
    async fn serve_subscribe(self, subscribed: Subscribed) -> Result<(), anyhow::Error> {
        let namespace = subscribed.namespace().clone();
        let track_name = subscribed.track_name().to_string();

        // 1. Check local tracks first
        if let Some(mut local) = self.locals.retrieve(&namespace) {
            if let Some(track) = local.subscribe(namespace.clone(), &track_name) {
                return Ok(subscribed.serve(track).await?);
            }
        }

        // 2. Check remote tracks
        if let Some(track) = self.remotes.subscribe(namespace.clone(), track_name.clone()).await? {
            return Ok(subscribed.serve(track).await?);
        }

        // 3. NEW: Check if we should linger
        let linger_mode = subscribed.linger_mode();
        if self.lingering.config().enabled && linger_mode != LingerMode::Disabled {
            let timeout = match linger_mode {
                LingerMode::EnabledWithTimeout(t) => t.min(self.lingering.config().max_timeout),
                _ => self.lingering.config().default_timeout,
            };

            let lingering_sub = LingeringSubscription {
                subscribed,
                namespace: namespace.clone(),
                track_name: track_name.clone(),
                created_at: Instant::now(),
                expires_at: Instant::now() + timeout,
                priority: 0,
            };

            match self.lingering.add(lingering_sub) {
                Ok(()) => {
                    tracing::debug!(?namespace, ?track_name, "Subscription lingering");
                    // Subscription is now managed by LingeringStore
                    // It will be served when track appears or closed on timeout
                    return Ok(());
                }
                Err(LingeringError::LimitExceeded) => {
                    tracing::warn!(?namespace, ?track_name, "Lingering limit exceeded");
                    // Fall through to NOT_FOUND
                }
                Err(e) => {
                    tracing::error!(?e, "Failed to add lingering subscription");
                    // Fall through to NOT_FOUND
                }
            }
        }

        // 4. Track not found and can't/won't linger
        let err = ServeError::not_found(format!("track {}/{} not found", namespace, track_name));
        subscribed.close(err.clone())?;
        Err(err.into())
    }
}
```

### 4. NamespaceWatch for Publisher Events

```rust
// In moq-relay-ietf/src/local.rs (extended)

pub struct Locals {
    lookup: HashMap<TrackNamespace, TracksReader>,
    watchers: Vec<mpsc::Sender<NamespaceEvent>>,
}

#[derive(Clone, Debug)]
pub enum NamespaceEvent {
    /// A namespace was registered (publisher announced)
    NamespaceRegistered {
        namespace: TrackNamespace,
    },
    /// A specific track became available
    TrackAvailable {
        namespace: TrackNamespace,
        track_name: String,
    },
    /// A namespace was unregistered (publisher departed)
    NamespaceUnregistered {
        namespace: TrackNamespace,
    },
    /// A specific track became unavailable
    TrackUnavailable {
        namespace: TrackNamespace,
        track_name: String,
    },
}

impl Locals {
    /// Subscribe to namespace events
    pub fn watch(&mut self) -> mpsc::Receiver<NamespaceEvent> {
        let (tx, rx) = mpsc::channel(256);
        self.watchers.push(tx);
        rx
    }

    /// Register tracks, notifying watchers
    pub fn register(&mut self, tracks: TracksReader) -> Registration {
        let namespace = tracks.namespace().clone();
        self.lookup.insert(namespace.clone(), tracks);

        // Notify watchers
        self.notify(NamespaceEvent::NamespaceRegistered { namespace: namespace.clone() });

        Registration {
            namespace,
            locals: self.clone(), // Needs Arc<Mutex<>> in real impl
        }
    }

    fn notify(&self, event: NamespaceEvent) {
        for watcher in &self.watchers {
            let _ = watcher.try_send(event.clone());
        }
    }
}
```

### 5. Lingering Notification Loop

```rust
// In moq-relay-ietf/src/producer.rs

impl Producer {
    /// Background task to handle lingering subscription activation
    async fn lingering_notification_loop(
        mut self,
        mut events: mpsc::Receiver<NamespaceEvent>,
    ) {
        loop {
            tokio::select! {
                // Handle namespace events
                Some(event) = events.recv() => {
                    match event {
                        NamespaceEvent::NamespaceRegistered { namespace } => {
                            // Get all lingering subs for this namespace
                            let subs = self.lingering.notify_namespace_announced(&namespace);
                            for (track_name, pending) in subs {
                                // Try to fulfill each
                                for sub in pending {
                                    self.try_fulfill_lingering(sub).await;
                                }
                            }
                        }
                        NamespaceEvent::TrackAvailable { namespace, track_name } => {
                            let subs = self.lingering.notify_track_available(&namespace, &track_name);
                            for sub in subs {
                                self.try_fulfill_lingering(sub).await;
                            }
                        }
                        NamespaceEvent::NamespaceUnregistered { namespace } => {
                            // Optionally close or keep lingering
                            if !self.lingering.config().relinger_on_publisher_departure {
                                // Close all lingering for this namespace
                                self.close_lingering_for_namespace(&namespace).await;
                            }
                        }
                        _ => {}
                    }
                }

                // Handle timeouts
                _ = self.lingering.next_timeout() => {
                    let expired = self.lingering.expire_timeouts();
                    for sub in expired {
                        let err = ServeError::new(
                            ServeErrorCode::NotFound,
                            format!("linger timeout for {}/{}", sub.namespace, sub.track_name)
                        );
                        let _ = sub.subscribed.close(err);
                    }
                }
            }
        }
    }

    async fn try_fulfill_lingering(&mut self, sub: LingeringSubscription) {
        // Re-check if track is now available
        if let Some(mut local) = self.locals.retrieve(&sub.namespace) {
            if let Some(track) = local.subscribe(sub.namespace.clone(), &sub.track_name) {
                // Track found! Serve it
                tokio::spawn(async move {
                    if let Err(e) = sub.subscribed.serve(track).await {
                        tracing::warn!(?e, "Failed to serve lingering subscription");
                    }
                });
                return;
            }
        }

        // Still not available, re-add to lingering
        // (This handles race conditions)
        let _ = self.lingering.add(sub);
    }
}
```

---

## Edge Cases & Failure Modes

### EC-1: Subscriber Disconnects While Lingering

| Scenario                     | Handling                                                     |
| ---------------------------- | ------------------------------------------------------------ |
| TCP/QUIC connection drops    | LingeringStore detects via `Subscribed` state, removes entry |
| Subscriber sends UNSUBSCRIBE | `LingeringStore::remove()` called, subscription dropped      |
| Subscriber process crashes   | Same as connection drop                                      |

<!--
**Implementation:**
```rust
impl LingeringSubscription {
    /// Check if the underlying connection is still alive
    fn is_alive(&self) -> bool {
        !self.subscribed.is_closed()
    }
}

// Periodic cleanup in LingeringStore
fn cleanup_dead_subscriptions(&mut self) {
    self.by_track.retain(|_, subs| {
        subs.retain(|s| s.is_alive());
        !subs.is_empty()
    });
}
``` -->

### EC-2: Publisher Announces Then Immediately Departs

| Scenario                                                                     | Handling                                                  |
| ---------------------------------------------------------------------------- | --------------------------------------------------------- |
| Publisher announces, lingers activate, publisher departs subscription closes |
| Publisher announces, departs before any data                                 | Subscriptions transition to active then immediately close |

<!-- **Implementation:**
```rust
// Grace period handling
async fn handle_publisher_departure(
    &mut self,
    namespace: &TrackNamespace,
    config: &LingeringConfig,
) {
    if config.publisher_departure_grace.is_zero() {
        // No grace period, immediately handle
        self.process_publisher_departure(namespace).await;
    } else {
        // Start grace period timer
        let grace_handle = tokio::spawn({
            let namespace = namespace.clone();
            let duration = config.publisher_departure_grace;
            async move {
                tokio::time::sleep(duration).await;
                // Signal grace period ended
            }
        });

        self.grace_periods.insert(namespace.clone(), grace_handle);
    }
}
``` -->

### EC-3: Namespace vs Track Granularity

| Scenario                                              | Handling                                                                                   |
| ----------------------------------------------------- | ------------------------------------------------------------------------------------------ |
| Subscribe to track before namespace is even announced | DO NOT LINGER, fail immediatly because namespace doesn't exists (future feature to linger) |
| Wildcard subscription (future feature)                | Match against all tracks in namespace                                                      |

<!--
**Implementation:**
```rust
// Two-phase notification
fn notify_namespace_announced(&mut self, namespace: &TrackNamespace) -> Vec<...> {
    // Get all track names waiting in this namespace
    if let Some(tracks) = self.by_namespace.get(namespace) {
        // Don't immediately fulfill - wait for specific track availability
        // But DO notify subscribers that namespace exists (optional status update)
    }
}

fn notify_track_available(&mut self, namespace: &TrackNamespace, track: &str) -> Vec<...> {
    // THIS is when we actually fulfill lingering subscriptions
    self.by_track.remove(&(namespace.clone(), track.to_string()))
        .unwrap_or_default()
}
``` -->

### EC-4: Relay Restart / Crash

| Scenario                                   | Handling                                            |
| ------------------------------------------ | --------------------------------------------------- |
| Relay crashes with lingering subscriptions | All subscriptions lost (not persisted)              |
| Relay graceful shutdown                    | Send GOAWAY, subscribers reconnect to another relay |

### EC-5: Resource Exhaustion

| Scenario                                       | Handling                                                |
| ---------------------------------------------- | ------------------------------------------------------- |
| Memory exhaustion from too many lingering subs | Configurable limits prevent this                        |
| Slow subscriber causes backpressure            | Lingering subs don't consume data, only activation does |
| Coordinator overload from watches              | Batch notifications, rate limiting                      |

<!--
**Implementation:**

```rust
fn add(&mut self, sub: LingeringSubscription) -> Result<(), LingeringError> {
    // Check global limit
    if self.total_count >= self.config.max_total {
        return self.handle_limit_exceeded(sub);
    }

    // Check per-namespace limit
    let ns_count = self.by_namespace.get(&sub.namespace)
        .map(|t| t.len())
        .unwrap_or(0);
    if ns_count >= self.config.max_per_namespace {
        return self.handle_namespace_limit_exceeded(sub);
    }

    // Add subscription
    self.insert(sub);
    Ok(())
}

fn handle_limit_exceeded(&mut self, sub: LingeringSubscription) -> Result<(), LingeringError> {
    match self.config.eviction_policy {
        EvictionPolicy::RejectNew => Err(LingeringError::LimitExceeded),
        EvictionPolicy::EvictOldest => {
            self.evict_oldest();
            self.insert(sub);
            Ok(())
        }
        EvictionPolicy::EvictNearestTimeout => {
            self.evict_nearest_timeout();
            self.insert(sub);
            Ok(())
        }
    }
}
``` -->

### EC-6: Clock Skew / Time-Based Issues

| Scenario                                            | Handling                                                     |
| --------------------------------------------------- | ------------------------------------------------------------ |
| System clock jumps forward                          | Subscriptions may expire early; use monotonic clock          |
| System clock jumps backward                         | Subscriptions last longer than expected; use monotonic clock |
| Different timeout expectations between client/relay | Relay timeout is authoritative; client timeout is a hint     |

<!--
**Implementation:**

```rust
// Always use Instant (monotonic) for timeouts
pub struct LingeringSubscription {
    pub expires_at: Instant,  // NOT SystemTime
}
``` -->

### EC-7: Duplicate Subscriptions

| Scenario                                  | Handling                                                  |
| ----------------------------------------- | --------------------------------------------------------- |
| Same subscriber sends duplicate SUBSCRIBE | Reject second with "duplicate" error per MoQ spec         |
| Same subscriber reconnects, re-subscribes | New subscription ID, treated as new lingering             |
| Different subscribers to same track       | Both linger independently, both served when track appears |

### EC-8: Subscription Parameter Conflicts

| Scenario                                  | Handling                     |
| ----------------------------------------- | ---------------------------- |
| Client requests timeout > max_timeout     | Clamp to max_timeout         |
| Client requests linger but relay disabled | Return NOT_FOUND immediately |
| Client doesn't specify, relay has default | Use relay default            |

### EC-9: Cascading Relay Topology

| Scenario                                            | Handling                                               |
| --------------------------------------------------- | ------------------------------------------------------ |
| Edge relay lingers, origin relay doesn't have track | Edge lingers locally; queries coordinator periodically |
| Origin relay track appears, edge was lingering      | Coordinator notifies edge, edge fulfills lingering     |
| Multi-hop relay chain                               | Each hop can linger independently (v1 scope)           |

---

## Security Considerations

### SEC-1: Denial of Service via Lingering Flood

**Threat:** Attacker creates many lingering subscriptions to exhaust relay memory.

**Mitigations:**

-   Configurable global and per-namespace limits
-   Short default timeouts and max timeout
-   Lazy resource allocation somehow??
<!--

````rust
pub struct LingeringPolicy {
    /// Require authentication to linger
    pub require_auth: bool,

    /// Per-IP lingering limit
    pub max_per_ip: usize,

    /// Rate limit: max new lingering subs per second per IP
    pub rate_limit_per_ip: f64,
}
``` -->

### SEC-2: Information Disclosure

**Threat:** Attacker lingers on track names to discover what namespaces/tracks exist when publishers appear.

**Mitigations:**

-   Don't reveal whether a namespace "will" exist vs "might never" exist
-   Same error message for both timeout and policy rejection
-   The behaviour will be same as success subscribe when lingering is not without giving information about if track exists.

### SEC-3: Resource Starvation

**Threat:** Legitimate subscribers can't linger because limits are reached.

**Mitigations:**

-   Priority-based eviction?? (maybe authenticate or pro/ent users?)
-   Fair sharing across namespaces

---

## Performance Considerations

### Memory Overhead

Each lingering subscription holds:

-   `Subscribed` struct (~200 bytes including session reference)
-   Namespace + track name strings (~100 bytes typical)
-   Timeout/metadata (~50 bytes)
-   **Total: ~350 bytes per lingering subscription**

With default limit of 10,000 lingering subscriptions: **~3.5 MB**

### CPU Overhead

| Operation                | Complexity                         | Frequency           |
| ------------------------ | ---------------------------------- | ------------------- |
| Add lingering            | O(log n) heap insert               | Per subscribe miss  |
| Timeout check            | O(1) heap peek                     | Every 100ms         |
| Namespace notification   | O(m) where m = tracks in namespace | Per announce        |
| Track notification       | O(k) where k = subs for track      | Per track available |
| Cleanup dead connections | O(n)                               | Every 10s           |

### Scalability

| Scale         | Recommendation                           |
| ------------- | ---------------------------------------- |
| <1K lingering | Single HashMap, simple implementation    |
| 1K-100K       | Sharded HashMap, background cleanup      |
| >100K         | Consider distributed state, Redis-backed |

---

## Configuration & Tunables

### Relay Configuration

```toml
[lingering]
# Enable/disable lingering feature
enabled = true

# Default timeout for lingering subscriptions
default_timeout = "30s"

# Maximum timeout a subscriber can request
max_timeout = "5m"

# Maximum lingering subscriptions per namespace
max_per_namespace = 1000

# Maximum total lingering subscriptions
max_total = 10000

# Eviction policy: "reject_new", "evict_oldest", "evict_nearest_timeout"
eviction_policy = "evict_oldest"

# Whether active subscriptions re-linger when publisher departs
relinger_on_publisher_departure = true

# Grace period before processing publisher departure
publisher_departure_grace = "5s"

# Cleanup interval for dead connections
cleanup_interval = "10s"

[lingering.security]
# Require authentication to linger
require_auth = false

# Per-IP lingering limit (0 = unlimited)
max_per_ip = 100

# Rate limit for new lingering subs per IP per second
rate_limit_per_ip = 10.0
````

### Environment Variables

```bash
# Override any config value
MOQ_LINGERING_ENABLED=true
MOQ_LINGERING_DEFAULT_TIMEOUT=30s
MOQ_LINGERING_MAX_TOTAL=10000
```

### Command-Line Arguments

```bash
moq-relay \
  --lingering-enabled \
  --lingering-default-timeout 30s \
  --lingering-max-total 10000
```

---

## Migration & Compatibility

### Backward Compatibility

| Client Version               | Behavior                                  |
| ---------------------------- | ----------------------------------------- |
| Old client (no linger param) | Uses relay default (linger if enabled)    |
| Old client + new relay       | Works, lingers by default if enabled      |
| New client + old relay       | Linger param ignored, immediate NOT_FOUND |
| New client + new relay       | Full feature support                      |

### Protocol Versioning

The lingering feature uses **extension parameters** in SUBSCRIBE, which are ignored by implementations that don't understand them. No protocol version bump required.

### Rollout Strategy

1. **Phase 1:** Add LingeringStore with feature flag disabled
2. **Phase 2:** Enable in VET, test with synthetic load
3. **Phase 3:** Enable in production with conservative limits
4. **Phase 4:** Tune limits based on observed usage
5. **Phase 5:** Document client-side linger parameter usage

---

## Testing Strategy

### Unit Tests

```rust
#[cfg(test)]
mod tests {
    #[test]
    fn test_add_lingering_subscription() { }

    #[test]
    fn test_timeout_expiration() { }

    #[test]
    fn test_limit_enforcement() { }

    #[test]
    fn test_eviction_policies() { }

    #[test]
    fn test_namespace_notification() { }

    #[test]
    fn test_track_notification() { }

    #[test]
    fn test_dead_connection_cleanup() { }
}
```

### Integration Tests

```rust
#[tokio::test]
async fn test_subscriber_before_publisher() {
    // 1. Start relay with lingering enabled
    // 2. Connect subscriber, subscribe to track
    // 3. Verify subscription is lingering (no error)
    // 4. Connect publisher, announce namespace
    // 5. Publish track
    // 6. Verify subscriber receives data
}

#[tokio::test]
async fn test_publisher_departure_and_return() {
    // 1. Publisher publishes track
    // 2. Subscriber subscribes (active)
    // 3. Publisher departs
    // 4. Verify subscriber enters lingering (or closes, based on config)
    // 5. Publisher returns
    // 6. Verify subscriber re-activates
}

#[tokio::test]
async fn test_timeout_expiration() {
    // 1. Subscriber subscribes with short timeout
    // 2. Wait for timeout
    // 3. Verify subscriber receives NOT_FOUND error
}
```

### Load Tests

-   10,000 concurrent lingering subscriptions
-   Publisher announce storm (1000 namespaces/second)
-   Memory usage under sustained load
-   Latency from track available to subscription activation

### Chaos Tests

-   Kill relay mid-lingering
-   Network partition between relay and coordinator
-   Clock skew simulation
-   Memory pressure simulation

---

## Metrics & Observability

### Metrics

```rust
pub struct LingeringMetrics {
    /// Current number of lingering subscriptions
    pub current_count: Gauge,

    /// Total lingering subscriptions added
    pub total_added: Counter,

    /// Total lingering subscriptions fulfilled
    pub total_fulfilled: Counter,

    /// Total lingering subscriptions timed out
    pub total_timed_out: Counter,

    /// Total lingering subscriptions cancelled
    pub total_cancelled: Counter,

    /// Total lingering subscriptions evicted
    pub total_evicted: Counter,

    /// Histogram of linger durations (time to fulfillment)
    pub fulfillment_duration: Histogram,

    /// Histogram of linger durations (time to timeout)
    pub timeout_duration: Histogram,

    /// Current count by namespace (high cardinality, optional)
    pub by_namespace: HashMap<String, Gauge>,
}
```

### Log Events

```rust
// Structured logging events
tracing::info!(
    namespace = %namespace,
    track = %track_name,
    timeout_ms = timeout.as_millis(),
    "subscription_lingering_started"
);

tracing::info!(
    namespace = %namespace,
    track = %track_name,
    waited_ms = waited.as_millis(),
    "subscription_lingering_fulfilled"
);

tracing::warn!(
    namespace = %namespace,
    track = %track_name,
    waited_ms = waited.as_millis(),
    "subscription_lingering_timeout"
);
```

### Health Checks

```
GET /health/lingering
{
  "enabled": true,
  "current_count": 1234,
  "max_total": 10000,
  "utilization_percent": 12.34,
  "oldest_subscription_age_ms": 45000
}
```

---

## Open Questions

### OQ-1: Should lingering be opt-in or opt-out?

**Options:**

-   A) Opt-in: Subscribers must explicitly request lingering (safer, more explicit)
-   B) Opt-out: Lingering is default, subscribers can disable (better UX, potential resource issues)
-   C) Relay-configurable default (most flexible)

**Recommendation:** Option C - allow relay operators to choose the default behavior.

### OQ-2: What happens with existing active subscriptions when publisher departs?

**Options:**

-   A) Close immediately with DONE
-   B) Close immediately with error
-   C) Transition to lingering (wait for publisher return)
-   D) Configurable per-subscription

**Recommendation:** Option D - let the subscription (or relay policy) decide.

### OQ-3: Should the coordinator be extended for cross-relay lingering?

**Context:** Currently, lingering is local to one relay. If subscriber is at Relay A and publisher will appear at Relay B, Relay A has no way to know to wait.

**Options:**

-   A) No cross-relay support (v1)
-   B) Coordinator gets a "watch namespace" API
-   C) Relay-to-relay gossip about "interested namespaces"

**Recommendation:** Option A for v1, design Option B for v2.

### OQ-4: How should subscription priority affect lingering?

**Context:** SUBSCRIBE has a `subscriber_priority` field. Should this affect lingering?

**Options:**

-   A) Ignore priority for lingering
-   B) Higher priority = longer allowed timeout
-   C) Higher priority = preferred for fulfillment order
-   D) Higher priority = protected from eviction

**Recommendation:** Option D for v1.

### OQ-5: Should we support partial fulfillment?

**Context:** Subscriber wants `video` track, namespace has `audio` only. Should we notify subscriber that namespace exists but track doesn't?

**Options:**

-   A) No - only fulfill when exact track exists
-   B) Yes - send intermediate status messages
-   C) Future feature - subscription to namespace-level events

**Recommendation:** Option A for v1, consider B/C for v2.

---

## Appendix A: Alternative Designs Considered

### A1: Client-Side Retry

**Approach:** Instead of relay lingering, have clients retry with exponential backoff.

**Pros:**

-   Simpler relay implementation
-   No relay resource consumption

**Cons:**

-   Poor user experience (error messages)
-   Inconsistent retry behavior across clients
-   Unnecessary network traffic

**Verdict:** Rejected - poor UX, doesn't solve the core problem.

### A2: Coordinator-Based Waiting

**Approach:** Subscriptions wait at the coordinator level, not the relay level.

**Pros:**

-   Works across relays
-   Single source of truth

**Cons:**

-   Coordinator becomes stateful and complex
-   Higher latency for fulfillment
-   Coordinator becomes bottleneck

**Verdict:** Deferred to v2 - too complex for initial implementation.

### A3: Separate "Waiting Room" Service

**Approach:** Dedicated microservice for managing waiting subscriptions.

**Pros:**

-   Separation of concerns
-   Independent scaling

**Cons:**

-   Additional operational complexity
-   More network hops
-   State synchronization challenges

**Verdict:** Rejected - over-engineered for the problem size.

---

## Appendix B: Related Work

### WebRTC

WebRTC uses ICE/STUN/TURN for connection establishment, which has similar "wait for peer" semantics through the signaling server. However, it's connection-oriented, not subscription-oriented.

### MQTT

MQTT has "retained messages" where the broker holds the last message for a topic. New subscribers immediately receive the retained message. This is similar but focuses on data retention, not subscription lingering.

### Apache Kafka

Kafka consumers can specify offsets including "latest" which waits for new data. However, Kafka topics must exist before consumption (no "wait for topic" semantic).

### Redis Pub/Sub

Redis Pub/Sub has no lingering - subscribers only receive messages published after they subscribe. Redis Streams (with XREAD BLOCK) provides waiting semantics.

---

## Appendix C: Glossary

| Term             | Definition                                                                                   |
| ---------------- | -------------------------------------------------------------------------------------------- |
| **Lingering**    | State where a subscription is held pending, waiting for its target track to become available |
| **Fulfill**      | Transitioning a lingering subscription to active when its track becomes available            |
| **Evict**        | Forcibly closing a lingering subscription due to resource limits                             |
| **Grace Period** | Time window after publisher departure before triggering re-lingering or closure              |
| **Namespace**    | A hierarchical grouping of tracks (e.g., `streaming/live/channel123`)                        |
| **Track**        | A single media stream within a namespace (e.g., `video`, `audio`)                            |

---

## Document History

| Version | Date     | Author | Changes       |
| ------- | -------- | ------ | ------------- |
| 1.0     | Dec 2024 | -      | Initial draft |
