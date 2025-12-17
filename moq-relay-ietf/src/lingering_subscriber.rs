use std::{
    collections::HashMap,
    sync::Arc,
    time::{Duration, Instant},
};

use moq_transport::{coding::TrackNamespace, session::Subscribed};
use tokio::sync::Mutex;

#[derive(thiserror::Error, Debug)]
pub enum LingeringError {
    #[error("lingering subscription limit reached")]
    SubscriptionLimitReached,

    #[error("lingering subscription not found")]
    SubscriptionNotFound,

    #[error("lingering subscription already exists")]
    SubscriptionAlreadyExists,

    #[error("internal error: {0}")]
    Other(anyhow::Error),
}

/// Configuration for lingering subscribers.
#[derive(Debug, Clone)]
pub struct LingeringConfig {
    /// Whether lingering subscribers are enabled.
    pub enabled: bool,

    /// Duration to wait before closing lingering subscribers.
    pub default_expiry_duration: Duration,

    /// Maximum allowed timeout for lingering subscribers.
    pub max_expiry_duration: Duration,

    /// Maximum number of lingering subscribers.
    pub max_subscriptions_per_session: usize,

    /// Maximum total number of lingering subscribers.
    /// FIXME(itzmanish): currently the producers doesn't know overall number of subscribers because
    /// producers are tied to sessions, so max_total_subscriptions is ignored
    pub max_total_subscriptions: usize,

    /// Eviction policy when limits are reached.
    pub eviction_policy: EvictionPolicy,
}

impl LingeringConfig {
    pub fn default() -> Self {
        Self {
            enabled: true,
            default_expiry_duration: Duration::from_secs(60), // 1 minutes
            max_expiry_duration: Duration::from_secs(60 * 30), // 30 minutes
            // these are very conservative values, suggested to tailer to actual usage
            max_subscriptions_per_session: 10,
            max_total_subscriptions: 100,
            eviction_policy: EvictionPolicy::EvictOldest,
        }
    }

    pub fn is_enabled(&self) -> bool {
        self.enabled
    }

    pub fn allowed(&self, active_lingering_subscriptions: usize) -> bool {
        self.is_enabled() && active_lingering_subscriptions < self.max_subscriptions_per_session
    }
}

#[derive(Debug, Clone, Copy)]
#[repr(u8)]
pub enum EvictionPolicy {
    /// Reject new lingering subscriptions.
    RejectNew,
    /// Evict the oldest lingering subscriber first.
    EvictOldest,
    /// Evict subscribers with the shortest remaining time first.
    EvictNearestExpiry,
}

#[derive(Clone)]
pub struct LingeringSubscription {
    pub subscribed: Subscribed,
    /// Time when the lingering subscription was created.
    pub created_at: Instant,
    /// Time when the lingering subscription expires.
    pub expires_at: Instant,
    /// Priority for eviction decisions.
    pub eviction_priority: u8,
}

impl LingeringSubscription {
    pub fn new(subscribed: Subscribed, expires_at: Instant) -> Self {
        Self {
            subscribed,
            created_at: Instant::now(),
            expires_at,
            eviction_priority: 0,
        }
    }
    /// Check if the lingering subscription has expired.
    pub fn is_expired(&self) -> bool {
        Instant::now() >= self.expires_at
    }
}

type TrackMap = HashMap<(TrackNamespace, String), Vec<LingeringSubscription>>;

/// Lingering subscription store is per session.
/// Uses Arc<Mutex<>> for shared mutable access without expensive cloning.
#[derive(Clone)]
pub struct LingeringSubscriptionStore {
    config: LingeringConfig,
    by_track: Arc<Mutex<TrackMap>>,
}

impl LingeringSubscriptionStore {
    pub fn new(config: LingeringConfig) -> Self {
        Self {
            config,
            by_track: Arc::new(Mutex::new(HashMap::new())),
        }
    }

    pub async fn lingering_allowed(&self) -> bool {
        let by_track = self.by_track.lock().await;
        self.config.allowed(by_track.len())
    }

    pub async fn add_subscription(&self, subscription: Subscribed) -> Result<(), LingeringError> {
        // FIXME(itzmanish): right now the lingering is handled only if the relay server
        // enabled it but eventually we need to consider lingering as default if not specified
        // and based on the client sending susbcription request with lingering enabled parameter
        //

        let subscription = LingeringSubscription::new(subscription, Instant::now());
        // check if we already have this subscription
        let key = (
            subscription.subscribed.track_namespace.clone(),
            subscription.subscribed.track_name.clone(),
        );

        let mut by_track = self.by_track.lock().await;
        let subscriptions = by_track.entry(key).or_default();

        if subscriptions
            .iter()
            .any(|sub| sub.subscribed.id == subscription.subscribed.id)
        {
            return Err(LingeringError::SubscriptionAlreadyExists);
        }

        if subscriptions.len() >= self.config.max_subscriptions_per_session {
            return Err(LingeringError::SubscriptionLimitReached);
        }

        subscriptions.push(subscription);

        Ok(())
    }

    pub async fn notify_track_available(
        &self,
        namespace: &TrackNamespace,
        track_name: &str,
    ) -> Vec<LingeringSubscription> {
        self.remove_subscriptions(namespace, track_name)
            .await
            .unwrap_or_default()
    }

    pub async fn remove_subscriptions(
        &self,
        namespace: &TrackNamespace,
        track_name: &str,
    ) -> Result<Vec<LingeringSubscription>, LingeringError> {
        let key = (namespace.clone(), track_name.to_string());
        let mut by_track = self.by_track.lock().await;
        if let Some(subscription) = by_track.remove(&key) {
            Ok(subscription)
        } else {
            Err(LingeringError::SubscriptionNotFound)
        }
    }

    pub async fn remove_subscription(
        &self,
        namespace: &TrackNamespace,
        track_name: &str,
        subscription_id: u64,
    ) -> Result<LingeringSubscription, LingeringError> {
        let key = (namespace.clone(), track_name.to_string());
        let mut by_track = self.by_track.lock().await;
        if let Some(subscriptions) = by_track.get_mut(&key) {
            if let Some(sub_position) = subscriptions
                .iter()
                .position(|sub| sub.subscribed.id == subscription_id)
            {
                return Ok(subscriptions.remove(sub_position));
            }
        }
        Err(LingeringError::SubscriptionNotFound)
    }

    pub async fn evict_expired(&self) {
        let mut by_track = self.by_track.lock().await;
        for subscription in by_track.values_mut() {
            subscription.retain(|sub| !sub.is_expired());
        }
    }

    /// Acquires the lock and returns a guard for direct access to the underlying HashMap.
    /// The caller can hold this lock across await points and iterate/modify as needed.
    pub async fn lock(
        &self,
    ) -> tokio::sync::MutexGuard<'_, HashMap<(TrackNamespace, String), Vec<LingeringSubscription>>>
    {
        self.by_track.lock().await
    }
}
