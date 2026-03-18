use std::net::SocketAddr;

use async_trait::async_trait;
use moq_native_ietf::quic;
use moq_transport::coding::TrackNamespace;
use url::Url;

#[derive(Debug, thiserror::Error)]
pub enum CoordinatorError {
    #[error("namespace not found")]
    NamespaceNotFound,

    #[error("namespace already registered")]
    NamespaceAlreadyRegistered,

    #[error("Internal Error: {0}")]
    Other(anyhow::Error),
}

impl From<anyhow::Error> for CoordinatorError {
    fn from(err: anyhow::Error) -> Self {
        Self::Other(err)
    }
}

impl From<tokio::task::JoinError> for CoordinatorError {
    fn from(err: tokio::task::JoinError) -> Self {
        Self::Other(err.into())
    }
}

impl From<std::io::Error> for CoordinatorError {
    fn from(err: std::io::Error) -> Self {
        Self::Other(err.into())
    }
}

pub type CoordinatorResult<T> = std::result::Result<T, CoordinatorError>;

/// Handle returned when a namespace is registered with the coordinator.
///
/// Dropping this handle automatically unregisters the namespace.
/// This provides RAII-based cleanup - when the publisher disconnects
/// or the namespace is no longer served, cleanup happens automatically.
pub struct NamespaceRegistration {
    _inner: Box<dyn Send + Sync>,
    _metadata: Option<Vec<(String, String)>>,
}

impl NamespaceRegistration {
    /// Create a new registration handle wrapping any Send + Sync type.
    ///
    /// The wrapped value's `Drop` implementation will be called when
    /// this registration is dropped.
    pub fn new<T: Send + Sync + 'static>(inner: T) -> Self {
        Self {
            _inner: Box::new(inner),
            _metadata: None,
        }
    }

    /// Add metadata as list of key value pair of string: string
    pub fn with_metadata(mut self, metadata: Vec<(String, String)>) -> Self {
        self._metadata = Some(metadata);
        self
    }
}

/// Result of a namespace lookup.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct NamespaceOrigin {
    /// The namespace of the track belongs to
    namespace: TrackNamespace,
    /// The URL of the relay serving this namespace
    /// If the relay is not discoverable via this URL, use `socket_addr`
    /// But you still have to pass a valid URL because the TLS verification
    /// happens for hostname
    url: Url,
    /// The socket address of the relay if the relay is not approachable
    /// via DNS lookup, This is to bypass DNS lookups.
    socket_addr: Option<SocketAddr>,
    /// Additional metadata associated with this namespace
    metadata: Option<Vec<(String, String)>>,
}

impl NamespaceOrigin {
    /// Create a new NamespaceOrigin.
    pub fn new(namespace: TrackNamespace, url: Url, addr: Option<SocketAddr>) -> Self {
        Self {
            namespace,
            url,
            socket_addr: addr,
            metadata: None,
        }
    }
    pub fn with_metadata(mut self, values: (String, String)) -> Self {
        if let Some(metadata) = &mut self.metadata {
            metadata.push(values);
        } else {
            self.metadata = Some(vec![values]);
        }
        self
    }

    /// Get the namespace.
    pub fn namespace(&self) -> &TrackNamespace {
        &self.namespace
    }

    /// Get the URL of the relay serving this namespace.
    pub fn url(&self) -> Url {
        self.url.clone()
    }

    pub fn addr(&self) -> Option<SocketAddr> {
        self.socket_addr
    }

    /// Get the metadata associated with this namespace.
    pub fn metadata(&self) -> Option<Vec<(String, String)>> {
        self.metadata.clone()
    }
}

/// Information about the resolved scope for a connection.
///
/// Returned by [`Coordinator::resolve_scope()`] to tell the relay:
/// - Which scope this connection belongs to (for routing and namespace isolation)
/// - What the connection is allowed to do (for permission enforcement)
///
/// Multiple connection paths can map to the same `scope_id` — for example,
/// a publisher path and a subscriber path that share a scope but have
/// different permissions.
#[derive(Debug, Clone)]
pub struct ScopeInfo {
    /// The resolved scope identity. Used as the key for namespace
    /// registration and lookup in all subsequent coordinator operations.
    ///
    /// Multiple connection paths can map to the same `scope_id`.
    pub scope_id: String,

    /// What this connection is allowed to do within the scope.
    pub permissions: ScopePermissions,
}

/// Permissions granted to a connection within its scope.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ScopePermissions {
    /// Can both publish (PUBLISH_NAMESPACE) and subscribe (SUBSCRIBE/FETCH).
    ReadWrite,
    /// Can subscribe/fetch only. Publishing attempts will be rejected
    /// by the relay (the Consumer side of the session will not be created).
    ReadOnly,
}

impl ScopePermissions {
    /// Whether this permission level allows publishing (PUBLISH_NAMESPACE).
    pub fn can_publish(&self) -> bool {
        matches!(self, Self::ReadWrite)
    }

    /// Whether this permission level allows subscribing (SUBSCRIBE/FETCH).
    ///
    /// Always returns `true` — both `ReadWrite` and `ReadOnly` connections
    /// can subscribe. This is intentional: the asymmetry with [`can_publish()`]
    /// reflects that subscribing is the baseline capability, while publishing
    /// requires elevated permissions. If a future permission level needs to
    /// deny subscribing, a new variant should be added.
    ///
    /// [`can_publish()`]: ScopePermissions::can_publish
    pub fn can_subscribe(&self) -> bool {
        true
    }
}

/// Coordinator handles namespace registration/discovery across relays.
///
/// Implementations are responsible for:
/// - Tracking which namespaces are served locally
/// - Caching remote namespace lookups
/// - Communicating with external registries (HTTP API, Redis, etc.)
/// - Periodic refresh/heartbeat of registrations
/// - Cleanup when registrations are dropped
///
/// # Thread Safety
///
/// All methods take `&self` and implementations must be thread-safe.
/// Multiple tasks will call these methods concurrently.
#[async_trait]
pub trait Coordinator: Send + Sync {
    /// Resolve a connection path to scope information.
    ///
    /// Called once per accepted session, before any register/lookup calls.
    /// The relay uses the returned [`ScopeInfo`] to:
    /// - Scope all subsequent coordinator operations to `scope_id`
    /// - Enforce permissions (e.g., skip creating the publish side for
    ///   `ReadOnly` connections)
    ///
    /// # Arguments
    ///
    /// * `connection_path` - The raw connection path from the WebTransport
    ///   URL or CLIENT_SETUP PATH parameter. `None` if no path was present.
    ///
    /// # Returns
    ///
    /// - `Ok(Some(ScopeInfo))` - Connection is scoped with the given
    ///   identity and permissions.
    /// - `Ok(None)` - Connection is unscoped. The relay will pass
    ///   `scope: None` to all subsequent coordinator calls and allow
    ///   both publish and subscribe.
    /// - `Err(...)` - Connection should be rejected (e.g., unrecognized
    ///   path, unauthorized).
    ///
    /// # Default Implementation
    ///
    /// Passes through the connection path as the `scope_id` with
    /// `ReadWrite` permissions. Connections without a path are unscoped.
    async fn resolve_scope(
        &self,
        connection_path: Option<&str>,
    ) -> CoordinatorResult<Option<ScopeInfo>> {
        Ok(connection_path.map(|path| ScopeInfo {
            scope_id: path.to_string(),
            permissions: ScopePermissions::ReadWrite,
        }))
    }

    /// Register a namespace as locally available on this relay.
    ///
    /// Called when a publisher sends PUBLISH_NAMESPACE.
    /// The coordinator should:
    /// 1. Record the namespace as locally available
    /// 2. Advertise to external registry if configured
    /// 3. Start any refresh/heartbeat tasks
    /// 4. Return a handle that unregisters on drop
    ///
    /// # Arguments
    ///
    /// * `scope` - The resolved scope identity, or `None` for unscoped
    ///   sessions. Used to isolate namespace registrations — the same
    ///   namespace in different scopes may route independently.
    /// * `namespace` - The namespace being registered
    ///
    /// # Returns
    ///
    /// A `NamespaceRegistration` handle. The namespace remains registered
    /// as long as this handle is held. Dropping it unregisters the namespace.
    async fn register_namespace(
        &self,
        scope: Option<&str>,
        namespace: &TrackNamespace,
    ) -> CoordinatorResult<NamespaceRegistration>;

    /// Unregister a namespace.
    ///
    /// Called when a publisher sends PUBLISH_NAMESPACE_DONE.
    /// This is an explicit unregistration - the registration handle may still exist
    /// but the namespace should be removed from the registry.
    ///
    /// # Arguments
    ///
    /// * `scope` - The resolved scope identity, or `None` for unscoped sessions.
    /// * `namespace` - The namespace to unregister
    async fn unregister_namespace(
        &self,
        scope: Option<&str>,
        namespace: &TrackNamespace,
    ) -> CoordinatorResult<()>;

    /// Lookup where a namespace is served from.
    ///
    /// Called when a subscriber requests a namespace.
    /// The coordinator should check in order:
    /// 1. Local registrations (return `Local`)
    /// 2. Cached remote lookups (return `Remote(url)` if not expired)
    /// 3. External registry (cache and return result)
    ///
    /// # Arguments
    ///
    /// * `scope` - The resolved scope identity, or `None` for unscoped
    ///   sessions. Coordinators use this to scope lookups (e.g., to route
    ///   to the correct origin for a particular application).
    /// * `namespace` - The namespace to look up
    ///
    /// # Returns
    ///
    /// - `Ok(NamespaceOrigin, Option<quic::Client>)` - Namespace origin and optional client if available
    /// - `Err` - Namespace not found anywhere
    async fn lookup(
        &self,
        scope: Option<&str>,
        namespace: &TrackNamespace,
    ) -> CoordinatorResult<(NamespaceOrigin, Option<quic::Client>)>;

    /// Graceful shutdown of the coordinator.
    ///
    /// Called when the relay is shutting down. Implementations should:
    /// - Unregister all local namespaces and tracks
    /// - Cancel refresh tasks
    /// - Close connections to external registries
    async fn shutdown(&self) -> CoordinatorResult<()> {
        Ok(())
    }
}
