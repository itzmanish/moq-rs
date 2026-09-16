// SPDX-FileCopyrightText: 2024-2026 Cloudflare Inc., Luke Curley, Mike English and contributors
// SPDX-License-Identifier: MIT OR Apache-2.0

use std::collections::HashSet;
use std::sync::Arc;
use std::time::Duration;

use futures::{stream::FuturesUnordered, FutureExt, StreamExt};
use moq_transport::{
    coding::{KeyValuePairs, TrackNamespace},
    message::{RequestErrorCode, SubscribeOptions},
    serve::{FullTrackName, ServeError, TrackReader, TracksReader},
    session::{
        FetchRequested, Publisher, SessionError, Subscribed, SubscribedNamespace,
        TrackStatusRequested,
    },
};
use tokio::sync::broadcast;

use crate::{
    metrics::{GaugeGuard, TimingGuard},
    upstream_namespaces::UpstreamNamespaces,
    Coordinator, Locals, NamespaceChange, RemoteManager, SessionContext, TrackChange,
    UpstreamReady,
};

/// Producer of tracks to a remote Subscriber
#[derive(Clone)]
pub struct Producer {
    publisher: Publisher,
    locals: Locals,
    remotes: RemoteManager,
    upstream_namespaces: UpstreamNamespaces,
    /// Relay-level context for this MoQT session.
    context: SessionContext,
}

/// Why the wait for upstream readiness ended without the subscription being
/// established.
///
/// The two cases carry the same [`ServeError`] variants — an upstream rejection
/// can be `Cancel` or `Done` just as a departing subscriber can — so the
/// direction has to come from which branch resolved rather than from the error.
/// Getting it wrong misreports publisher failures as subscriber cancellations in
/// the subscribe metrics.
enum UpstreamWait {
    /// The upstream subscription could not be established.
    UpstreamFailed(ServeError),

    /// The downstream subscriber went away before it was established.
    DownstreamLeft(ServeError),
}

impl Producer {
    pub fn new(
        publisher: Publisher,
        locals: Locals,
        remotes: RemoteManager,
        coordinator: Arc<dyn Coordinator>,
        context: SessionContext,
    ) -> Self {
        let (upstream_namespaces, runner) =
            UpstreamNamespaces::new(locals.clone(), remotes.clone(), coordinator);
        tokio::spawn(runner.run());
        Self::new_with_upstream_namespaces(publisher, locals, remotes, upstream_namespaces, context)
    }

    pub(crate) fn new_with_upstream_namespaces(
        publisher: Publisher,
        locals: Locals,
        remotes: RemoteManager,
        upstream_namespaces: UpstreamNamespaces,
        context: SessionContext,
    ) -> Self {
        Self {
            publisher,
            locals,
            remotes,
            upstream_namespaces,
            context,
        }
    }

    /// Send PUBLISH_NAMESPACE for a set of tracks to the remote peer.
    pub async fn publish_namespace(&mut self, tracks: TracksReader) -> Result<(), SessionError> {
        self.publisher.publish_namespace(tracks).await
    }

    /// Run the producer to serve subscribe requests.
    pub async fn run(self) -> Result<(), SessionError> {
        let mut tasks: FuturesUnordered<futures::future::BoxFuture<'static, ()>> =
            FuturesUnordered::new();

        loop {
            let mut publisher_subscribed = self.publisher.clone();
            let mut publisher_track_status = self.publisher.clone();
            let mut publisher_subscribed_namespace = self.publisher.clone();
            let mut publisher_fetch = self.publisher.clone();

            tokio::select! {
                // Handle a new subscribe request
                Some(subscribed) = publisher_subscribed.subscribed() => {
                    metrics::counter!("moq_relay_subscribers_total").increment(1);

                    let this = self.clone();

                    // Spawn a new task to handle the subscribe
                    tasks.push(async move {
                        let info = subscribed.clone();
                        let namespace = info.track_namespace.to_utf8_path();
                        let track_name = info.track_name.clone();
                        tracing::info!(namespace = %namespace, track = %track_name, "serving subscribe: {:?}", info);

                        // Serve the subscribe request
                        if let Err(err) = this.serve_subscribe(subscribed).await {
                            if Self::is_expected_serve_shutdown(&err) {
                                tracing::debug!(namespace = %namespace, track = %track_name, subscribe_info = ?info, error = %err, "stopped serving subscribe");
                            } else {
                                tracing::warn!(namespace = %namespace, track = %track_name, subscribe_info = ?info, error = %err, "failed serving subscribe");
                            }
                        }
                    }.boxed())
                },
                // Handle a new track_status request
                Some(track_status_requested) = publisher_track_status.track_status_requested() => {
                    let this = self.clone();

                    // Spawn a new task to handle the track_status request
                    tasks.push(async move {
                        let info = track_status_requested.request_msg.clone();
                        let namespace = info.track_namespace.to_utf8_path();
                        let track_name = info.track_name.clone();
                        tracing::info!(namespace = %namespace, track = %track_name, "serving track_status: {:?}", info);

                        // Serve the track_status request
                        if let Err(err) = this.serve_track_status(track_status_requested).await {
                            tracing::warn!(namespace = %namespace, track = %track_name, error = %err, "failed serving track_status: {:?}, error: {}", info, err)
                        }
                    }.boxed())
                },
                // Handle a new namespace subscription request.
                Some(subscribed_namespace) = publisher_subscribed_namespace.subscribed_namespace() => {
                    let this = self.clone();

                    tasks.push(async move {
                        let prefix = subscribed_namespace.namespace_prefix.to_utf8_path();
                        tracing::info!(namespace_prefix = %prefix, "serving subscribe namespace");

                        if let Err(err) = this.serve_subscribe_namespace(subscribed_namespace).await {
                            if Self::is_expected_serve_shutdown(&err) {
                                tracing::debug!(namespace_prefix = %prefix, error = %err, "stopped serving subscribe namespace");
                            } else {
                                tracing::warn!(namespace_prefix = %prefix, error = %err, "failed serving subscribe namespace");
                            }
                        }
                    }.boxed())
                },
                Some(fetch) = publisher_fetch.fetch_requested() => {
                    let this = self.clone();
                    tasks.push(async move {
                        if let Err(err) = this.serve_fetch(fetch).await {
                            tracing::debug!(error = %err, "failed serving FETCH");
                        }
                    }.boxed())
                },
                _= tasks.next(), if !tasks.is_empty() => {},
                else => return Ok(()),
            };
        }
    }

    async fn serve_fetch(self, fetch: FetchRequested) -> Result<(), anyhow::Error> {
        let deadline = fetch.deadline(Duration::from_secs(30));
        let standalone = fetch
            .request
            .standalone_fetch
            .as_ref()
            .ok_or_else(|| anyhow::anyhow!("standalone FETCH missing range"))?;
        let Some(mut source) = self
            .locals
            .fetch_source(self.context.scope(), &standalone.track_namespace)
        else {
            fetch.reject(
                RequestErrorCode::NotSupported,
                "FETCH requires a local namespace publisher",
            )?;
            return Ok(());
        };
        let params = match upstream_fetch_params(&fetch.request.params) {
            Ok(params) => params,
            Err(err) => {
                fetch.reject(RequestErrorCode::InternalError, "invalid FETCH parameters")?;
                return Err(err.into());
            }
        };
        let upstream = match source.fetch(standalone.clone(), params) {
            Ok(upstream) => upstream,
            Err(err) => {
                fetch.reject(
                    RequestErrorCode::InternalError,
                    "failed to open upstream FETCH",
                )?;
                return Err(err.into());
            }
        };
        fetch
            .proxy(
                upstream,
                deadline.saturating_duration_since(tokio::time::Instant::now()),
            )
            .await?;
        Ok(())
    }

    /// Serve a subscribe request.
    async fn serve_subscribe(self, subscribed: Subscribed) -> Result<(), anyhow::Error> {
        // Track subscribe latency from request to track resolution (records on drop)
        let mut timing_guard =
            TimingGuard::with_label("moq_relay_subscribe_latency_seconds", "source", "not_found");
        // Track active subscriptions - decrements when this function returns
        let _sub_guard = GaugeGuard::new("moq_relay_active_subscriptions");

        let namespace = subscribed.track_namespace.clone();
        let track_name = subscribed.track_name.clone();

        // Local lookup order inside Locals:
        // 1. actual FullTrackName -> TrackReader media cache
        // 2. PUBLISH_NAMESPACE route source, which triggers upstream SUBSCRIBE
        let mut locals = self.locals.clone();
        if let Some(local) = locals
            .get_or_request_track(self.context.scope(), namespace.clone(), &track_name)
            .await
        {
            let ns = namespace.to_utf8_path();
            tracing::info!(namespace = %ns, track = %track_name, source = "local", "serving subscribe from local: {:?}", local.reader.info);
            timing_guard.set_label("source", "local");
            let _track_guard = GaugeGuard::new("moq_relay_active_tracks");
            // Held until serving finishes. Once the last guard for a cached track
            // drops, its upstream subscription becomes eligible for release.
            let _interest_guard = local.interest;

            // Draft-16 §8.4: a relay MUST have an Established upstream
            // subscription before it sends SUBSCRIBE_OK. A pull-through cache
            // entry exists before its upstream subscription does, so wait for it.
            if let Some(upstream) = local.upstream {
                if let Err(outcome) = Self::await_upstream(&subscribed, &upstream).await {
                    // Which side ended the wait is decided by the branch that
                    // resolved, not by the error variant: an upstream failure can
                    // itself be Cancel or Done, so sniffing the variant would
                    // report a publisher-side failure as a subscriber cancellation
                    // and skip the upstream-error counter.
                    let err = match outcome {
                        UpstreamWait::DownstreamLeft(err) => {
                            tracing::debug!(namespace = %ns, track = %track_name, error = %err, "downstream subscriber left before the upstream subscription was established");
                            timing_guard.set_label("source", "downstream_left");
                            err
                        }
                        UpstreamWait::UpstreamFailed(err) => {
                            tracing::warn!(namespace = %ns, track = %track_name, error = %err, "upstream subscription could not be established");
                            metrics::counter!("moq_relay_subscribe_upstream_errors_total")
                                .increment(1);
                            timing_guard.set_label("source", "upstream_error");
                            err
                        }
                    };

                    // Rejects when the subscription is already closed (the
                    // downstream-left case), which is fine: the error below is
                    // still the reason we stopped.
                    let _ = subscribed.close(err.clone());
                    return Err(err.into());
                }
            }

            return Ok(subscribed.serve(local.reader).await?);
        }

        // Check remote tracks after local exact tracks and namespace route sources.
        match self
            .remotes
            .subscribe(self.context.scope(), &namespace, &track_name)
            .await
        {
            Ok(track) => {
                if let Some((track, interest_guard)) = track {
                    let ns = namespace.to_utf8_path();
                    tracing::info!(namespace = %ns, track = %track_name, source = "remote", "serving subscribe from remote: {:?}", track.info);
                    // Update label to indicate remote source, timing recorded on drop
                    timing_guard.set_label("source", "remote");
                    // Track active tracks - decrements when serve completes
                    let _track_guard = GaugeGuard::new("moq_relay_active_tracks");
                    // Held until serving finishes; the cross-relay subscription is
                    // released once the last guard for this track drops.
                    let _interest_guard = interest_guard;
                    return Ok(subscribed.serve(track).await?);
                }
            }
            Err(e) => {
                // Route error = infrastructure failure (couldn't reach coordinator/upstream)
                // This is different from "not found" - we don't know if the track exists
                let ns = namespace.to_utf8_path();
                tracing::error!(namespace = %ns, track = %track_name, error = %e, "failed to route to remote: {}", e);
                timing_guard.set_label("source", "route_error");
                metrics::counter!("moq_relay_subscribe_route_errors_total").increment(1);

                // Return an internal error rather than "not found" since we couldn't check
                // TODO: Consider returning a more specific error to the subscriber
                let err = ServeError::internal_ctx(format!(
                    "route error for namespace '{}': {}",
                    namespace, e
                ));
                subscribed.close(err.clone())?;
                return Err(err.into());
            }
        }

        // Track not found - we checked all sources and the track doesn't exist
        // timing_guard label already set to "not_found", will record on drop
        metrics::counter!("moq_relay_subscribe_not_found_total").increment(1);

        let err = ServeError::not_found_ctx(format!(
            "track '{}/{}' not found in local or remote tracks",
            namespace, track_name
        ));
        subscribed.close(err.clone())?;
        Err(err.into())
    }

    /// Wait for the upstream subscription behind a cached track to be established.
    ///
    /// Also completes when the downstream subscriber goes away first, so a
    /// cancelled SUBSCRIBE is not held here for the full upstream response
    /// timeout. The two cases are reported separately because they cannot be
    /// told apart from the error alone — see [`UpstreamWait`].
    async fn await_upstream(
        subscribed: &Subscribed,
        upstream: &UpstreamReady,
    ) -> Result<(), UpstreamWait> {
        tokio::select! {
            res = upstream.established() => res.map_err(UpstreamWait::UpstreamFailed),
            res = subscribed.closed() => Err(UpstreamWait::DownstreamLeft(
                res.err().unwrap_or(ServeError::Done),
            )),
        }
    }

    /// Serve a SUBSCRIBE_NAMESPACE request using relay-local namespace state.
    async fn serve_subscribe_namespace(
        self,
        mut subscribed_namespace: SubscribedNamespace,
    ) -> Result<(), anyhow::Error> {
        let wants_namespace = wants_namespace(subscribed_namespace.subscribe_options);
        let wants_publish = wants_publish(subscribed_namespace.subscribe_options);
        let namespace_changes = self.locals.subscribe_namespace_changes();
        let track_changes = self.locals.subscribe_track_changes();
        let mut publish_tasks: FuturesUnordered<futures::future::BoxFuture<'static, ()>> =
            FuturesUnordered::new();

        let _upstream_lease = if wants_namespace {
            match self
                .upstream_namespaces
                .subscribe(&self.context, subscribed_namespace.namespace_prefix.clone())
            {
                Ok(lease) => Some(lease),
                Err(error) => {
                    tracing::error!(
                        prefix = %subscribed_namespace.namespace_prefix,
                        error = %error,
                        "failed to acquire shared upstream namespace lease; serving local state only"
                    );
                    None
                }
            }
        } else {
            None
        };

        subscribed_namespace.ok()?;

        let mut known_namespaces = HashSet::new();

        if wants_namespace {
            self.send_namespace_snapshot(&mut subscribed_namespace, &mut known_namespaces)?;
        }

        let mut known_tracks = HashSet::new();
        if wants_publish {
            self.send_publish_snapshot(
                &subscribed_namespace,
                &mut known_tracks,
                &mut publish_tasks,
            )
            .await?;
        }

        self.serve_subscribe_namespace_loop(
            subscribed_namespace,
            wants_namespace,
            wants_publish,
            namespace_changes,
            track_changes,
            publish_tasks,
            known_namespaces,
            known_tracks,
        )
        .await
    }

    #[allow(clippy::too_many_arguments)]
    async fn serve_subscribe_namespace_loop(
        self,
        subscribed_namespace: SubscribedNamespace,
        wants_namespace: bool,
        wants_publish: bool,
        mut namespace_changes: tokio::sync::broadcast::Receiver<NamespaceChange>,
        mut track_changes: tokio::sync::broadcast::Receiver<TrackChange>,
        mut publish_tasks: FuturesUnordered<futures::future::BoxFuture<'static, ()>>,
        mut known_namespaces: HashSet<TrackNamespace>,
        mut known_tracks: HashSet<FullTrackName>,
    ) -> Result<(), anyhow::Error> {
        let mut subscribed_namespace = subscribed_namespace;
        loop {
            tokio::select! {
                res = subscribed_namespace.closed() => {
                    res?;
                    return Ok(());
                }
                change = namespace_changes.recv(), if wants_namespace => {
                    match change {
                        Ok(change) => {
                            self.apply_namespace_change(&mut subscribed_namespace, &mut known_namespaces, change)?;
                        }
                        Err(broadcast::error::RecvError::Lagged(skipped)) => {
                            // Recoverable: a full resync reconstructs the state the
                            // skipped events would have produced. Counted so that
                            // sustained churn outgrowing the channel capacity is
                            // visible before it shows up as latency.
                            metrics::counter!("moq_relay_change_channel_lagged_total", "channel" => "namespace")
                                .increment(skipped);
                            self.resync_namespaces(&mut subscribed_namespace, &mut known_namespaces)?;
                        }
                        Err(broadcast::error::RecvError::Closed) => return Ok(()),
                    }
                }
                change = track_changes.recv(), if wants_publish => {
                    match change {
                        Ok(change) => {
                            self.apply_track_change(&subscribed_namespace, &mut known_tracks, &mut publish_tasks, change).await?;
                        }
                        Err(broadcast::error::RecvError::Lagged(skipped)) => {
                            metrics::counter!("moq_relay_change_channel_lagged_total", "channel" => "track")
                                .increment(skipped);
                            self.resync_publish_tracks(&subscribed_namespace, &mut known_tracks, &mut publish_tasks).await?;
                        }
                        Err(broadcast::error::RecvError::Closed) => return Ok(()),
                    }
                }
                _ = publish_tasks.next(), if !publish_tasks.is_empty() => {},
            }
        }
    }

    fn send_namespace_snapshot(
        &self,
        subscribed_namespace: &mut SubscribedNamespace,
        known: &mut HashSet<TrackNamespace>,
    ) -> Result<(), ServeError> {
        for namespace in self
            .locals
            .list_namespaces_matching(self.context.scope(), &subscribed_namespace.namespace_prefix)
        {
            if known.insert(namespace.clone()) {
                subscribed_namespace.namespace(&namespace)?;
            }
        }

        Ok(())
    }

    fn apply_namespace_change(
        &self,
        subscribed_namespace: &mut SubscribedNamespace,
        known: &mut HashSet<TrackNamespace>,
        change: NamespaceChange,
    ) -> Result<(), ServeError> {
        if change.scope.as_deref() != self.context.scope() {
            return Ok(());
        }

        if !subscribed_namespace
            .namespace_prefix
            .is_prefix_of(&change.namespace)
        {
            return Ok(());
        }

        if change.added {
            if known.insert(change.namespace.clone()) {
                subscribed_namespace.namespace(&change.namespace)?;
            }
        } else if known.remove(&change.namespace) {
            subscribed_namespace.namespace_done(&change.namespace)?;
        }

        Ok(())
    }

    fn resync_namespaces(
        &self,
        subscribed_namespace: &mut SubscribedNamespace,
        known: &mut HashSet<TrackNamespace>,
    ) -> Result<(), ServeError> {
        let current: HashSet<_> = self
            .locals
            .list_namespaces_matching(self.context.scope(), &subscribed_namespace.namespace_prefix)
            .into_iter()
            .collect();

        for namespace in current.difference(known) {
            subscribed_namespace.namespace(namespace)?;
        }

        for namespace in known.difference(&current) {
            subscribed_namespace.namespace_done(namespace)?;
        }

        *known = current;
        Ok(())
    }

    async fn send_publish_snapshot(
        &self,
        subscribed_namespace: &SubscribedNamespace,
        known: &mut HashSet<FullTrackName>,
        publish_tasks: &mut FuturesUnordered<futures::future::BoxFuture<'static, ()>>,
    ) -> Result<(), anyhow::Error> {
        for track in self
            .locals
            .list_tracks_matching(self.context.scope(), &subscribed_namespace.namespace_prefix)
        {
            self.publish_track_for_namespace(subscribed_namespace, known, publish_tasks, track)
                .await?;
        }

        Ok(())
    }

    async fn apply_track_change(
        &self,
        subscribed_namespace: &SubscribedNamespace,
        known: &mut HashSet<FullTrackName>,
        publish_tasks: &mut FuturesUnordered<futures::future::BoxFuture<'static, ()>>,
        change: TrackChange,
    ) -> Result<(), anyhow::Error> {
        match change {
            TrackChange::Added { scope, track } => {
                if scope.as_deref() != self.context.scope()
                    || !subscribed_namespace
                        .namespace_prefix
                        .is_prefix_of(&track.namespace)
                {
                    return Ok(());
                }

                self.publish_track_for_namespace(subscribed_namespace, known, publish_tasks, track)
                    .await
            }
            TrackChange::Removed { scope, full_name } => {
                if scope.as_deref() == self.context.scope() {
                    known.remove(&full_name);
                }
                Ok(())
            }
        }
    }

    async fn resync_publish_tracks(
        &self,
        subscribed_namespace: &SubscribedNamespace,
        known: &mut HashSet<FullTrackName>,
        publish_tasks: &mut FuturesUnordered<futures::future::BoxFuture<'static, ()>>,
    ) -> Result<(), anyhow::Error> {
        // Single pass: build only the `current` set while publishing new tracks,
        // instead of materializing an intermediate Vec of (name, reader) pairs.
        let mut current = HashSet::new();
        for track in self
            .locals
            .list_tracks_matching(self.context.scope(), &subscribed_namespace.namespace_prefix)
        {
            let full_name = full_name_for_track(&track);
            if !known.contains(&full_name) {
                self.publish_track_for_namespace(subscribed_namespace, known, publish_tasks, track)
                    .await?;
            }
            current.insert(full_name);
        }

        known.retain(|full_name| current.contains(full_name));
        Ok(())
    }

    async fn publish_track_for_namespace(
        &self,
        subscribed_namespace: &SubscribedNamespace,
        known: &mut HashSet<FullTrackName>,
        publish_tasks: &mut FuturesUnordered<futures::future::BoxFuture<'static, ()>>,
        track: TrackReader,
    ) -> Result<(), anyhow::Error> {
        let full_name = full_name_for_track(&track);
        if known.contains(&full_name) {
            return Ok(());
        }

        let mut params = KeyValuePairs::default();
        if !subscribed_namespace.forward {
            params.set_forward(false);
        }

        let namespace = full_name.namespace.to_utf8_path();
        let track_name = full_name.name.to_string();
        let mut publisher = self.publisher.clone();
        let published = match publisher.publish(track, params).await {
            Ok(published) => published,
            Err(SessionError::Serve(ServeError::Duplicate)) => return Ok(()),
            Err(err) => return Err(err.into()),
        };
        known.insert(full_name);
        publish_tasks.push(
            async move {
                if let Err(err) = published.serve().await {
                    tracing::warn!(namespace = %namespace, track = %track_name, error = %err, "failed serving PUBLISH for SUBSCRIBE_NAMESPACE");
                }
            }
            .boxed(),
        );

        Ok(())
    }

    fn is_expected_serve_shutdown(err: &anyhow::Error) -> bool {
        let serve = match err.downcast_ref::<SessionError>() {
            Some(SessionError::Serve(err)) => Some(err),
            _ => err.downcast_ref::<ServeError>(),
        };

        serve.is_some_and(Self::is_expected_serve_shutdown_err)
    }

    /// True for the errors that mean nobody is waiting for the subscription any
    /// more, rather than a failure worth warning about.
    fn is_expected_serve_shutdown_err(err: &ServeError) -> bool {
        matches!(err, ServeError::Cancel | ServeError::Done)
    }

    /// Serve a track_status request.
    async fn serve_track_status(
        self,
        mut track_status_requested: TrackStatusRequested,
    ) -> Result<(), anyhow::Error> {
        let full_name = FullTrackName {
            namespace: track_status_requested.request_msg.track_namespace.clone(),
            name: track_status_requested.request_msg.track_name.clone(),
        };

        // Check actual local tracks first.
        if let Some(track) = self.locals.retrieve_track(self.context.scope(), &full_name) {
            let namespace = full_name.namespace.to_utf8_path();
            let track_name = &full_name.name;
            tracing::info!(namespace = %namespace, track = %track_name, source = "local", "serving track_status from local: {:?}", track.info);
            return Ok(track_status_requested.respond_ok(&track)?);
        }

        // TODO - forward track status to remotes?
        // Check remote tracks second, and serve from remote if possible
        /*
        if let Some(remotes) = &self.remotes {
            // Try to route to a remote for this namespace
            if let Some(remote) = remotes.route(&subscribe.track_namespace).await? {
                if let Some(track) =
                    remote.subscribe(subscribe.track_namespace.clone(), subscribe.track_name.clone())?
                {
                    tracing::info!("serving from remote: {:?} {:?}", remote.info, track.info);

                    // NOTE: Depends on drop(track) being called afterwards
                    return Ok(subscribe.serve(track.reader).await?);
                }
            }
        }*/

        track_status_requested.respond_error(
            moq_transport::message::RequestErrorCode::DoesNotExist as u64,
            "track not found",
        )?;

        Err(ServeError::not_found_ctx(format!(
            "track '{}/{}' not found for track_status",
            track_status_requested.request_msg.track_namespace,
            track_status_requested.request_msg.track_name
        ))
        .into())
    }
}

fn wants_namespace(options: SubscribeOptions) -> bool {
    matches!(
        options,
        SubscribeOptions::Namespace | SubscribeOptions::Both
    )
}

fn wants_publish(options: SubscribeOptions) -> bool {
    matches!(options, SubscribeOptions::Publish | SubscribeOptions::Both)
}

fn full_name_for_track(track: &TrackReader) -> FullTrackName {
    FullTrackName {
        namespace: track.namespace.clone(),
        name: track.name.clone(),
    }
}

fn upstream_fetch_params(
    params: &KeyValuePairs,
) -> Result<KeyValuePairs, moq_transport::coding::DecodeError> {
    let mut upstream = KeyValuePairs::default();
    if let Some(priority) = params.subscriber_priority()? {
        upstream.set_subscriber_priority(priority);
    }
    if let Some(order) = params.group_order()? {
        upstream.set_group_order(order);
    }
    Ok(upstream)
}

#[cfg(test)]
mod tests {
    use std::collections::HashSet;
    use std::io::Cursor;
    use std::sync::Arc;
    use std::time::Duration;

    use async_trait::async_trait;
    use moq_native_ietf::quic;
    use moq_transport::{
        coding::{Decode, DecodeError, Encode, KeyValuePairs, Location, TrackNamespace},
        data::{FetchHeader, StreamHeader, StreamHeaderType},
        message::{self, parameter_type, FetchType, GroupOrder, Message, RequestErrorCode},
        serve::ServeError,
        session::{Session, SessionError},
        setup,
    };

    use crate::{
        Consumer, Coordinator, CoordinatorContext, CoordinatorError, CoordinatorResult, Locals,
        NamespaceOrigin, NamespaceRegistration, RemoteManager, SessionContext,
    };

    use super::{upstream_fetch_params, Producer};

    struct NoopCoordinator;

    #[async_trait]
    impl Coordinator for NoopCoordinator {
        async fn register_namespace(
            &self,
            _scope: Option<&str>,
            _namespace: &TrackNamespace,
            _context: &CoordinatorContext,
        ) -> CoordinatorResult<NamespaceRegistration> {
            Ok(NamespaceRegistration::new(()))
        }

        async fn unregister_namespace(
            &self,
            _scope: Option<&str>,
            _namespace: &TrackNamespace,
        ) -> CoordinatorResult<()> {
            Ok(())
        }

        async fn lookup(
            &self,
            _scope: Option<&str>,
            _namespace: &TrackNamespace,
        ) -> CoordinatorResult<(NamespaceOrigin, Option<quic::Client>)> {
            Err(CoordinatorError::NamespaceNotFound)
        }
    }

    struct WireReader {
        stream: web_transport::RecvStream,
        buffer: Vec<u8>,
    }

    impl WireReader {
        fn new(stream: web_transport::RecvStream) -> Self {
            Self {
                stream,
                buffer: Vec::new(),
            }
        }

        async fn decode<T: Decode>(&mut self) -> T {
            loop {
                let mut cursor = Cursor::new(self.buffer.as_slice());
                match T::decode(&mut cursor) {
                    Ok(value) => {
                        self.buffer.drain(..cursor.position() as usize);
                        return value;
                    }
                    Err(DecodeError::More(_)) => {
                        let chunk = self.stream.read(64 * 1024).await.unwrap().unwrap();
                        self.buffer.extend_from_slice(&chunk);
                    }
                    Err(err) => panic!("failed to decode test wire message: {err}"),
                }
            }
        }

        async fn read_to_end(mut self) -> Vec<u8> {
            while let Some(chunk) = self.stream.read(64 * 1024).await.unwrap() {
                self.buffer.extend_from_slice(&chunk);
            }
            self.buffer
        }
    }

    async fn write<T: Encode>(stream: &mut web_transport::SendStream, value: &T) {
        let mut encoded = Vec::new();
        value.encode(&mut encoded).unwrap();
        write_bytes(stream, &encoded).await;
    }

    async fn write_bytes(stream: &mut web_transport::SendStream, mut bytes: &[u8]) {
        while !bytes.is_empty() {
            let written = stream.write(bytes).await.unwrap();
            assert_ne!(written, 0);
            bytes = &bytes[written..];
        }
    }

    struct ManualPeer {
        transport: web_transport::Session,
        control_send: web_transport::SendStream,
        control_recv: WireReader,
        server_session: Session,
        server_publisher: moq_transport::session::Publisher,
        server_subscriber: moq_transport::session::Subscriber,
        _client: quic::Client,
        _server: quic::Server,
    }

    async fn manual_peer() -> ManualPeer {
        tokio::time::timeout(Duration::from_secs(5), manual_peer_inner())
            .await
            .unwrap()
    }

    async fn manual_peer_inner() -> ManualPeer {
        let _ = rustls::crypto::ring::default_provider().install_default();
        let certified = rcgen::generate_simple_self_signed(vec!["localhost".to_string()]).unwrap();
        let certificate = certified.cert.der().clone();
        let key = rustls::pki_types::PrivateKeyDer::Pkcs8(
            rustls::pki_types::PrivatePkcs8KeyDer::from(certified.key_pair.serialize_der()),
        );
        let provider = Arc::new(rustls::crypto::ring::default_provider());
        let server_tls = rustls::ServerConfig::builder_with_provider(provider.clone())
            .with_protocol_versions(&[&rustls::version::TLS13])
            .unwrap()
            .with_no_client_auth()
            .with_single_cert(vec![certificate.clone()], key)
            .unwrap();
        let mut roots = rustls::RootCertStore::empty();
        roots.add(certificate).unwrap();
        let client_tls = rustls::ClientConfig::builder_with_provider(provider)
            .with_protocol_versions(&[&rustls::version::TLS13])
            .unwrap()
            .with_root_certificates(roots)
            .with_no_client_auth();
        let tls = moq_native_ietf::tls::Config {
            client: client_tls,
            server: Some(server_tls),
            fingerprints: Vec::new(),
        };
        let endpoint = quic::Endpoint::new(
            quic::Config::new("0.0.0.0:0".parse().unwrap(), None, tls).unwrap(),
        )
        .unwrap();
        let quic_client = endpoint.client;
        let mut server = endpoint.server.unwrap();
        let port = server.local_addr().unwrap().port();
        let addr = format!("127.0.0.1:{port}").parse().unwrap();
        let url = url::Url::parse(&format!("moqt://localhost:{port}/")).unwrap();
        let (client_connection, server_connection) =
            tokio::join!(quic_client.connect(&url, Some(addr)), server.accept());
        let (client_transport, _, client_kind) = client_connection.unwrap();
        let (server_transport, server_info) = server_connection.unwrap();
        let manual_setup = async {
            let (mut send, recv) = client_transport.open_bi().await.unwrap();
            let mut params = KeyValuePairs::default();
            params.set_intvalue(setup::ParameterType::MaxRequestId.into(), 100);
            write(&mut send, &setup::Client { params }).await;
            let mut recv = WireReader::new(recv);
            let _: setup::Server = recv.decode().await;
            (client_transport, send, recv)
        };
        let (manual, server_parts) = tokio::try_join!(
            async { Ok::<_, SessionError>(manual_setup.await) },
            Session::accept(server_transport, None, server_info.transport),
        )
        .unwrap();
        assert_eq!(client_kind, moq_transport::session::Transport::RawQuic);
        let (transport, control_send, control_recv) = manual;
        let (server_session, server_publisher, server_subscriber) = server_parts;
        ManualPeer {
            transport,
            control_send,
            control_recv,
            server_session,
            server_publisher: server_publisher.unwrap(),
            server_subscriber: server_subscriber.unwrap(),
            _client: quic_client,
            _server: server,
        }
    }

    fn fetch_request(id: u64, namespace: &TrackNamespace) -> Message {
        message::Fetch {
            id,
            fetch_type: FetchType::Standalone,
            standalone_fetch: Some(message::StandaloneFetch {
                track_namespace: namespace.clone(),
                track_name: "video".into(),
                start_location: Location::new(0, 0),
                end_location: Location::new(1, 0),
            }),
            joining_fetch: None,
            params: KeyValuePairs::default(),
        }
        .into()
    }

    #[tokio::test]
    async fn local_namespace_fetch_passthrough_is_fresh_and_bidirectional() {
        let mut downstream = manual_peer().await;
        let mut upstream = manual_peer().await;
        let coordinator: Arc<dyn Coordinator> = Arc::new(NoopCoordinator);
        let locals = Locals::new();
        let remotes = RemoteManager::new(coordinator.clone(), Vec::new());
        let producer = Producer::new(
            downstream.server_publisher,
            locals.clone(),
            remotes.clone(),
            coordinator.clone(),
            SessionContext::public(None),
        );
        let consumer = Consumer::new(
            upstream.server_subscriber,
            locals.clone(),
            coordinator,
            remotes,
            None,
            SessionContext::public(None),
        );
        let namespace = TrackNamespace::from_utf8_path("test/fetch");

        let scenario = async {
            write(
                &mut upstream.control_send,
                &Message::PublishNamespace(message::PublishNamespace {
                    id: 0,
                    track_namespace: namespace.clone(),
                    params: KeyValuePairs::default(),
                }),
            )
            .await;
            assert!(matches!(
                upstream.control_recv.decode::<Message>().await,
                Message::RequestOk(message::RequestOk { id: 0, .. })
            ));

            let body = b"opaque fetch body";
            write(&mut downstream.control_send, &fetch_request(0, &namespace)).await;
            let Message::Fetch(first) = upstream.control_recv.decode::<Message>().await else {
                panic!("expected upstream FETCH");
            };
            let mut stream = upstream.transport.open_uni().await.unwrap();
            write(
                &mut stream,
                &FetchHeader {
                    header_type: StreamHeaderType::Fetch,
                    request_id: first.id,
                },
            )
            .await;
            write_bytes(&mut stream, body).await;
            stream.finish().unwrap();
            write(
                &mut upstream.control_send,
                &Message::FetchOk(message::FetchOk {
                    id: first.id,
                    end_of_track: true,
                    end_location: Location::new(1, 0),
                    params: KeyValuePairs::default(),
                    track_extensions: Default::default(),
                }),
            )
            .await;

            let stream = downstream.transport.accept_uni().await.unwrap();
            let mut stream = WireReader::new(stream);
            let header: StreamHeader = stream.decode().await;
            assert_eq!(header.fetch_header.unwrap().request_id, 0);
            assert_eq!(stream.read_to_end().await, body);
            assert!(matches!(
                downstream.control_recv.decode::<Message>().await,
                Message::FetchOk(message::FetchOk {
                    id: 0,
                    end_of_track: true,
                    ..
                })
            ));

            let mut upstream_ids = HashSet::from([first.id]);
            for (id, reason) in [(2, "first miss"), (4, "second miss")] {
                write(&mut downstream.control_send, &fetch_request(id, &namespace)).await;
                let Message::Fetch(request) = upstream.control_recv.decode::<Message>().await
                else {
                    panic!("expected upstream FETCH");
                };
                assert!(upstream_ids.insert(request.id));
                write(
                    &mut upstream.control_send,
                    &Message::RequestError(message::RequestError::new(
                        request.id,
                        RequestErrorCode::DoesNotExist,
                        0,
                        reason,
                    )),
                )
                .await;
                let Message::RequestError(error) =
                    downstream.control_recv.decode::<Message>().await
                else {
                    panic!("expected downstream REQUEST_ERROR");
                };
                assert_eq!(error.id, id);
                assert_eq!(error.error_code, RequestErrorCode::DoesNotExist as u64);
                assert_eq!(error.reason.0, reason);
            }

            write(&mut downstream.control_send, &fetch_request(6, &namespace)).await;
            let Message::Fetch(request) = upstream.control_recv.decode::<Message>().await else {
                panic!("expected reset upstream FETCH");
            };
            assert!(upstream_ids.insert(request.id));
            let mut stream = upstream.transport.open_uni().await.unwrap();
            write(
                &mut stream,
                &FetchHeader {
                    header_type: StreamHeaderType::Fetch,
                    request_id: request.id,
                },
            )
            .await;
            write_bytes(&mut stream, b"x").await;
            tokio::time::sleep(Duration::from_millis(20)).await;
            stream.reset(moq_transport::data::DataStreamResetCode::InternalError.into());
            tokio::time::sleep(Duration::from_millis(20)).await;
            write(
                &mut upstream.control_send,
                &Message::RequestError(message::RequestError::new(
                    request.id,
                    RequestErrorCode::DoesNotExist,
                    42,
                    "reset miss",
                )),
            )
            .await;
            let Message::RequestError(error) = downstream.control_recv.decode::<Message>().await
            else {
                panic!("expected reset REQUEST_ERROR");
            };
            assert_eq!(error.id, 6);
            assert_eq!(error.retry_interval, 42);
            assert_eq!(error.reason.0, "reset miss");

            write(&mut downstream.control_send, &fetch_request(8, &namespace)).await;
            let Message::Fetch(request) = upstream.control_recv.decode::<Message>().await else {
                panic!("expected cancellable upstream FETCH");
            };
            assert!(upstream_ids.insert(request.id));
            write(
                &mut downstream.control_send,
                &Message::FetchCancel(message::FetchCancel { id: 8 }),
            )
            .await;
            assert!(matches!(
                upstream.control_recv.decode::<Message>().await,
                Message::FetchCancel(message::FetchCancel { id }) if id == request.id
            ));
        };

        tokio::time::timeout(Duration::from_secs(5), async {
            tokio::select! {
                _ = scenario => {},
                result = producer.run() => panic!("producer ended: {result:?}"),
                result = consumer.run() => panic!("consumer ended: {result:?}"),
                result = downstream.server_session.run() => panic!("downstream server ended: {result:?}"),
                result = upstream.server_session.run() => panic!("upstream server ended: {result:?}"),
            }
        })
        .await
        .unwrap();
    }

    #[test]
    fn fetch_params_keep_delivery_policy_without_forwarding_credentials() {
        let mut params = KeyValuePairs::default();
        params.set_bytesvalue(parameter_type::AUTHORIZATION_TOKEN, b"private".to_vec());
        params.set_subscriber_priority(7);
        params.set_group_order(GroupOrder::Descending);
        params.set_intvalue(0x40, 9);

        let upstream = upstream_fetch_params(&params).unwrap();

        assert_eq!(upstream.subscriber_priority().unwrap(), Some(7));
        assert_eq!(
            upstream.group_order().unwrap(),
            Some(GroupOrder::Descending)
        );
        assert!(upstream.get(parameter_type::AUTHORIZATION_TOKEN).is_none());
        assert!(upstream.get(0x40).is_none());
    }

    #[test]
    fn expected_serve_shutdown_accepts_wrapped_session_errors() {
        assert!(Producer::is_expected_serve_shutdown(&anyhow::Error::new(
            SessionError::Serve(ServeError::Cancel)
        )));
        assert!(Producer::is_expected_serve_shutdown(&anyhow::Error::new(
            SessionError::Serve(ServeError::Done)
        )));
        assert!(!Producer::is_expected_serve_shutdown(&anyhow::Error::new(
            SessionError::Serve(ServeError::NotFound)
        )));
    }

    #[test]
    fn expected_serve_shutdown_accepts_direct_serve_errors() {
        assert!(Producer::is_expected_serve_shutdown(&anyhow::Error::new(
            ServeError::Cancel
        )));
        assert!(Producer::is_expected_serve_shutdown(&anyhow::Error::new(
            ServeError::Done
        )));
        assert!(!Producer::is_expected_serve_shutdown(&anyhow::Error::new(
            ServeError::NotFound
        )));
    }
}
