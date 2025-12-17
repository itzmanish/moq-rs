use futures::{stream::FuturesUnordered, FutureExt, StreamExt};
use moq_transport::{
    coding::TrackNamespace,
    serve::{ServeError, TracksReader},
    session::{Publisher, SessionError, Subscribed, TrackStatusRequested},
};

use crate::{lingering_subscriber::LingeringSubscriptionStore, Locals, RemoteManager};

/// Producer of tracks to a remote Subscriber
#[derive(Clone)]
pub struct Producer {
    lingering_subs: LingeringSubscriptionStore,
    publisher: Publisher,
    locals: Locals,
    remotes: RemoteManager,
}

impl Producer {
    pub fn new(
        publisher: Publisher,
        locals: Locals,
        remotes: RemoteManager,
        lingering_subs: LingeringSubscriptionStore,
    ) -> Self {
        Self {
            lingering_subs,
            publisher,
            locals,
            remotes,
        }
    }

    /// Announce new tracks to the remote server.
    pub async fn announce(&mut self, tracks: TracksReader) -> Result<(), SessionError> {
        self.publisher.announce(tracks).await
    }

    /// Run the producer to serve subscribe requests.
    pub async fn run(self) -> Result<(), SessionError> {
        //let mut tasks = FuturesUnordered::new();
        let mut tasks: FuturesUnordered<futures::future::BoxFuture<'static, ()>> =
            FuturesUnordered::new();

        // FIXME(itzmanish): this is hardcoded for 10s interval, we should make it configurable
        let mut timer = tokio::time::interval(std::time::Duration::from_secs(10));
        loop {
            let mut publisher_subscribed = self.publisher.clone();
            let mut publisher_track_status = self.publisher.clone();

            tokio::select! {
                // Handle a new subscribe request
                Some(subscribed) = publisher_subscribed.subscribed() => {
                    let this = self.clone();

                    // Spawn a new task to handle the subscribe
                    tasks.push(async move {
                        let info = subscribed.info.clone();
                        log::info!("serving subscribe: {:?}", info);

                        // Serve the subscribe request
                        if let Err(err) = this.serve_subscribe(subscribed).await {
                            log::warn!("failed serving subscribe: {:?}, error: {}", info, err);
                        }
                    }.boxed())
                },
                // Handle a new track_status request
                Some(track_status_requested) = publisher_track_status.track_status_requested() => {
                    let this = self.clone();

                    // Spawn a new task to handle the track_status request
                    tasks.push(async move {
                        let info = track_status_requested.request_msg.clone();
                        log::info!("serving track_status: {:?}", info);

                        // Serve the track_status request
                        if let Err(err) = this.serve_track_status(track_status_requested).await {
                            log::warn!("failed serving track_status: {:?}, error: {}", info, err)
                        }
                    }.boxed())
                },
                _ = timer.tick() => {
                    // Check lingering subscriptions
                    let this = self.clone();
                    tasks.push(async move {
                        let _ = this.check_lingering_subscriptions().await;
                    }.boxed())
                }
                _= tasks.next(), if !tasks.is_empty() => {},
                else => return Ok(()),
            };
        }
    }

    async fn check_lingering_subscriptions(&self) -> Result<(), SessionError> {
        // Hold the lock for the entire iteration to avoid copying
        let mut by_track = self.lingering_subs.lock().await;

        // Collect keys to remove after processing (can't modify while iterating)
        let mut subscriptions_to_remove: Vec<((TrackNamespace, String), u64)> = Vec::new();

        for ((namespace, track_name), subscriptions) in by_track.iter() {
            if self.locals.retrieve(namespace).is_none() {
                continue;
            }

            for lingering_subscription in subscriptions.iter() {
                if lingering_subscription.is_expired() {
                    subscriptions_to_remove.push((
                        (namespace.clone(), track_name.clone()),
                        lingering_subscription.subscribed.id,
                    ));
                    continue;
                }

                if let Err(err) = self
                    .clone()
                    .serve_subscribe(lingering_subscription.subscribed.clone())
                    .await
                {
                    log::warn!(
                        "failed serving subscribe: {:?}, error: {}",
                        lingering_subscription.subscribed.info,
                        err
                    );
                } else {
                    // Successfully served, mark for removal
                    subscriptions_to_remove.push((
                        (namespace.clone(), track_name.clone()),
                        lingering_subscription.subscribed.id,
                    ));
                }
            }
        }

        // Remove processed subscriptions
        for ((namespace, track_name), subscription_id) in subscriptions_to_remove {
            let key = (namespace, track_name);
            if let Some(subscriptions) = by_track.get_mut(&key) {
                if let Some(pos) = subscriptions
                    .iter()
                    .position(|sub| sub.subscribed.id == subscription_id)
                {
                    subscriptions.remove(pos);
                }
            }
        }

        Ok(())
    }

    /// Serve a subscribe request.
    async fn serve_subscribe(self, subscribed: Subscribed) -> Result<(), anyhow::Error> {
        let namespace = subscribed.track_namespace.clone();
        let track_name = subscribed.track_name.clone();

        // Check local tracks first, and serve from local if possible
        if let Some(mut local) = self.locals.retrieve(&namespace) {
            // Pass the full requested namespace, not the announced prefix
            if let Some(track) = local.subscribe(namespace.clone(), &track_name) {
                log::info!("serving subscribe from local: {:?}", track.info);
                return Ok(subscribed.serve(track).await?);
            }
        }

        // Check remote tracks second, and serve from remote if possible
        if let Some(track) = self
            .remotes
            .subscribe(namespace.clone(), track_name.clone())
            .await?
        {
            log::info!("serving subscribe from remote: {:?}", track.info);
            return Ok(subscribed.serve(track).await?);
        }

        // NOTE(itzmanish): checking if we have subscription policy set for lingering
        // on the request or globally on the relay
        //
        if self.lingering_subs.lingering_allowed().await {
            if let Err(err) = self
                .lingering_subs
                .add_subscription(subscribed.clone())
                .await
            {
                log::info!("failed to add lingering subscription: {}", err);
            } else {
                log::info!("lingering subscription added");
                return Ok(());
            }
        }

        // Track not found - close the subscription with not found error
        let err = ServeError::not_found_ctx(format!(
            "track '{}/{}' not found in local or remote tracks",
            namespace, track_name
        ));
        subscribed.close(err.clone())?;
        Err(err.into())
    }

    /// Serve a track_status request.
    async fn serve_track_status(
        self,
        mut track_status_requested: TrackStatusRequested,
    ) -> Result<(), anyhow::Error> {
        // Check local tracks first, and serve from local if possible
        if let Some(mut local_tracks) = self
            .locals
            .retrieve(&track_status_requested.request_msg.track_namespace)
        {
            if let Some(track) = local_tracks.get_track_reader(
                &track_status_requested.request_msg.track_namespace,
                &track_status_requested.request_msg.track_name,
            ) {
                log::info!("serving track_status from local: {:?}", track.info);
                return Ok(track_status_requested.respond_ok(&track)?);
            }
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
                    log::info!("serving from remote: {:?} {:?}", remote.info, track.info);

                    // NOTE: Depends on drop(track) being called afterwards
                    return Ok(subscribe.serve(track.reader).await?);
                }
            }
        }*/

        track_status_requested.respond_error(4, "Track not found")?;

        Err(ServeError::not_found_ctx(format!(
            "track '{}/{}' not found for track_status",
            track_status_requested.request_msg.track_namespace,
            track_status_requested.request_msg.track_name
        ))
        .into())
    }
}
