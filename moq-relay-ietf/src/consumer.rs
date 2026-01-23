use std::sync::Arc;

use anyhow::Context;
use futures::{stream::FuturesUnordered, FutureExt, StreamExt};
use moq_transport::{
    serve::Tracks,
    session::{PublishNamespaceReceived, SessionError, Subscriber},
};

use crate::{Coordinator, Locals, Producer};

/// Consumer of tracks from a remote Publisher
#[derive(Clone)]
pub struct Consumer {
    subscriber: Subscriber,
    locals: Locals,
    coordinator: Arc<dyn Coordinator>,
    forward: Option<Producer>, // Forward all announcements to this subscriber
}

impl Consumer {
    pub fn new(
        subscriber: Subscriber,
        locals: Locals,
        coordinator: Arc<dyn Coordinator>,
        forward: Option<Producer>,
    ) -> Self {
        Self {
            subscriber,
            locals,
            coordinator,
            forward,
        }
    }

    /// Run the consumer to serve announce requests.
    pub async fn run(mut self) -> Result<(), SessionError> {
        let mut tasks = FuturesUnordered::new();

        loop {
            tokio::select! {
                Some(publish_ns) = self.subscriber.publish_ns_recvd() => {
                    let this = self.clone();

                    tasks.push(async move {
                        let info = publish_ns.clone();
                        log::info!("serving publish_namespace: {:?}", info);

                        if let Err(err) = this.serve(publish_ns).await {
                            log::warn!("failed serving publish_namespace: {:?}, error: {}", info, err)
                        }
                    });
                },
                _ = tasks.next(), if !tasks.is_empty() => {},
                else => return Ok(()),
            };
        }
    }

    async fn serve(
        mut self,
        mut publish_ns: PublishNamespaceReceived,
    ) -> Result<(), anyhow::Error> {
        let mut tasks = FuturesUnordered::new();

        let (_, mut request, reader) = Tracks::new(publish_ns.namespace.clone()).produce();

        // NOTE(mpandit): once the track is pulled from origin, internally it will be relayed
        // from this metal only, because now coordinator will have entry for the namespace.

        // should we allow the same namespace being served from multiple relays??

        // Register namespace with the coordinator
        let _namespace_registration = self
            .coordinator
            .register_namespace(&reader.namespace)
            .await?;

        // Register the local tracks, unregister on drop
        let _register = self.locals.register(reader.clone()).await?;

        publish_ns.ok()?;

        // Forward the announce, if needed
        if let Some(mut forward) = self.forward {
            tasks.push(
                async move {
                    log::info!("forwarding publish_namespace: {:?}", reader.info);
                    forward
                        .publish_namespace(reader)
                        .await
                        .context("failed forwarding publish_namespace")
                }
                .boxed(),
            );
        }

        // Serve subscribe requests
        loop {
            tokio::select! {
                Err(err) = publish_ns.closed() => return Err(err.into()),

                // Wait for the next subscriber and serve the track.
                Some(track) = request.next() => {
                    let mut subscriber = self.subscriber.clone();

                    // Spawn a new task to handle the subscribe
                    tasks.push(async move {
                        let info = track.clone();
                        log::info!("forwarding subscribe: {:?}", info);

                        // Forward the subscribe request
                        if let Err(err) = subscriber.subscribe(track).await {
                            log::warn!("failed forwarding subscribe: {:?}, error: {}", info, err)
                        }

                        Ok(())
                    }.boxed());
                },
                res = tasks.next(), if !tasks.is_empty() => res.unwrap()?,
                else => return Ok(()),
            }
        }
    }
}
