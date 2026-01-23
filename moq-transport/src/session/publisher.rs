use std::{
    collections::{hash_map, HashMap},
    sync::{atomic, Arc, Mutex},
};

use futures::{stream::FuturesUnordered, StreamExt};

use crate::{
    coding::TrackNamespace,
    message::{self, GroupOrder, Message},
    mlog,
    serve::{self, ServeError, TracksReader},
};

use crate::watch::Queue;

use super::{
    PublishNamespace, PublishNamespaceRecv, Published, PublishedRecv, Session, SessionError,
    Subscribed, SubscribedRecv, TrackStatusRequested,
};

#[derive(Clone)]
pub struct Publisher {
    webtransport: web_transport::Session,

    publish_namespaces: Arc<Mutex<HashMap<TrackNamespace, PublishNamespaceRecv>>>,

    subscribeds: Arc<Mutex<HashMap<u64, SubscribedRecv>>>,

    unknown_subscribed: Queue<Subscribed>,

    unknown_track_status_requested: Queue<TrackStatusRequested>,

    publisheds: Arc<Mutex<HashMap<u64, PublishedRecv>>>,

    next_track_alias: Arc<atomic::AtomicU64>,

    outgoing: Queue<Message>,

    next_requestid: Arc<atomic::AtomicU64>,

    mlog: Option<Arc<Mutex<mlog::MlogWriter>>>,
}

impl Publisher {
    pub(crate) fn new(
        outgoing: Queue<Message>,
        webtransport: web_transport::Session,
        next_requestid: Arc<atomic::AtomicU64>,
        mlog: Option<Arc<Mutex<mlog::MlogWriter>>>,
    ) -> Self {
        Self {
            webtransport,
            publish_namespaces: Default::default(),
            subscribeds: Default::default(),
            unknown_subscribed: Default::default(),
            unknown_track_status_requested: Default::default(),
            publisheds: Default::default(),
            next_track_alias: Arc::new(atomic::AtomicU64::new(1)),
            outgoing,
            next_requestid,
            mlog,
        }
    }

    pub async fn accept(
        session: web_transport::Session,
    ) -> Result<(Session, Publisher), SessionError> {
        let (session, publisher, _) = Session::accept(session, None).await?;
        Ok((session, publisher.unwrap()))
    }

    pub async fn connect(
        session: web_transport::Session,
    ) -> Result<(Session, Publisher), SessionError> {
        let (session, publisher, _) = Session::connect(session, None).await?;
        Ok((session, publisher))
    }

    pub async fn publish_namespace(&mut self, tracks: TracksReader) -> Result<(), SessionError> {
        let publish_ns = match self
            .publish_namespaces
            .lock()
            .unwrap()
            .entry(tracks.namespace.clone())
        {
            hash_map::Entry::Occupied(_) => return Err(ServeError::Duplicate.into()),

            hash_map::Entry::Vacant(entry) => {
                let request_id = self.next_requestid.fetch_add(2, atomic::Ordering::Relaxed);

                let (send, recv) =
                    PublishNamespace::new(self.clone(), request_id, tracks.namespace.clone());
                entry.insert(recv);
                send
            }
        };

        let mut subscribe_tasks = FuturesUnordered::new();
        let mut status_tasks = FuturesUnordered::new();
        let mut subscribe_done = false;
        let mut status_done = false;

        loop {
            tokio::select! {
                res = publish_ns.subscribed(), if !subscribe_done => {
                    match res? {
                        Some(subscribed) => {
                            let tracks = tracks.clone();

                            subscribe_tasks.push(async move {
                                let info = subscribed.info.clone();
                                if let Err(err) = Self::serve_subscribe(subscribed, tracks).await {
                                    log::warn!("failed serving subscribe: {:?}, error: {}", info, err)
                                }
                            });
                        },
                        None => subscribe_done = true,
                    }

                },
                res = publish_ns.track_status_requested(), if !status_done => {
                    match res? {
                        Some(status) => {
                            let tracks = tracks.clone();

                            status_tasks.push(async move {
                                let request_msg = status.request_msg.clone();
                                if let Err(err) = Self::serve_track_status(status, tracks).await {
                                    log::warn!("failed serving track status request: {:?}, error: {}", request_msg, err)
                                }
                            });
                        },
                        None => status_done = true,
                    }
                },
                Some(res) = subscribe_tasks.next() => res,
                Some(res) = status_tasks.next() => res,
                else => return Ok(())
            }
        }
    }

    pub async fn serve_subscribe(
        subscribed: Subscribed,
        mut tracks: TracksReader,
    ) -> Result<(), SessionError> {
        if let Some(track) = tracks.subscribe(
            subscribed.info.track_namespace.clone(),
            &subscribed.info.track_name,
        ) {
            subscribed.serve(track).await?;
        } else {
            let namespace = subscribed.info.track_namespace.clone();
            let name = subscribed.info.track_name.clone();
            subscribed.close(ServeError::not_found_ctx(format!(
                "track '{}/{}' not found in tracks",
                namespace, name
            )))?;
        }

        Ok(())
    }

    pub async fn serve_track_status(
        track_status_request: TrackStatusRequested,
        mut tracks: TracksReader,
    ) -> Result<(), SessionError> {
        let track = tracks
            .subscribe(
                track_status_request.request_msg.track_namespace.clone(),
                &track_status_request.request_msg.track_name,
            )
            .ok_or_else(|| {
                ServeError::not_found_ctx(format!(
                    "track '{}/{}' not found for track_status",
                    track_status_request.request_msg.track_namespace,
                    track_status_request.request_msg.track_name
                ))
            })?;

        track_status_request.respond_ok(&track)?;

        Ok(())
    }

    // Returns subscriptions that do not map to an active announce.
    pub async fn subscribed(&mut self) -> Option<Subscribed> {
        self.unknown_subscribed.pop().await
    }

    pub async fn track_status_requested(&mut self) -> Option<TrackStatusRequested> {
        self.unknown_track_status_requested.pop().await
    }

    pub async fn publish(&mut self, track: serve::TrackReader) -> Result<Published, SessionError> {
        let request_id = self.next_requestid.fetch_add(2, atomic::Ordering::Relaxed);
        let track_alias = self.next_track_alias.fetch_add(1, atomic::Ordering::Relaxed);

        let largest_location = track.largest_location();
        let content_exists = largest_location.is_some();

        let msg = message::Publish {
            id: request_id,
            track_namespace: track.namespace.clone(),
            track_name: track.name.clone(),
            track_alias,
            group_order: GroupOrder::Ascending,
            content_exists,
            largest_location,
            forward: true,
            params: Default::default(),
        };

        let (send, recv) = Published::new(self.clone(), msg, self.mlog.clone());

        self.publisheds.lock().unwrap().insert(request_id, recv);

        Ok(send)
    }

    pub async fn publish_with_options(
        &mut self,
        track: serve::TrackReader,
        group_order: GroupOrder,
        forward: bool,
    ) -> Result<Published, SessionError> {
        let request_id = self.next_requestid.fetch_add(2, atomic::Ordering::Relaxed);
        let track_alias = self.next_track_alias.fetch_add(1, atomic::Ordering::Relaxed);

        let largest_location = track.largest_location();
        let content_exists = largest_location.is_some();

        let msg = message::Publish {
            id: request_id,
            track_namespace: track.namespace.clone(),
            track_name: track.name.clone(),
            track_alias,
            group_order,
            content_exists,
            largest_location,
            forward,
            params: Default::default(),
        };

        let (send, recv) = Published::new(self.clone(), msg, self.mlog.clone());

        self.publisheds.lock().unwrap().insert(request_id, recv);

        Ok(send)
    }

    pub(crate) fn recv_message(&mut self, msg: message::Subscriber) -> Result<(), SessionError> {
        let res = match msg {
            message::Subscriber::Subscribe(msg) => self.recv_subscribe(msg),
            message::Subscriber::SubscribeUpdate(msg) => self.recv_subscribe_update(msg),
            message::Subscriber::Unsubscribe(msg) => self.recv_unsubscribe(msg),
            message::Subscriber::Fetch(_msg) => Err(SessionError::unimplemented("FETCH")),
            message::Subscriber::FetchCancel(_msg) => {
                Err(SessionError::unimplemented("FETCH_CANCEL"))
            }
            message::Subscriber::TrackStatus(msg) => self.recv_track_status(msg),
            message::Subscriber::SubscribeNamespace(_msg) => {
                Err(SessionError::unimplemented("SUBSCRIBE_NAMESPACE"))
            }
            message::Subscriber::UnsubscribeNamespace(_msg) => {
                Err(SessionError::unimplemented("UNSUBSCRIBE_NAMESPACE"))
            }
            message::Subscriber::PublishNamespaceCancel(msg) => {
                self.recv_publish_namespace_cancel(msg)
            }
            message::Subscriber::PublishNamespaceOk(msg) => self.recv_publish_namespace_ok(msg),
            message::Subscriber::PublishNamespaceError(msg) => {
                self.recv_publish_namespace_error(msg)
            }
            message::Subscriber::PublishOk(msg) => self.recv_publish_ok(msg),
            message::Subscriber::PublishError(msg) => self.recv_publish_error(msg)
        };

        if let Err(err) = res {
            log::warn!("failed to process message: {}", err);
        }

        Ok(())
    }

    fn recv_publish_namespace_ok(
        &mut self,
        msg: message::PublishNamespaceOk,
    ) -> Result<(), SessionError> {
        let mut publish_namespaces = self.publish_namespaces.lock().unwrap();
        let entry = publish_namespaces
            .iter_mut()
            .find(|(_k, v)| v.request_id == msg.id);

        if let Some(entry) = entry {
            entry.1.recv_ok()?;
        }

        Ok(())
    }

    fn recv_publish_namespace_error(
        &mut self,
        msg: message::PublishNamespaceError,
    ) -> Result<(), SessionError> {
        let mut publish_namespaces = self.publish_namespaces.lock().unwrap();

        let key_opt = publish_namespaces
            .iter()
            .find(|(_k, v)| v.request_id == msg.id)
            .map(|(k, _)| k.clone());

        if let Some(key) = key_opt {
            if let Some((_ns, v)) = publish_namespaces.remove_entry(&key) {
                v.recv_error(ServeError::Closed(msg.error_code))?;
            }
        }

        Ok(())
    }

    fn recv_publish_namespace_cancel(
        &mut self,
        msg: message::PublishNamespaceCancel,
    ) -> Result<(), SessionError> {
        if let Some(entry) = self
            .publish_namespaces
            .lock()
            .unwrap()
            .remove(&msg.track_namespace)
        {
            entry.recv_error(ServeError::Cancel)?;
        }

        Ok(())
    }

    fn recv_publish_ok(&mut self, msg: message::PublishOk) -> Result<(), SessionError> {
        if let Some(published) = self.publisheds.lock().unwrap().get_mut(&msg.id) {
            published.recv_ok(&msg)?;
        }

        Ok(())
    }

    fn recv_publish_error(&mut self, msg: message::PublishError) -> Result<(), SessionError> {
        if let Some(published) = self.publisheds.lock().unwrap().remove(&msg.id) {
            published.recv_error(ServeError::Closed(msg.error_code))?;
        }

        Ok(())
    }

    fn recv_subscribe(&mut self, msg: message::Subscribe) -> Result<(), SessionError> {
        let namespace = msg.track_namespace.clone();

        let subscribed = {
            let mut subscribeds = self.subscribeds.lock().unwrap();

            // See if entry exists for this request id already, if so error out
            let entry = match subscribeds.entry(msg.id) {
                hash_map::Entry::Occupied(_) => return Err(SessionError::Duplicate),
                hash_map::Entry::Vacant(entry) => entry,
            };

            // Create new Subscribed entry and add to HashMap
            let (send, recv) = Subscribed::new(self.clone(), msg, self.mlog.clone());
            entry.insert(recv);

            send
        };

        if let Some(publish_ns) = self.publish_namespaces.lock().unwrap().get_mut(&namespace) {
            return publish_ns.recv_subscribe(subscribed).map_err(Into::into);
        }
        if let Err(err) = self.unknown_subscribed.push(subscribed) {
            // Default to closing with a not found error I guess.
            err.close(ServeError::not_found_ctx(format!(
                "unknown_subscribed queue full for namespace {:?}",
                namespace
            )))?;
        }

        Ok(())
    }

    fn recv_subscribe_update(
        &mut self,
        _msg: message::SubscribeUpdate,
    ) -> Result<(), SessionError> {
        // TODO: Implement updating subscriptions.
        Err(SessionError::unimplemented("SUBSCRIBE_UPDATE"))
    }

    fn recv_track_status(&mut self, msg: message::TrackStatus) -> Result<(), SessionError> {
        let namespace = msg.track_namespace.clone();

        // Create TrackStatusRequested to track this request
        let track_status_requested = TrackStatusRequested::new(self.clone(), msg);

        if let Some(publish_ns) = self.publish_namespaces.lock().unwrap().get_mut(&namespace) {
            return publish_ns
                .recv_track_status_requested(track_status_requested)
                .map_err(Into::into);
        }
        if let Err(mut err) = self
            .unknown_track_status_requested
            .push(track_status_requested)
        {
            // push only fails if the queue is dropped, send  TrackStatusError, Internal error
            err.respond_error(0, "Internal error")?;
        }

        Ok(())
    }

    fn recv_unsubscribe(&mut self, msg: message::Unsubscribe) -> Result<(), SessionError> {
        if let Some(subscribed) = self.subscribeds.lock().unwrap().get_mut(&msg.id) {
            subscribed.recv_unsubscribe()?;
        }

        Ok(())
    }

    fn act_on_message_to_send<T: Into<message::Publisher>>(
        &mut self,
        msg: T,
    ) -> message::Publisher {
        let msg = msg.into();
        match &msg {
            message::Publisher::PublishDone(m) => {
                self.drop_subscribe(m.id);
                self.drop_published(m.id);
            }
            message::Publisher::SubscribeError(m) => self.drop_subscribe(m.id),
            message::Publisher::PublishNamespaceDone(m) => {
                self.drop_publish_namespace(&m.track_namespace);
            }
            _ => {}
        }
        msg
    }

    /// Send a message without waiting for it to be sent.
    pub(super) fn send_message<T: Into<message::Publisher> + Into<Message>>(&mut self, msg: T) {
        let msg = self.act_on_message_to_send(msg);
        self.outgoing.push(msg.into()).ok();
    }

    /// Send a message and wait until it is sent (or at least popped off the outgoing control message queue)
    pub(super) async fn send_message_and_wait<T: Into<message::Publisher> + Into<Message>>(
        &mut self,
        msg: T,
    ) {
        let msg = self.act_on_message_to_send(msg);
        self.outgoing
            .push_and_wait_until_popped(msg.into())
            .await
            .ok();
    }

    fn drop_subscribe(&mut self, id: u64) {
        self.subscribeds.lock().unwrap().remove(&id);
    }

    fn drop_publish_namespace(&mut self, namespace: &TrackNamespace) {
        self.publish_namespaces.lock().unwrap().remove(namespace);
    }

    fn drop_published(&mut self, id: u64) {
        self.publisheds.lock().unwrap().remove(&id);
    }

    pub(super) async fn open_uni(&mut self) -> Result<web_transport::SendStream, SessionError> {
        Ok(self.webtransport.open_uni().await?)
    }

    pub(super) async fn send_datagram(&mut self, data: bytes::Bytes) -> Result<(), SessionError> {
        Ok(self.webtransport.send_datagram(data).await?)
    }
}
