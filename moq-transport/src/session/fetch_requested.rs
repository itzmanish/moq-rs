// SPDX-FileCopyrightText: 2026 Cloudflare Inc.
// SPDX-License-Identifier: MIT OR Apache-2.0

use std::{
    collections::HashMap,
    sync::{Arc, Mutex},
    time::Duration,
};

use crate::{
    coding::{KeyValuePairs, Location, VarInt},
    data::{DataStreamResetCode, FetchHeader, StreamHeaderType},
    message::{self, Message, RequestErrorCode},
    serve::ServeError,
    watch::{Queue, State},
};

use super::{Fetch, SessionError, SessionId, Writer};

const COPY_CHUNK_SIZE: usize = 64 * 1024;

struct FetchRequestedState {
    closed: Result<(), ServeError>,
}

impl Default for FetchRequestedState {
    fn default() -> Self {
        Self { closed: Ok(()) }
    }
}

/// An inbound standalone FETCH waiting for application routing.
#[must_use = "proxy, reject, or drop the FETCH request"]
pub struct FetchRequested {
    webtransport: Option<web_transport::Session>,
    session_id: SessionId,
    outgoing: Queue<Message>,
    active: Arc<Mutex<HashMap<u64, FetchRequestedRecv>>>,
    state: State<FetchRequestedState>,
    id: u64,
    received_at: tokio::time::Instant,
    pub request: message::Fetch,
}

pub(crate) struct FetchRequestedRecv {
    state: State<FetchRequestedState>,
}

impl FetchRequested {
    pub(super) fn new(
        webtransport: Option<web_transport::Session>,
        session_id: SessionId,
        outgoing: Queue<Message>,
        active: Arc<Mutex<HashMap<u64, FetchRequestedRecv>>>,
        request: message::Fetch,
    ) -> (Self, FetchRequestedRecv) {
        let id = request.id;
        let (send, recv) = State::default().split();
        (
            Self {
                webtransport,
                session_id,
                outgoing,
                active,
                state: send,
                id,
                received_at: tokio::time::Instant::now(),
                request,
            },
            FetchRequestedRecv { state: recv },
        )
    }

    pub async fn closed(&self) -> Result<(), ServeError> {
        loop {
            let notify = {
                let state = self.state.lock();
                state.closed.clone()?;
                state.modified()
            };
            match notify {
                Some(notify) => notify.await,
                None => return Ok(()),
            }
        }
    }

    pub fn reject(
        self,
        code: RequestErrorCode,
        reason: impl Into<String>,
    ) -> Result<(), ServeError> {
        self.claim_response()?;
        self.send_error(code, reason);
        self.remove_active();
        Ok(())
    }

    pub fn deadline(&self, timeout: Duration) -> tokio::time::Instant {
        self.received_at + timeout
    }

    pub async fn proxy(self, mut upstream: Fetch, timeout: Duration) -> Result<(), SessionError> {
        let deadline = tokio::time::Instant::now() + timeout;
        let result = {
            let operation = self.proxy_inner(&mut upstream);
            tokio::pin!(operation);
            tokio::select! {
                biased;
                closed = self.closed() => {
                    return Err(closed.err().unwrap_or(ServeError::Done).into());
                },
                _ = tokio::time::sleep_until(deadline) => None,
                result = &mut operation => Some(result),
            }
        };

        match result {
            Some(Ok(response)) => self.respond_ok(response),
            None => {
                self.reject(RequestErrorCode::Timeout, "fetch proxy timed out")?;
                Err(ServeError::Cancel.into())
            }
            Some(Err(err)) => {
                if upstream.request_error().is_none() {
                    tokio::select! {
                        closed = self.closed() => {
                            return Err(closed.err().unwrap_or(ServeError::Done).into());
                        },
                        _ = tokio::time::sleep_until(deadline) => {},
                        _ = upstream.ok() => {},
                    }
                }
                if let Some(error) = upstream.request_error() {
                    let request_id = self.id;
                    self.respond_error(proxied_error(error, request_id))?;
                    return Err(err);
                }
                self.reject(RequestErrorCode::InternalError, "fetch proxy failed")?;
                Err(err)
            }
        }
    }

    async fn proxy_inner(&self, upstream: &mut Fetch) -> Result<message::FetchOk, SessionError> {
        let webtransport = self.webtransport.as_ref().ok_or(SessionError::Internal)?;
        let mut stream = FetchStream::new(Writer::new(
            self.session_id.clone(),
            webtransport.open_uni().await?,
        ));
        stream
            .writer
            .encode(&FetchHeader {
                header_type: StreamHeaderType::Fetch,
                request_id: self.id,
            })
            .await?;

        while let Some(chunk) = upstream.read_stream_chunk(COPY_CHUNK_SIZE).await? {
            stream.writer.write(&chunk).await?;
        }

        stream.finish()?;
        let response = upstream.ok().await?;
        Ok(proxied_response(response, self.id))
    }

    fn respond_ok(self, response: message::FetchOk) -> Result<(), SessionError> {
        self.claim_response()?;
        let _ = self.outgoing.clone().push(response.into());
        self.remove_active();
        Ok(())
    }

    fn respond_error(self, response: message::RequestError) -> Result<(), SessionError> {
        self.claim_response()?;
        let _ = self.outgoing.clone().push(response.into());
        self.remove_active();
        Ok(())
    }

    fn claim_response(&self) -> Result<(), ServeError> {
        let state = self.state.lock();
        state.closed.clone()?;
        let mut state = state.into_mut().ok_or(ServeError::Done)?;
        state.closed = Err(ServeError::Done);
        Ok(())
    }

    fn send_error(&self, code: RequestErrorCode, reason: impl Into<String>) {
        let reason = reason.into();
        let _ = self
            .outgoing
            .clone()
            .push(message::RequestError::new(self.id, code, 0, &reason).into());
    }

    fn remove_active(&self) {
        if let Ok(mut active) = self.active.lock() {
            active.remove(&self.id);
        }
    }
}

impl Drop for FetchRequested {
    fn drop(&mut self) {
        if self.claim_response().is_ok() {
            self.send_error(RequestErrorCode::InternalError, "fetch request dropped");
        }
        self.remove_active();
    }
}

impl FetchRequestedRecv {
    pub fn cancel(&mut self) -> Result<(), ServeError> {
        let state = self.state.lock();
        if state.closed.is_err() {
            return Ok(());
        }
        let mut state = state.into_mut().ok_or(ServeError::Done)?;
        state.closed = Err(ServeError::Cancel);
        Ok(())
    }
}

pub(super) fn inclusive_end(end: Location) -> Location {
    if end.object_id == 0 {
        Location::new(end.group_id, VarInt::MAX.into_inner())
    } else {
        Location::new(end.group_id, end.object_id - 1)
    }
}

pub(super) fn fetch_end_in_range(start: Location, end: Location, response: Location) -> bool {
    (response == start || inclusive_end(response) >= start)
        && inclusive_end(response) <= inclusive_end(end)
}

fn proxied_response(mut response: message::FetchOk, request_id: u64) -> message::FetchOk {
    response.id = request_id;
    response.params = KeyValuePairs::default();
    response
}

fn proxied_error(mut response: message::RequestError, request_id: u64) -> message::RequestError {
    response.id = request_id;
    response
}

struct FetchStream {
    writer: Writer,
    finished: bool,
}

impl FetchStream {
    fn new(writer: Writer) -> Self {
        Self {
            writer,
            finished: false,
        }
    }

    fn finish(&mut self) -> Result<(), SessionError> {
        self.writer.finish()?;
        self.finished = true;
        Ok(())
    }
}

impl Drop for FetchStream {
    fn drop(&mut self) {
        if !self.finished {
            self.writer.reset(DataStreamResetCode::InternalError.into());
        }
    }
}

#[cfg(test)]
mod tests {
    use crate::{
        coding::{Location, TrackNamespace},
        message::{Fetch, FetchType, StandaloneFetch},
    };

    use super::*;

    fn request(id: u64) -> Fetch {
        Fetch {
            id,
            fetch_type: FetchType::Standalone,
            standalone_fetch: Some(StandaloneFetch {
                track_namespace: TrackNamespace::from_utf8_path("test"),
                track_name: "video".into(),
                start_location: Location::new(0, 0),
                end_location: Location::new(1, 0),
            }),
            joining_fetch: None,
            params: Default::default(),
        }
    }

    struct Handles {
        request: FetchRequested,
        recv: FetchRequestedRecv,
        _keepalive: Queue<Message>,
        outgoing: Queue<Message>,
        active: Arc<Mutex<HashMap<u64, FetchRequestedRecv>>>,
    }

    fn handles(id: u64) -> Handles {
        let (outgoing, receiver) = Queue::default().split();
        let keepalive = outgoing.clone();
        let active = Arc::new(Mutex::new(HashMap::new()));
        let (request, recv) = FetchRequested::new(
            None,
            SessionId::generate(),
            outgoing,
            active.clone(),
            request(id),
        );
        Handles {
            request,
            recv,
            _keepalive: keepalive,
            outgoing: receiver,
            active,
        }
    }

    async fn webtransport_pair() -> (
        web_transport::Session,
        web_transport::Session,
        web_transport::quinn::Client,
        web_transport::quinn::quinn::Endpoint,
    ) {
        tokio::time::timeout(Duration::from_secs(5), async {
            let _ = rustls::crypto::ring::default_provider().install_default();
            let certified =
                rcgen::generate_simple_self_signed(vec!["localhost".to_string()]).unwrap();
            let certificate = certified.cert.der().clone();
            let key = rustls::pki_types::PrivateKeyDer::Pkcs8(
                rustls::pki_types::PrivatePkcs8KeyDer::from(certified.key_pair.serialize_der()),
            );
            let mut server = web_transport::quinn::ServerBuilder::new()
                .with_addr("0.0.0.0:0".parse().unwrap())
                .with_certificate(vec![certificate.clone()], key)
                .unwrap();
            let port = server.local_addr().unwrap().port();
            let client = web_transport::quinn::ClientBuilder::new()
                .with_server_certificates(vec![certificate])
                .unwrap();
            let client_keepalive = client.clone();
            let server_keepalive = (*server).clone();
            let url = url::Url::parse(&format!("https://127.0.0.1:{port}/")).unwrap();
            let (client_session, server_session) = tokio::join!(client.connect(url), async move {
                server.accept().await.unwrap().ok().await
            });
            (
                client_session.unwrap().into(),
                server_session.unwrap().into(),
                client_keepalive,
                server_keepalive,
            )
        })
        .await
        .unwrap()
    }

    #[tokio::test]
    async fn reject_sends_one_error_and_removes_active_state() {
        let Handles {
            request,
            recv,
            _keepalive,
            mut outgoing,
            active,
        } = handles(7);
        active.lock().unwrap().insert(7, recv);

        request
            .reject(RequestErrorCode::NotSupported, "not supported")
            .unwrap();

        let Message::RequestError(error) = outgoing.pop().await.unwrap() else {
            panic!("expected REQUEST_ERROR");
        };
        assert_eq!(error.id, 7);
        assert_eq!(error.error_code, RequestErrorCode::NotSupported as u64);
        assert!(active.lock().unwrap().is_empty());
        assert!(outgoing.close().is_empty());
    }

    #[tokio::test]
    async fn cancellation_wakes_request_and_suppresses_drop_error() {
        let Handles {
            request,
            mut recv,
            _keepalive,
            outgoing,
            active,
        } = handles(9);
        active.lock().unwrap().insert(
            9,
            FetchRequestedRecv {
                state: recv.state.clone(),
            },
        );

        recv.cancel().unwrap();
        assert!(matches!(request.closed().await, Err(ServeError::Cancel)));
        drop(request);

        assert!(active.lock().unwrap().is_empty());
        assert!(outgoing.close().is_empty());
    }

    #[tokio::test]
    async fn dropping_unanswered_request_sends_internal_error() {
        let Handles {
            mut request,
            recv,
            _keepalive,
            mut outgoing,
            active,
        } = handles(11);
        active.lock().unwrap().insert(11, recv);
        request.request.id = 99;

        drop(request);

        let Message::RequestError(error) = outgoing.pop().await.unwrap() else {
            panic!("expected REQUEST_ERROR");
        };
        assert_eq!(error.id, 11);
        assert_eq!(error.error_code, RequestErrorCode::InternalError as u64);
        assert!(active.lock().unwrap().is_empty());
    }

    #[test]
    fn deadline_starts_when_request_is_received() {
        let before = tokio::time::Instant::now();
        let handles = handles(13);
        let after = tokio::time::Instant::now();

        let deadline = handles.request.deadline(Duration::from_secs(30));

        assert!(deadline >= before + Duration::from_secs(30));
        assert!(deadline <= after + Duration::from_secs(30));
    }

    #[test]
    fn proxied_terminal_messages_remap_only_request_id() {
        let mut ok = message::FetchOk {
            id: 1,
            end_of_track: true,
            end_location: Location::new(4, 8),
            params: KeyValuePairs::default(),
            track_extensions: Default::default(),
        };
        ok.params.set_intvalue(2, 7);
        ok.track_extensions.set_delivery_timeout(10);
        let mapped = proxied_response(ok.clone(), 99);
        assert_eq!(mapped.id, 99);
        assert_eq!(mapped.end_of_track, ok.end_of_track);
        assert_eq!(mapped.end_location, ok.end_location);
        assert_eq!(mapped.track_extensions, ok.track_extensions);
        assert!(mapped.params.0.is_empty());

        let error = message::RequestError::new(1, RequestErrorCode::DoesNotExist, 42, "not here");
        let mapped = proxied_error(error.clone(), 99);
        assert_eq!(mapped.id, 99);
        assert_eq!(mapped.error_code, error.error_code);
        assert_eq!(mapped.retry_interval, error.retry_interval);
        assert_eq!(mapped.reason, error.reason);
    }

    #[tokio::test]
    async fn proxy_waits_for_request_error_after_stream_reset() {
        let (upstream_receive, upstream_send, _upstream_client, _upstream_server) =
            webtransport_pair().await;
        let _upstream_keepalive = upstream_send.clone();
        let subscriber = super::super::Subscriber::new(
            Queue::default(),
            Queue::default(),
            None,
            super::super::RequestId::new(0, 100, 100, 0),
            super::super::PendingRequests::default(),
            super::super::SessionId::generate(),
        );
        let (upstream, mut upstream_recv) = super::super::Fetch::new(subscriber, request(64));
        let (accepted, wait_accepted) = tokio::sync::oneshot::channel();
        let send_reset = async move {
            let mut stream = upstream_send.open_uni().await.unwrap();
            stream.write(b"x").await.unwrap();
            wait_accepted.await.unwrap();
            stream.reset(DataStreamResetCode::InternalError.into());
        };
        let receive_reset = async {
            let stream = upstream_receive.accept_uni().await.unwrap();
            accepted.send(()).unwrap();
            upstream_recv
                .recv_stream(super::super::Reader::new(SessionId::generate(), stream))
                .unwrap();
        };
        tokio::join!(send_reset, receive_reset);

        let (downstream_send, _downstream_receive, _downstream_client, _downstream_server) =
            webtransport_pair().await;
        let _downstream_keepalive = downstream_send.clone();
        let Handles {
            mut request,
            recv,
            _keepalive,
            mut outgoing,
            active,
        } = handles(7);
        active.lock().unwrap().insert(7, recv);
        request.webtransport = Some(downstream_send);
        let expected =
            message::RequestError::new(64, RequestErrorCode::DoesNotExist, 42, "origin failed");
        let deliver_error = async {
            tokio::time::sleep(Duration::from_millis(20)).await;
            upstream_recv.recv_error(&expected).unwrap();
        };

        let (result, ()) = tokio::join!(
            request.proxy(upstream, Duration::from_secs(5)),
            deliver_error
        );

        assert!(result.is_err());
        let Message::RequestError(error) = outgoing.pop().await.unwrap() else {
            panic!("expected REQUEST_ERROR");
        };
        assert_eq!(error.id, 7);
        assert_eq!(error.error_code, RequestErrorCode::DoesNotExist as u64);
        assert_eq!(error.retry_interval, 42);
        assert_eq!(error.reason.0, "origin failed");
        assert!(active.lock().unwrap().is_empty());
    }
}
