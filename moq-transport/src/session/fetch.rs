// SPDX-FileCopyrightText: 2026 Cloudflare Inc.
// SPDX-License-Identifier: MIT OR Apache-2.0

use crate::{
    message::{self, FetchOk},
    serve::ServeError,
    watch::State,
};

use super::{Reader, Subscriber};

struct FetchState {
    reader: Option<Reader>,
    ok: Option<FetchOk>,
    request_error: Option<message::RequestError>,
    closed: Result<(), ServeError>,
    stream_received: bool,
}

impl Default for FetchState {
    fn default() -> Self {
        Self {
            reader: None,
            ok: None,
            request_error: None,
            closed: Ok(()),
            stream_received: false,
        }
    }
}

/// An outbound standalone FETCH.
#[must_use = "dropping a FETCH sends FETCH_CANCEL"]
pub struct Fetch {
    subscriber: Subscriber,
    state: State<FetchState>,
    reader: Option<Reader>,
    stream_done: bool,
    id: u64,
    pub request: message::Fetch,
}

pub(crate) struct FetchRecv {
    state: State<FetchState>,
    pub start: crate::coding::Location,
}

impl Fetch {
    pub(super) fn new(subscriber: Subscriber, request: message::Fetch) -> (Self, FetchRecv) {
        let id = request.id;
        let start = request
            .standalone_fetch
            .as_ref()
            .map(|fetch| fetch.start_location)
            .unwrap_or_default();
        let (send, recv) = State::default().split();
        (
            Self {
                subscriber,
                state: send,
                reader: None,
                stream_done: false,
                id,
                request,
            },
            FetchRecv { state: recv, start },
        )
    }

    pub async fn ok(&self) -> Result<FetchOk, ServeError> {
        loop {
            let notify = {
                let state = self.state.lock();
                state.closed.clone()?;
                if let Some(ok) = &state.ok {
                    return Ok(ok.clone());
                }
                state.modified()
            };
            match notify {
                Some(notify) => notify.await,
                None => return Err(ServeError::Done),
            }
        }
    }

    pub fn request_error(&self) -> Option<message::RequestError> {
        self.state.lock().request_error.clone()
    }

    pub(super) async fn read_stream_chunk(
        &mut self,
        max: usize,
    ) -> Result<Option<bytes::Bytes>, super::SessionError> {
        self.ensure_reader().await?;
        let state = self.state.clone();
        let reader = self.reader.as_mut().ok_or(ServeError::Done)?;
        let chunk = tokio::select! {
            result = reader.read_chunk(max) => result?,
            err = wait_closed(state) => return Err(err.into()),
        };
        if chunk.is_none() {
            self.stream_done = true;
        }
        Ok(chunk)
    }

    async fn ensure_reader(&mut self) -> Result<(), ServeError> {
        while self.reader.is_none() {
            let notify = {
                let state = self.state.lock();
                state.closed.clone()?;
                if state.reader.is_some() {
                    self.reader = state.into_mut().and_then(|mut state| state.reader.take());
                    continue;
                }
                state.modified().ok_or(ServeError::Done)?
            };
            notify.await;
        }
        Ok(())
    }
}

impl Drop for Fetch {
    fn drop(&mut self) {
        let state = self.state.lock();
        if !(self.stream_done && state.ok.is_some()) && state.closed.is_ok() {
            self.subscriber
                .send_message(message::FetchCancel { id: self.id });
        }
        drop(state);
        self.subscriber.remove_fetch(self.id);
    }
}

impl FetchRecv {
    pub fn recv_ok(&mut self, ok: &FetchOk) -> Result<(), ServeError> {
        let mut state = self.state.lock_mut().ok_or(ServeError::Done)?;
        if state.ok.is_some() {
            return Err(ServeError::Duplicate);
        }
        state.ok = Some(ok.clone());
        Ok(())
    }

    pub fn recv_error(&mut self, error: &message::RequestError) -> Result<(), ServeError> {
        let Some(mut state) = self.state.lock_mut() else {
            return Ok(());
        };
        state.request_error = Some(error.clone());
        state.closed = Err(ServeError::Closed(error.error_code));
        Ok(())
    }

    pub fn recv_timeout(&mut self, err: ServeError) -> Result<(), ServeError> {
        let Some(mut state) = self.state.lock_mut() else {
            return Ok(());
        };
        state.closed = Err(err);
        Ok(())
    }

    pub fn recv_stream(&mut self, reader: Reader) -> Result<(), ServeError> {
        let mut state = self.state.lock_mut().ok_or(ServeError::Done)?;
        if state.stream_received {
            return Err(ServeError::Duplicate);
        }
        state.stream_received = true;
        state.reader = Some(reader);
        Ok(())
    }
}

async fn wait_closed(state: State<FetchState>) -> ServeError {
    loop {
        let notify = {
            let state = state.lock();
            if let Err(err) = &state.closed {
                return err.clone();
            }
            state.modified()
        };
        match notify {
            Some(notify) => notify.await,
            None => return ServeError::Done,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn request_error_after_fetch_drop_is_benign() {
        let (fetch, state) = State::<FetchState>::default().split();
        let mut recv = FetchRecv {
            state,
            start: Default::default(),
        };
        drop(fetch);

        let error =
            message::RequestError::new(0, message::RequestErrorCode::InternalError, 0, "failed");
        assert!(recv.recv_error(&error).is_ok());
    }

    #[test]
    fn timeout_after_fetch_drop_is_benign() {
        let (fetch, state) = State::<FetchState>::default().split();
        let mut recv = FetchRecv {
            state,
            start: Default::default(),
        };
        drop(fetch);

        assert!(recv.recv_timeout(ServeError::Cancel).is_ok());
    }
}
