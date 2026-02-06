use crate::{coding, serve, setup};

#[derive(thiserror::Error, Debug, Clone)]
pub enum SessionError {
    #[error("webtransport session: {0}")]
    Session(#[from] web_transport::SessionError),

    #[error("webtransport write: {0}")]
    Write(#[from] web_transport::WriteError),

    #[error("webtransport read: {0}")]
    Read(#[from] web_transport::ReadError),

    #[error("encode error: {0}")]
    Encode(#[from] coding::EncodeError),

    #[error("decode error: {0}")]
    Decode(#[from] coding::DecodeError),

    // TODO move to a ConnectError
    #[error("unsupported versions: client={0:?} server={1:?}")]
    Version(setup::Versions, setup::Versions),

    /// TODO SLG - eventually remove or morph into error for incorrect control message for publisher/subscriber
    /// The role negiotiated in the handshake was violated. For example, a publisher sent a SUBSCRIBE, or a subscriber sent an OBJECT.
    #[error("role violation")]
    RoleViolation,

    /// Some VarInt was too large and we were too lazy to handle it
    #[error("varint bounds exceeded")]
    BoundsExceeded(#[from] coding::BoundsExceeded),

    /// A duplicate ID was used
    #[error("duplicate")]
    Duplicate,

    #[error("internal error")]
    Internal,

    #[error("serve error: {0}")]
    Serve(#[from] serve::ServeError),

    #[error("wrong size")]
    WrongSize,
}

// Session Termination Error Codes from draft-ietf-moq-transport-14 Section 13.1.1
impl SessionError {
    /// An integer code that is sent over the wire.
    /// Returns Session Termination Error Codes per draft-14.
    pub fn code(&self) -> u64 {
        match self {
            // PROTOCOL_VIOLATION (0x3) - The role negotiated in the handshake was violated
            Self::RoleViolation => 0x3,
            // INTERNAL_ERROR (0x1) - Generic internal errors
            Self::Session(_) => 0x1,
            Self::Read(_) => 0x1,
            Self::Write(_) => 0x1,
            Self::Encode(_) => 0x1,
            Self::BoundsExceeded(_) => 0x1,
            Self::Internal => 0x1,
            // VERSION_NEGOTIATION_FAILED (0x15)
            Self::Version(..) => 0x15,
            // PROTOCOL_VIOLATION (0x3) - Malformed messages
            Self::Decode(_) => 0x3,
            Self::WrongSize => 0x3,
            // DUPLICATE_TRACK_ALIAS (0x5)
            Self::Duplicate => 0x5,
            // Delegate to ServeError for per-request error codes
            Self::Serve(err) => err.code(),
        }
    }

    /// Helper for unimplemented protocol features
    /// Logs a warning and returns a NotImplemented error instead of panicking
    pub fn unimplemented(feature: &str) -> Self {
        Self::Serve(serve::ServeError::not_implemented_ctx(feature))
    }

    /// Returns true if this error represents a graceful connection close.
    ///
    /// A graceful close occurs when the peer sends APPLICATION_CLOSE with error code 0
    /// (NO_ERROR). This is normal session termination, not an error condition.
    ///
    /// This method checks for:
    /// - Quinn's `ApplicationClosed` with code 0
    /// - The local side closing the connection (`LocallyClosed`)
    ///
    /// Note: This uses string matching on the error message because the underlying
    /// quinn types are not directly accessible without adding quinn as a dependency.
    /// This approach is transport-agnostic and will work with future WebTransport
    /// implementations (e.g., tokio-quiche) as long as they use similar error messages.
    pub fn is_graceful_close(&self) -> bool {
        match self {
            Self::Session(session_err) => {
                let err_str = session_err.to_string();
                // Quinn's ApplicationClosed with code 0: "closed by peer: 0" or "closed by peer: reason (code 0)"
                // Quinn's LocallyClosed: "closed"
                if err_str == "connection error: closed" {
                    return true;
                }
                if err_str.starts_with("connection error: closed by peer:") {
                    // Check if error code is 0
                    // Format: "closed by peer: 0" or "closed by peer: reason (code 0)"
                    if err_str.ends_with(": 0") || err_str.ends_with("(code 0)") {
                        return true;
                    }
                }
                false
            }
            // Read/Write errors during graceful shutdown can also indicate clean close
            Self::Read(read_err) => {
                let err_str = read_err.to_string();
                // Stream closed during graceful shutdown
                err_str.contains("closed by peer: 0")
                    || err_str.contains("(code 0)")
                    || err_str == "session error: connection error: closed"
            }
            Self::Write(write_err) => {
                let err_str = write_err.to_string();
                err_str.contains("closed by peer: 0")
                    || err_str.contains("(code 0)")
                    || err_str == "session error: connection error: closed"
            }
            _ => false,
        }
    }
}

impl From<SessionError> for serve::ServeError {
    fn from(err: SessionError) -> Self {
        match err {
            SessionError::Serve(err) => err,
            _ => serve::ServeError::internal_ctx(format!("session error: {}", err)),
        }
    }
}
