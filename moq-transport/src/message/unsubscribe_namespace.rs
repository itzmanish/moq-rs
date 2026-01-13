use crate::coding::{Decode, DecodeError, Encode, EncodeError, TrackNamespace, VarInt};

/// Unsubscribe Namespace
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct UnsubscribeNamespace {
    // reqeust id of the subscribe_namespace being cancelled by this message
    pub request_id: VarInt,
}

impl Decode for UnsubscribeNamespace {
    fn decode<R: bytes::Buf>(r: &mut R) -> Result<Self, DecodeError> {
        let request_id = VarInt::decode(r)?;
        Ok(Self { request_id })
    }
}

impl Encode for UnsubscribeNamespace {
    fn encode<W: bytes::BufMut>(&self, w: &mut W) -> Result<(), EncodeError> {
        self.request_id.encode(w)?;
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use bytes::BytesMut;

    #[test]
    fn encode_decode() {
        let mut buf = BytesMut::new();

        let msg = UnsubscribeNamespace {
            request_id: 1u32.into(),
        };
        msg.encode(&mut buf).unwrap();
        let decoded = UnsubscribeNamespace::decode(&mut buf).unwrap();
        assert_eq!(decoded, msg);
    }
}
