//! Frames del stream `/fhs/v1/0.1.0`: longitud varint sin signo + Envelope
//! (lo que escribe `encodeEnvelopeFrame` y lee `it-length-prefixed` en TS).

use futures::{AsyncRead, AsyncReadExt, AsyncWrite, AsyncWriteExt};
use prost::Message;

use crate::protocol::fhs::Envelope;
use crate::signing;

/// Tamaño máximo de un frame (adjuntos inline en base64 incluidos).
pub const MAX_FRAME_BYTES: usize = 64 * 1024 * 1024;

#[derive(Debug, thiserror::Error)]
pub enum FrameError {
    #[error("E/S del stream: {0}")]
    Io(#[from] std::io::Error),
    #[error("frame demasiado grande ({0} bytes)")]
    TooLarge(usize),
    #[error("varint de longitud inválido")]
    BadLength,
    #[error("Envelope inválido: {0}")]
    Decode(#[from] prost::DecodeError),
}

pub async fn write_envelope<W: AsyncWrite + Unpin>(
    writer: &mut W,
    envelope: &Envelope,
) -> Result<(), FrameError> {
    writer
        .write_all(&envelope.encode_length_delimited_to_vec())
        .await?;
    writer.flush().await?;
    Ok(())
}

async fn read_varint<R: AsyncRead + Unpin>(reader: &mut R) -> Result<Option<usize>, FrameError> {
    let mut value: u64 = 0;
    for shift in (0..64).step_by(7) {
        let mut byte = [0u8; 1];
        let n = reader.read(&mut byte).await?;
        if n == 0 {
            return if shift == 0 {
                Ok(None)
            } else {
                Err(FrameError::BadLength)
            };
        }
        value |= u64::from(byte[0] & 0x7f) << shift;
        if byte[0] & 0x80 == 0 {
            return usize::try_from(value)
                .map(Some)
                .map_err(|_| FrameError::BadLength);
        }
    }
    Err(FrameError::BadLength)
}

/// Siguiente frame: `None` al cerrarse el stream. Devuelve los bytes crudos
/// para verificar la firma sobre ellos.
pub async fn read_frame<R: AsyncRead + Unpin>(
    reader: &mut R,
) -> Result<Option<Vec<u8>>, FrameError> {
    let Some(len) = read_varint(reader).await? else {
        return Ok(None);
    };
    if len > MAX_FRAME_BYTES {
        return Err(FrameError::TooLarge(len));
    }
    let mut buf = vec![0u8; len];
    reader.read_exact(&mut buf).await?;
    Ok(Some(buf))
}

/// Siguiente Envelope con firma válida; los frames con firma inválida se
/// descartan con un aviso (como `decodeStream` en el TS).
pub async fn read_verified<R: AsyncRead + Unpin>(
    reader: &mut R,
) -> Result<Option<Envelope>, FrameError> {
    loop {
        let Some(bytes) = read_frame(reader).await? else {
            return Ok(None);
        };
        match signing::verify_envelope_bytes(&bytes)? {
            Some(envelope) => return Ok(Some(envelope)),
            None => tracing::warn!("frame FHS descartado: firma ausente o inválida"),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::p2p::{identity::NodeIdentity, wire};
    use crate::protocol::fhs::{envelope::Payload, ChatDeltaMessage};
    use futures::io::Cursor;

    #[tokio::test]
    async fn writes_and_reads_verified_frames() {
        let id = NodeIdentity::from_keypair(libp2p::identity::Keypair::generate_ed25519()).unwrap();
        let envelope = wire::sealed_envelope(
            &id,
            "",
            Payload::ChatDelta(ChatDeltaMessage {
                mission_id: "m".into(),
                delta: "hola".into(),
            }),
        );
        let mut buf = Vec::new();
        write_envelope(&mut buf, &envelope).await.unwrap();
        write_envelope(&mut buf, &envelope).await.unwrap();
        let mut reader = Cursor::new(buf);
        assert_eq!(
            read_verified(&mut reader)
                .await
                .unwrap()
                .unwrap()
                .message_id,
            envelope.message_id
        );
        assert!(read_verified(&mut reader).await.unwrap().is_some());
        assert!(read_verified(&mut reader).await.unwrap().is_none());
    }
}
