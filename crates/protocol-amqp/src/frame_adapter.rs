//! Narrow wire adaptations for Service Bus interoperability.
//!
//! The adapter sits inside TLS and leaves protocol ownership with the AMQP
//! engine. It only rewrites outgoing delivery tags registered by the broker
//! and supplies the omitted final disposition range in settle mode Second.

use std::{
    collections::{BTreeSet, HashMap, VecDeque},
    io,
    sync::{Arc, Mutex, MutexGuard},
};

use amqp_runtime::types::{
    definitions::{DeliveryTag, ReceiverSettleMode, Role},
    performatives::{Disposition, Performative},
};
use tokio::{
    io::{AsyncRead, AsyncReadExt, AsyncWrite, AsyncWriteExt, DuplexStream},
    sync::mpsc,
};

const AMQP_PROTOCOL_ID: u8 = 0;
const AMQP_FRAME_TYPE: u8 = 0;
const FRAME_HEADER_BYTES: usize = 8;
const PROTOCOL_HEADER_BYTES: usize = 8;
const BRIDGE_CAPACITY: usize = 64 * 1024;
const MAX_WIRE_FRAME_BYTES: usize = 4 * 1024 * 1024;

type LinkKey = (u16, u32);

#[derive(Clone, Debug, Default)]
pub(crate) struct DeliveryTagRegistry {
    state: Arc<Mutex<AdapterState>>,
}

impl DeliveryTagRegistry {
    pub(crate) fn register(&self, link_name: &str, delivery_tag: DeliveryTag) {
        self.lock()
            .requested_tags
            .entry(link_name.to_owned())
            .or_default()
            .push_back(delivery_tag.to_vec());
    }

    fn lock(&self) -> MutexGuard<'_, AdapterState> {
        self.state
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
    }
}

#[derive(Clone, Debug)]
struct SenderLink {
    name: String,
    settle_second: bool,
}

#[derive(Debug, Default)]
struct AdapterState {
    requested_tags: HashMap<String, VecDeque<Vec<u8>>>,
    sender_links: HashMap<LinkKey, SenderLink>,
    settle_second_deliveries: HashMap<u16, BTreeSet<u32>>,
}

/// Bridges an already-secured connection through the frame adapter.
pub(crate) fn adapt_connection<Io>(stream: Io) -> (DuplexStream, DeliveryTagRegistry)
where
    Io: AsyncRead + AsyncWrite + std::fmt::Debug + Send + Unpin + 'static,
{
    let registry = DeliveryTagRegistry::default();
    let bridge_registry = registry.clone();
    let (protocol_stream, bridge_stream) = tokio::io::duplex(BRIDGE_CAPACITY);
    tokio::spawn(async move {
        if let Err(error) = run_bridge(bridge_stream, stream, bridge_registry).await {
            tracing::debug!(%error, "AMQP frame adapter ended");
        }
    });
    (protocol_stream, registry)
}

async fn run_bridge<Io>(
    protocol_stream: DuplexStream,
    network_stream: Io,
    registry: DeliveryTagRegistry,
) -> io::Result<()>
where
    Io: AsyncRead + AsyncWrite + Unpin,
{
    let (protocol_reader, protocol_writer) = tokio::io::split(protocol_stream);
    let (network_reader, network_writer) = tokio::io::split(network_stream);
    let (injected_tx, injected_rx) = mpsc::channel(16);

    tokio::try_join!(
        forward_incoming(
            network_reader,
            protocol_writer,
            injected_tx,
            registry.clone()
        ),
        forward_outgoing(protocol_reader, network_writer, injected_rx, registry),
    )?;
    Ok(())
}

async fn forward_incoming<R, W>(
    reader: R,
    mut writer: W,
    injected: mpsc::Sender<Vec<u8>>,
    registry: DeliveryTagRegistry,
) -> io::Result<()>
where
    R: AsyncRead + Unpin,
    W: AsyncWrite + Unpin,
{
    let mut wire = WireReader::new(reader);
    while let Some(unit) = wire.next().await? {
        if let WireUnit::Frame { protocol_id, bytes } = &unit
            && *protocol_id == AMQP_PROTOCOL_ID
            && let Some(reply) = final_settle_second_disposition(bytes, &registry)
        {
            injected
                .send(reply)
                .await
                .map_err(|_| io::Error::new(io::ErrorKind::BrokenPipe, "outgoing bridge closed"))?;
        }
        writer.write_all(unit.bytes()).await?;
    }
    writer.shutdown().await
}

async fn forward_outgoing<R, W>(
    reader: R,
    mut writer: W,
    mut injected: mpsc::Receiver<Vec<u8>>,
    registry: DeliveryTagRegistry,
) -> io::Result<()>
where
    R: AsyncRead + Unpin,
    W: AsyncWrite + Unpin,
{
    let mut wire = WireReader::new(reader);
    loop {
        tokio::select! {
            biased;
            Some(frame) = injected.recv() => writer.write_all(&frame).await?,
            unit = wire.next() => {
                let Some(unit) = unit? else {
                    writer.shutdown().await?;
                    return Ok(());
                };
                match unit {
                    WireUnit::Header(bytes) => writer.write_all(&bytes).await?,
                    WireUnit::Frame { protocol_id, bytes } => {
                        let bytes = if protocol_id == AMQP_PROTOCOL_ID {
                            adapt_outgoing_frame(bytes, &registry)
                        } else {
                            bytes
                        };
                        writer.write_all(&bytes).await?;
                    }
                }
            }
        }
    }
}

fn adapt_outgoing_frame(mut frame: Vec<u8>, registry: &DeliveryTagRegistry) -> Vec<u8> {
    let Some((channel, performative_start, performative_len, performative)) =
        decode_performative(&frame)
    else {
        return frame;
    };

    match performative {
        Performative::Attach(attach) if attach.role == Role::Sender => {
            registry.lock().sender_links.insert(
                (channel, attach.handle.0),
                SenderLink {
                    name: attach.name,
                    settle_second: attach.rcv_settle_mode == ReceiverSettleMode::Second,
                },
            );
        }
        Performative::Transfer(mut transfer) if transfer.delivery_id.is_some() => {
            let key = (channel, transfer.handle.0);
            let mut state = registry.lock();
            let Some(link) = state.sender_links.get(&key).cloned() else {
                return frame;
            };
            if link.settle_second && transfer.settled != Some(true) {
                state
                    .settle_second_deliveries
                    .entry(channel)
                    .or_default()
                    .insert(transfer.delivery_id.expect("checked above"));
            }
            let Some(tags) = state.requested_tags.get_mut(&link.name) else {
                return frame;
            };
            let Some(delivery_tag) = tags.pop_front() else {
                return frame;
            };
            if tags.is_empty() {
                state.requested_tags.remove(&link.name);
            }
            drop(state);

            transfer.delivery_tag = Some(delivery_tag.into());
            let Ok(encoded) = serde_amqp::to_vec(&transfer) else {
                return frame;
            };
            frame.splice(
                performative_start..performative_start + performative_len,
                encoded,
            );
            let Ok(frame_size) = u32::try_from(frame.len()) else {
                return frame;
            };
            frame[..4].copy_from_slice(&frame_size.to_be_bytes());
        }
        Performative::Detach(detach) => {
            registry
                .lock()
                .sender_links
                .remove(&(channel, detach.handle.0));
        }
        Performative::End(_) => {
            let mut state = registry.lock();
            state
                .sender_links
                .retain(|(link_channel, _), _| *link_channel != channel);
            state.settle_second_deliveries.remove(&channel);
        }
        _ => {}
    }
    frame
}

/// Generate only the omitted suffix so the engine's responses and ours do not
/// overlap.
fn final_settle_second_disposition(
    frame: &[u8],
    registry: &DeliveryTagRegistry,
) -> Option<Vec<u8>> {
    let (channel, _, _, Performative::Disposition(disposition)) = decode_performative(frame)?
    else {
        return None;
    };
    if disposition.role != Role::Receiver
        || disposition.settled
        || !disposition
            .state
            .as_ref()
            .is_some_and(|state| state.is_terminal())
    {
        return None;
    }

    let last = disposition.last.unwrap_or(disposition.first);
    let mut state = registry.lock();
    let pending = state.settle_second_deliveries.get_mut(&channel)?;
    let delivery_ids = pending
        .range(disposition.first..=last)
        .copied()
        .collect::<Vec<_>>();
    for delivery_id in &delivery_ids {
        pending.remove(delivery_id);
    }
    if pending.is_empty() {
        state.settle_second_deliveries.remove(&channel);
    }
    drop(state);

    let final_range_start = delivery_ids
        .windows(2)
        .rposition(|ids| ids[1].wrapping_sub(ids[0]) != 1)
        .map_or(0, |index| index + 1);
    let final_range = delivery_ids.get(final_range_start..)?;
    let first = *final_range.first()?;
    let reply = Disposition {
        role: Role::Sender,
        first,
        last: final_range.last().copied(),
        settled: true,
        state: disposition.state,
        batchable: false,
    };
    encode_frame(channel, &Performative::Disposition(reply), &[])
}

fn decode_performative(frame: &[u8]) -> Option<(u16, usize, usize, Performative)> {
    if frame.len() < FRAME_HEADER_BYTES || frame[5] != AMQP_FRAME_TYPE {
        return None;
    }
    let frame_size = u32::from_be_bytes(frame[..4].try_into().ok()?) as usize;
    if frame_size != frame.len() {
        return None;
    }
    let performative_start = usize::from(frame[4]).checked_mul(4)?;
    let body = frame.get(performative_start..)?;
    if body.is_empty() {
        return None;
    }
    let performative_len = described_list_len(body)?;
    let performative = serde_amqp::from_slice(body.get(..performative_len)?).ok()?;
    let channel = u16::from_be_bytes(frame[6..8].try_into().ok()?);
    Some((channel, performative_start, performative_len, performative))
}

fn described_list_len(encoded: &[u8]) -> Option<usize> {
    if encoded.first() != Some(&0x00) {
        return None;
    }
    let descriptor_len = match *encoded.get(1)? {
        0x44 => 1,
        0x53 => 2,
        0x80 => 9,
        0xa3 => 2 + usize::from(*encoded.get(2)?),
        0xb3 => {
            let length = u32::from_be_bytes(encoded.get(2..6)?.try_into().ok()?) as usize;
            5 + length
        }
        _ => return None,
    };
    let list_start = 1_usize.checked_add(descriptor_len)?;
    let list_len = match *encoded.get(list_start)? {
        0x45 => 1,
        0xc0 => 2 + usize::from(*encoded.get(list_start + 1)?),
        0xd0 => {
            let length = u32::from_be_bytes(
                encoded
                    .get(list_start + 1..list_start + 5)?
                    .try_into()
                    .ok()?,
            ) as usize;
            5 + length
        }
        _ => return None,
    };
    let total = list_start.checked_add(list_len)?;
    (total <= encoded.len()).then_some(total)
}

fn encode_frame(channel: u16, performative: &Performative, payload: &[u8]) -> Option<Vec<u8>> {
    let encoded = serde_amqp::to_vec(performative).ok()?;
    let frame_size = FRAME_HEADER_BYTES
        .checked_add(encoded.len())?
        .checked_add(payload.len())?;
    let mut frame = Vec::with_capacity(frame_size);
    frame.extend_from_slice(&u32::try_from(frame_size).ok()?.to_be_bytes());
    frame.extend_from_slice(&[2, AMQP_FRAME_TYPE]);
    frame.extend_from_slice(&channel.to_be_bytes());
    frame.extend_from_slice(&encoded);
    frame.extend_from_slice(payload);
    Some(frame)
}

enum WireUnit {
    Header(Vec<u8>),
    Frame { protocol_id: u8, bytes: Vec<u8> },
}

impl WireUnit {
    fn bytes(&self) -> &[u8] {
        match self {
            Self::Header(bytes) | Self::Frame { bytes, .. } => bytes,
        }
    }
}

struct WireReader<R> {
    reader: R,
    buffer: Vec<u8>,
    protocol_id: Option<u8>,
}

impl<R: AsyncRead + Unpin> WireReader<R> {
    fn new(reader: R) -> Self {
        Self {
            reader,
            buffer: Vec::new(),
            protocol_id: None,
        }
    }

    async fn next(&mut self) -> io::Result<Option<WireUnit>> {
        loop {
            if self.buffer.starts_with(b"AMQP") {
                if self.buffer.len() >= PROTOCOL_HEADER_BYTES {
                    let bytes = self
                        .buffer
                        .drain(..PROTOCOL_HEADER_BYTES)
                        .collect::<Vec<_>>();
                    self.protocol_id = Some(bytes[4]);
                    return Ok(Some(WireUnit::Header(bytes)));
                }
            } else if self.buffer.len() >= 4 {
                let frame_size =
                    u32::from_be_bytes(self.buffer[..4].try_into().expect("length checked"))
                        as usize;
                if !(FRAME_HEADER_BYTES..=MAX_WIRE_FRAME_BYTES).contains(&frame_size) {
                    return Err(io::Error::new(
                        io::ErrorKind::InvalidData,
                        format!("invalid AMQP frame size {frame_size}"),
                    ));
                }
                if self.buffer.len() >= frame_size {
                    let bytes = self.buffer.drain(..frame_size).collect::<Vec<_>>();
                    let protocol_id = self.protocol_id.ok_or_else(|| {
                        io::Error::new(
                            io::ErrorKind::InvalidData,
                            "AMQP frame arrived before a protocol header",
                        )
                    })?;
                    return Ok(Some(WireUnit::Frame { protocol_id, bytes }));
                }
            }

            let read = self.reader.read_buf(&mut self.buffer).await?;
            if read == 0 {
                return if self.buffer.is_empty() {
                    Ok(None)
                } else {
                    Err(io::Error::new(
                        io::ErrorKind::UnexpectedEof,
                        "connection ended inside an AMQP frame",
                    ))
                };
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use amqp_runtime::types::{
        definitions::{ReceiverSettleMode, Role, SenderSettleMode},
        messaging::{Accepted, DeliveryState},
        performatives::{Attach, Disposition, Transfer},
    };

    use super::*;

    fn sender_attach(name: &str, handle: u32, settle_second: bool) -> Performative {
        Performative::Attach(Attach {
            name: name.to_owned(),
            handle: handle.into(),
            role: Role::Sender,
            snd_settle_mode: SenderSettleMode::Unsettled,
            rcv_settle_mode: if settle_second {
                ReceiverSettleMode::Second
            } else {
                ReceiverSettleMode::First
            },
            source: None,
            target: None,
            unsettled: None,
            incomplete_unsettled: false,
            initial_delivery_count: Some(0),
            max_message_size: None,
            offered_capabilities: None,
            desired_capabilities: None,
            properties: None,
        })
    }

    fn transfer(handle: u32, delivery_id: u32, delivery_tag: &[u8]) -> Performative {
        Performative::Transfer(Transfer {
            handle: handle.into(),
            delivery_id: Some(delivery_id),
            delivery_tag: Some(delivery_tag.to_vec().into()),
            message_format: Some(0),
            settled: None,
            more: false,
            rcv_settle_mode: None,
            state: None,
            resume: false,
            aborted: false,
            batchable: false,
        })
    }

    fn accepted_disposition(first: u32, last: Option<u32>) -> Performative {
        Performative::Disposition(Disposition {
            role: Role::Receiver,
            first,
            last,
            settled: false,
            state: Some(DeliveryState::Accepted(Accepted {})),
            batchable: false,
        })
    }

    fn frame(channel: u16, performative: Performative, payload: &[u8]) -> Vec<u8> {
        encode_frame(channel, &performative, payload).expect("frame encodes")
    }

    #[test]
    fn registered_tag_replaces_only_the_transfer_performative() {
        let registry = DeliveryTagRegistry::default();
        let attach = frame(3, sender_attach("receiver", 7, false), &[]);
        adapt_outgoing_frame(attach, &registry);
        registry.register("receiver", vec![9; 16].into());

        let payload = b"encoded AMQP message";
        let original = frame(3, transfer(7, 42, &[0, 0, 0, 1]), payload);
        let adapted = adapt_outgoing_frame(original, &registry);
        let (_, start, length, Performative::Transfer(transfer)) =
            decode_performative(&adapted).expect("transfer decodes")
        else {
            panic!("expected transfer");
        };

        assert_eq!(
            transfer.delivery_tag.as_ref().map(|tag| tag.as_slice()),
            Some([9; 16].as_slice())
        );
        assert_eq!(&adapted[start + length..], payload);
        assert_eq!(
            u32::from_be_bytes(adapted[..4].try_into().unwrap()) as usize,
            adapted.len()
        );
    }

    #[test]
    fn unregistered_transfer_keeps_generated_tag() {
        let registry = DeliveryTagRegistry::default();
        adapt_outgoing_frame(
            frame(1, sender_attach("receiver", 2, false), &[]),
            &registry,
        );
        let adapted =
            adapt_outgoing_frame(frame(1, transfer(2, 3, &[0, 0, 0, 4]), b"body"), &registry);
        let (_, _, _, Performative::Transfer(transfer)) =
            decode_performative(&adapted).expect("transfer decodes")
        else {
            panic!("expected transfer");
        };

        assert_eq!(
            transfer.delivery_tag.as_ref().map(|tag| tag.as_slice()),
            Some([0, 0, 0, 4].as_slice())
        );
    }

    #[test]
    fn settle_second_echo_supplies_the_omitted_final_range() {
        let registry = DeliveryTagRegistry::default();
        adapt_outgoing_frame(frame(5, sender_attach("receiver", 4, true), &[]), &registry);
        for delivery_id in [7, 8, 11, 12] {
            adapt_outgoing_frame(
                frame(5, transfer(4, delivery_id, &[0, 0, 0, 1]), b"body"),
                &registry,
            );
        }

        let disposition = frame(5, accepted_disposition(7, Some(12)), &[]);
        let echo =
            final_settle_second_disposition(&disposition, &registry).expect("echo generated");
        let (_, _, _, Performative::Disposition(echo)) =
            decode_performative(&echo).expect("disposition decodes")
        else {
            panic!("expected disposition");
        };

        assert_eq!(echo.role, Role::Sender);
        assert_eq!(echo.first, 11);
        assert_eq!(echo.last, Some(12));
        assert!(echo.settled);
    }

    #[test]
    fn settle_second_echo_covers_a_single_consecutive_range() {
        let registry = DeliveryTagRegistry::default();
        adapt_outgoing_frame(frame(2, sender_attach("receiver", 1, true), &[]), &registry);
        for delivery_id in 20..=22 {
            adapt_outgoing_frame(
                frame(2, transfer(1, delivery_id, &[0, 0, 0, 1]), b"body"),
                &registry,
            );
        }

        let disposition = frame(2, accepted_disposition(20, Some(22)), &[]);
        let echo =
            final_settle_second_disposition(&disposition, &registry).expect("echo generated");
        let (_, _, _, Performative::Disposition(echo)) =
            decode_performative(&echo).expect("disposition decodes")
        else {
            panic!("expected disposition");
        };

        assert_eq!(echo.first, 20);
        assert_eq!(echo.last, Some(22));
    }
}
