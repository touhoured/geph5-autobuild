//! Byte accounting for direct (non-bridged) client connections, tagged by
//! the client's ASN and country — the direct-connection counterpart of the
//! bridge's `bridge_bytes`.

use std::{
    io,
    pin::Pin,
    sync::LazyLock,
    task::{Context, Poll},
};

use dashmap::DashMap;
use geph5_broker_protocol::StatEvent;
use sillad::Pipe;
use tokio::io::{AsyncRead, AsyncWrite, ReadBuf};

static DIRECT_BYTE_COUNTS: LazyLock<DashMap<(u32, String), u64>> = LazyLock::new(DashMap::new);

/// Drains the accumulated per-(ASN, country) byte counts into counter events.
pub fn direct_bytes_stat_events() -> Vec<StatEvent> {
    let keys: Vec<(u32, String)> = DIRECT_BYTE_COUNTS.iter().map(|e| e.key().clone()).collect();
    keys.into_iter()
        .filter_map(|key| {
            let (_, bytes) = DIRECT_BYTE_COUNTS.remove(&key)?;
            let (asn, country) = key;
            Some(StatEvent::counter(
                "exit_direct_bytes",
                &[("asn", &asn.to_string()), ("country", &country)],
                bytes as f64,
            ))
        })
        .collect()
}

/// Wraps a direct client connection, attributing all bytes in both directions
/// to the client's (ASN, country).
pub struct DirectCountPipe<P> {
    inner: P,
    key: (u32, String),
}

impl<P: Pipe> DirectCountPipe<P> {
    pub fn new(inner: P, asn: u32, country: String) -> Self {
        Self {
            inner,
            key: (asn, country),
        }
    }

    fn add(&self, n: usize) {
        if n > 0 {
            *DIRECT_BYTE_COUNTS.entry(self.key.clone()).or_insert(0) += n as u64;
        }
    }
}

impl<P: Pipe> AsyncRead for DirectCountPipe<P> {
    fn poll_read(
        mut self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        buf: &mut ReadBuf<'_>,
    ) -> Poll<io::Result<()>> {
        let before = buf.filled().len();
        let res = Pin::new(&mut self.inner).poll_read(cx, buf);
        if let Poll::Ready(Ok(())) = &res {
            self.add(buf.filled().len() - before);
        }
        res
    }
}

impl<P: Pipe> AsyncWrite for DirectCountPipe<P> {
    fn poll_write(
        mut self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        buf: &[u8],
    ) -> Poll<io::Result<usize>> {
        let res = Pin::new(&mut self.inner).poll_write(cx, buf);
        if let Poll::Ready(Ok(n)) = &res {
            self.add(*n);
        }
        res
    }

    fn poll_flush(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<io::Result<()>> {
        Pin::new(&mut self.inner).poll_flush(cx)
    }

    fn poll_shutdown(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<io::Result<()>> {
        Pin::new(&mut self.inner).poll_shutdown(cx)
    }
}

impl<P: Pipe> Pipe for DirectCountPipe<P> {
    fn shared_secret(&self) -> Option<&[u8]> {
        self.inner.shared_secret()
    }

    fn protocol(&self) -> &str {
        self.inner.protocol()
    }

    fn remote_addr(&self) -> Option<&str> {
        self.inner.remote_addr()
    }
}
