//! Bounded inbound listeners (see `docs/design/operations.md`).
//!
//! Every HTTP listener of both processes accepts through [`guarded`]. A
//! connection is closed when
//!
//! - nothing was read or written on it for [`IDLE`];
//! - it waits for a request (nothing was read since the last answer)
//!   for [`BUSY_IDLE`] while the listener holds more than half of the
//!   connections it may: connections that only sit there are the first
//!   to go when room is short, and one with a request in flight is left
//!   alone;
//! - nothing was written on it for [`UNANSWERED`], whatever was read: a
//!   client that sends a request head a byte at a time is never idle,
//!   and never gets an answer either.
//!
//! At most a fixed number of connections are open at once; further ones
//! wait in the accept queue and take no open file. A client that opens
//! sockets and sends nothing, or stops in the middle of a request head,
//! therefore holds a slot for a bounded time and never an open file for
//! good.
//!
//! [`drain_within`] bounds the wait for open connections once a listener
//! was told to stop, so that a process that must exit does exit.

use std::future::Future;
use std::io;
use std::net::SocketAddr;
use std::pin::Pin;
use std::sync::Arc;
use std::task::{Context, Poll};
use std::time::Duration;

use axum::serve::{Listener, ListenerExt, TapIo};
use tokio::io::{AsyncRead, AsyncWrite, ReadBuf};
use tokio::net::{TcpListener, TcpStream};
use tokio::sync::{OwnedSemaphorePermit, Semaphore};
use tokio::time::{Instant, Sleep};

/// A connection with no byte read or written for this long is closed.
/// It bounds the life of an idle keep-alive connection. Not a round
/// minute: a client that asks once a minute (a scraper, a page that
/// refreshes) would send its next request at the moment its connection
/// is closed.
pub const IDLE: Duration = Duration::from_secs(75);

/// How long a connection may wait for a request while the listener
/// holds more than half of the connections it may. It is also how often
/// an open connection looks at its bounds.
pub const BUSY_IDLE: Duration = Duration::from_secs(5);

/// A connection on which nothing was written for this long is closed,
/// whatever was read on it meanwhile. It bounds the time a client may
/// take to send a request, and the time one request may take.
pub const UNANSWERED: Duration = Duration::from_secs(120);

/// Connections the public listener (`server.bind`, and the setup
/// listener) holds open at once. Further ones wait in the accept queue.
pub const MAX_CONNECTIONS: usize = 2048;

/// Connections a metrics listener holds open at once.
pub const MAX_METRICS_CONNECTIONS: usize = 32;

/// How long a listener that was told to stop waits for its open
/// connections before the process goes on without them.
pub const DRAIN: Duration = Duration::from_secs(10);

/// The time bounds of one connection.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct Bounds {
    /// No byte either way for this long: closed.
    pub idle: Duration,
    /// Waiting for a request this long while the listener is more than
    /// half full: closed. Also the period at which a connection looks
    /// at its bounds.
    pub busy_idle: Duration,
    /// No byte written for this long: closed.
    pub unanswered: Duration,
}

impl Bounds {
    /// [`IDLE`], [`BUSY_IDLE`] and [`UNANSWERED`].
    pub const DEFAULT: Bounds = Bounds {
        idle: IDLE,
        busy_idle: BUSY_IDLE,
        unanswered: UNANSWERED,
    };

    /// When a connection last read or written at `last`, last written
    /// at `wrote`, is due to be closed; `busy` says that its listener is
    /// more than half full.
    pub fn due(&self, last: Instant, wrote: Instant, busy: bool) -> Instant {
        // Nothing was read since the last answer (or since it was
        // opened): no request is in flight on it.
        let waiting = last <= wrote;
        let idle = if busy && waiting {
            self.idle.min(self.busy_idle)
        } else {
            self.idle
        };
        (last + idle).min(wrote + self.unanswered)
    }
}

/// The connection slots of one listener.
#[derive(Debug, Clone)]
struct Slots {
    free: Arc<Semaphore>,
    max: usize,
}

impl Slots {
    fn new(max: usize) -> Slots {
        let max = max.max(1);
        Slots {
            free: Arc::new(Semaphore::new(max)),
            max,
        }
    }

    /// More than half of the slots are taken.
    fn busy(&self) -> bool {
        self.free.available_permits() * 2 < self.max
    }
}

/// A TCP listener with a connection cap and time bounds per connection.
pub struct Guarded {
    inner: TcpListener,
    slots: Slots,
    bounds: Bounds,
}

/// The listener type [`guarded`] returns. The no-op tap is what gives it
/// axum's `ConnectInfo<SocketAddr>`.
pub type GuardedListener = TapIo<Guarded, fn(&mut Timed<TcpStream>)>;

fn untouched(_: &mut Timed<TcpStream>) {}

/// Wraps `listener`: at most `max` connections at once, each under
/// [`Bounds::DEFAULT`].
pub fn guarded(listener: TcpListener, max: usize) -> GuardedListener {
    Guarded {
        inner: listener,
        slots: Slots::new(max),
        bounds: Bounds::DEFAULT,
    }
    .tap_io(untouched as fn(&mut Timed<TcpStream>))
}

impl Listener for Guarded {
    type Io = Timed<TcpStream>;
    type Addr = SocketAddr;

    async fn accept(&mut self) -> (Self::Io, Self::Addr) {
        loop {
            // The slot is taken before the accept, so a full listener
            // leaves further connections in the kernel's queue and does
            // not spend an open file on them.
            let Ok(slot) = self.slots.free.clone().acquire_owned().await else {
                // The semaphore is never closed.
                tokio::time::sleep(Duration::from_secs(1)).await;
                continue;
            };
            match self.inner.accept().await {
                Ok((io, addr)) => {
                    let held = Some((slot, self.slots.clone()));
                    return (Timed::new(io, self.bounds, held), addr);
                }
                Err(e) => {
                    drop(slot);
                    if !connection_error(&e) {
                        // Out of files or memory: wait for some to come
                        // back instead of spinning.
                        tracing::warn!(error = %e, "accept failed");
                        tokio::time::sleep(Duration::from_secs(1)).await;
                    }
                }
            }
        }
    }

    fn local_addr(&self) -> io::Result<Self::Addr> {
        self.inner.local_addr()
    }
}

/// An accept error that belongs to the one connection and not to the
/// listener.
fn connection_error(e: &io::Error) -> bool {
    matches!(
        e.kind(),
        io::ErrorKind::ConnectionRefused
            | io::ErrorKind::ConnectionAborted
            | io::ErrorKind::ConnectionReset
    )
}

/// A stream that fails with `TimedOut` once it is past its [`Bounds`].
/// It holds its listener's connection slot until dropped.
pub struct Timed<S> {
    inner: S,
    bounds: Bounds,
    /// The last byte read or written.
    last: Instant,
    /// The last byte written; the time the stream was opened before
    /// the first.
    wrote: Instant,
    timer: Pin<Box<Sleep>>,
    held: Option<(OwnedSemaphorePermit, Slots)>,
}

impl<S> Timed<S> {
    fn new(inner: S, bounds: Bounds, held: Option<(OwnedSemaphorePermit, Slots)>) -> Self {
        let now = Instant::now();
        Self {
            inner,
            bounds,
            last: now,
            wrote: now,
            timer: Box::pin(tokio::time::sleep_until(
                now + bounds.busy_idle.min(bounds.idle),
            )),
            held,
        }
    }

    /// Whether the stream is past its bounds. Progress only stores an
    /// instant; the bounds are looked at here, every `busy_idle`, so a
    /// busy stream does not pay for a timer reset per byte and a
    /// listener that fills up is noticed by the streams that sit idle.
    fn expired(&mut self, cx: &mut Context<'_>) -> bool {
        loop {
            if self.timer.as_mut().poll(cx).is_pending() {
                return false;
            }
            let now = Instant::now();
            let busy = self.held.as_ref().is_some_and(|(_, slots)| slots.busy());
            let due = self.bounds.due(self.last, self.wrote, busy);
            if now >= due {
                return true;
            }
            self.timer
                .as_mut()
                .reset(due.min(now + self.bounds.busy_idle));
        }
    }
}

fn timed_out() -> io::Error {
    io::Error::new(io::ErrorKind::TimedOut, "connection idle")
}

impl<S: AsyncRead + Unpin> AsyncRead for Timed<S> {
    fn poll_read(
        self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        buf: &mut ReadBuf<'_>,
    ) -> Poll<io::Result<()>> {
        let this = self.get_mut();
        match Pin::new(&mut this.inner).poll_read(cx, buf) {
            Poll::Ready(r) => {
                this.last = Instant::now();
                Poll::Ready(r)
            }
            Poll::Pending if this.expired(cx) => Poll::Ready(Err(timed_out())),
            Poll::Pending => Poll::Pending,
        }
    }
}

impl<S: AsyncWrite + Unpin> AsyncWrite for Timed<S> {
    fn poll_write(
        self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        buf: &[u8],
    ) -> Poll<io::Result<usize>> {
        let this = self.get_mut();
        match Pin::new(&mut this.inner).poll_write(cx, buf) {
            Poll::Ready(r) => {
                this.last = Instant::now();
                this.wrote = this.last;
                Poll::Ready(r)
            }
            Poll::Pending if this.expired(cx) => Poll::Ready(Err(timed_out())),
            Poll::Pending => Poll::Pending,
        }
    }

    fn poll_flush(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<io::Result<()>> {
        let this = self.get_mut();
        match Pin::new(&mut this.inner).poll_flush(cx) {
            Poll::Ready(r) => Poll::Ready(r),
            Poll::Pending if this.expired(cx) => Poll::Ready(Err(timed_out())),
            Poll::Pending => Poll::Pending,
        }
    }

    fn poll_shutdown(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<io::Result<()>> {
        Pin::new(&mut self.get_mut().inner).poll_shutdown(cx)
    }
}

/// Awaits `served` (a listener's serve future). Once `stopping` resolves
/// (the listener was told to stop) the wait is bounded by `limit`:
/// `None` means connections were still open then and were left behind.
pub async fn drain_within<F, S>(served: F, stopping: S, limit: Duration) -> Option<F::Output>
where
    F: Future,
    S: Future,
{
    let deadline = async {
        stopping.await;
        tokio::time::sleep(limit).await;
    };
    tokio::select! {
        r = served => Some(r),
        () = deadline => None,
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use tokio::io::{AsyncReadExt, AsyncWriteExt};

    const QUICK: Bounds = Bounds {
        idle: Duration::from_millis(300),
        busy_idle: Duration::from_millis(50),
        unanswered: Duration::from_secs(3600),
    };

    #[test]
    fn when_a_connection_is_due() {
        let b = Bounds::DEFAULT;
        let t0 = Instant::now();
        // Quiet since t0, answered at t0.
        assert_eq!(b.due(t0, t0, false), t0 + IDLE);
        // A listener more than half full gives a connection that waits
        // for a request seconds.
        assert_eq!(b.due(t0, t0, true), t0 + BUSY_IDLE);
        // One with a request in flight keeps its time.
        let asked = t0 + Duration::from_secs(1);
        assert_eq!(b.due(asked, t0, true), asked + IDLE);
        // Bytes keep arriving, and nothing was ever written: the read
        // progress does not move the second bound.
        let reading = t0 + Duration::from_secs(110);
        assert_eq!(b.due(reading, t0, false), t0 + UNANSWERED);
    }

    #[tokio::test]
    async fn a_silent_stream_times_out_and_a_busy_one_does_not() {
        let (a, mut b) = tokio::io::duplex(64);
        let mut t = Timed::new(a, QUICK, None);
        // Bytes arriving inside the allowance keep the stream alive well
        // past it.
        for _ in 0..4 {
            tokio::time::sleep(Duration::from_millis(150)).await;
            b.write_all(b"x").await.expect("write");
            let mut one = [0u8; 1];
            t.read_exact(&mut one).await.expect("read");
        }
        // Silence for the whole allowance ends it, and not before.
        let started = Instant::now();
        let mut one = [0u8; 1];
        let e = t.read_exact(&mut one).await.expect_err("idle");
        assert_eq!(e.kind(), io::ErrorKind::TimedOut);
        assert!(started.elapsed() >= QUICK.idle - Duration::from_millis(20));
    }

    #[tokio::test]
    async fn a_request_sent_a_byte_at_a_time_is_never_idle_and_is_closed_all_the_same() {
        let bounds = Bounds {
            unanswered: Duration::from_millis(400),
            ..QUICK
        };
        let (a, mut b) = tokio::io::duplex(64);
        let mut t = Timed::new(a, bounds, None);
        let started = Instant::now();
        let feeder = tokio::spawn(async move {
            loop {
                tokio::time::sleep(Duration::from_millis(100)).await;
                if b.write_all(b"x").await.is_err() {
                    break;
                }
            }
        });
        let mut one = [0u8; 1];
        let e = loop {
            match t.read_exact(&mut one).await {
                Ok(_) => {}
                Err(e) => break e,
            }
        };
        feeder.abort();
        assert_eq!(e.kind(), io::ErrorKind::TimedOut);
        assert!(started.elapsed() >= bounds.unanswered - Duration::from_millis(20));
        assert!(started.elapsed() < Duration::from_secs(5));
    }

    #[tokio::test]
    async fn an_idle_connection_goes_within_seconds_once_its_listener_is_half_full() {
        let slots = Slots::new(4);
        let hold = |slots: &Slots| slots.free.clone().try_acquire_owned().expect("slot");
        let (a, _b) = tokio::io::duplex(64);
        let mut idle = Timed::new(a, QUICK, Some((hold(&slots), slots.clone())));
        // One of four taken: not busy, the stream has its whole idle time.
        assert!(!slots.busy());
        let others = [hold(&slots), hold(&slots)];
        // Three of four: busy.
        assert!(slots.busy());
        let started = Instant::now();
        let mut one = [0u8; 1];
        let e = idle.read_exact(&mut one).await.expect_err("reaped");
        assert_eq!(e.kind(), io::ErrorKind::TimedOut);
        assert!(started.elapsed() < QUICK.idle, "{:?}", started.elapsed());
        drop(others);
    }

    #[tokio::test]
    async fn the_drain_is_bounded_only_after_the_stop() {
        let limit = Duration::from_millis(100);
        // Before the stop the serve future may run for any time.
        let (tx, rx) = tokio::sync::oneshot::channel::<()>();
        let served = async {
            tokio::time::sleep(limit * 4).await;
            7
        };
        let r = drain_within(served, std::future::pending::<()>(), limit).await;
        assert_eq!(r, Some(7));
        // After it, a serve future that never ends is left behind.
        let started = Instant::now();
        tx.send(()).expect("send");
        let r = drain_within(std::future::pending::<()>(), rx, limit).await;
        assert_eq!(r, None);
        assert!(started.elapsed() >= limit);
    }

    #[tokio::test]
    async fn a_full_listener_leaves_the_next_connection_waiting() {
        let l = TcpListener::bind("127.0.0.1:0").await.expect("bind");
        let addr = l.local_addr().expect("addr");
        let mut g = Guarded {
            inner: l,
            slots: Slots::new(1),
            bounds: Bounds::DEFAULT,
        };
        let _c1 = TcpStream::connect(addr).await.expect("c1");
        let _c2 = TcpStream::connect(addr).await.expect("c2");
        let (first, _) = g.accept().await;
        let second = tokio::time::timeout(Duration::from_millis(200), g.accept()).await;
        assert!(second.is_err(), "the second accept waits for a slot");
        drop(first);
        let third = tokio::time::timeout(Duration::from_secs(5), g.accept()).await;
        assert!(third.is_ok(), "a closed connection frees its slot");
    }
}
