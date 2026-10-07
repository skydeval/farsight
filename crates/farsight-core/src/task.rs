//! Keeping background tasks alive (see `docs/design/operations.md`).
//!
//! A panic in a spawned task ends that task and nothing else: the process
//! keeps running without the subsystem the task was. Every long-lived
//! task therefore runs under [`supervise`], which catches the panic, logs
//! and counts it, and starts the task again; a task that cannot be
//! started again is run with [`catch`] and its caller ends the process.

use std::any::Any;
use std::future::Future;
use std::panic::{AssertUnwindSafe, catch_unwind};
use std::pin::Pin;
use std::task::{Context, Poll};
use std::time::Duration;

/// `farsight_task_panics_total{task}`: panics caught in background tasks.
pub const TASK_PANICS: &str = "farsight_task_panics_total";

/// Pause before a task that panicked is started again.
pub const RESTART_DELAY: Duration = Duration::from_secs(5);

/// A future that turns a panic of the future it wraps into an error
/// carrying the panic message.
pub struct CatchUnwind<F> {
    inner: Pin<Box<F>>,
}

/// Wraps `f` so that a panic while it is polled becomes `Err(message)`.
pub fn catch<F: Future>(f: F) -> CatchUnwind<F> {
    CatchUnwind { inner: Box::pin(f) }
}

impl<F: Future> Future for CatchUnwind<F> {
    type Output = Result<F::Output, String>;

    fn poll(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
        let inner = self.inner.as_mut();
        // The wrapped future is dropped after a panic and never polled
        // again, so no state it left half-changed is observed through it.
        match catch_unwind(AssertUnwindSafe(|| inner.poll(cx))) {
            Ok(Poll::Pending) => Poll::Pending,
            Ok(Poll::Ready(v)) => Poll::Ready(Ok(v)),
            Err(p) => Poll::Ready(Err(panic_message(p.as_ref()))),
        }
    }
}

/// The message of a panic payload.
pub fn panic_message(p: &(dyn Any + Send)) -> String {
    if let Some(s) = p.downcast_ref::<&'static str>() {
        (*s).to_owned()
    } else if let Some(s) = p.downcast_ref::<String>() {
        s.clone()
    } else {
        "panic with a non-string payload".to_owned()
    }
}

/// Logs a caught panic of `task` and counts it in [`TASK_PANICS`].
pub fn report_panic(task: &'static str, message: &str) {
    tracing::error!(task, panic = message, "background task panicked");
    metrics::counter!(TASK_PANICS, "task" => task).increment(1);
}

/// Registers the series of `tasks` at zero so they are visible before
/// first use.
pub fn register(tasks: &[&'static str]) {
    for t in tasks {
        metrics::counter!(TASK_PANICS, "task" => *t).increment(0);
    }
}

/// Runs the task `make` builds until it returns. A panic is reported
/// ([`report_panic`]) and, after [`RESTART_DELAY`], a new task is built
/// and run. Dropping the returned future drops the running task.
pub async fn supervise<F, Fut>(task: &'static str, make: F)
where
    F: FnMut() -> Fut,
    Fut: Future<Output = ()>,
{
    supervise_with(task, RESTART_DELAY, make).await;
}

/// [`supervise`] with the pause given.
pub async fn supervise_with<F, Fut>(task: &'static str, delay: Duration, mut make: F)
where
    F: FnMut() -> Fut,
    Fut: Future<Output = ()>,
{
    loop {
        match catch(make()).await {
            Ok(()) => return,
            Err(message) => {
                report_panic(task, &message);
                tokio::time::sleep(delay).await;
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::Arc;
    use std::sync::atomic::{AtomicU32, Ordering};

    #[tokio::test]
    async fn a_panic_becomes_an_error_with_its_message() {
        let r = catch(async {
            if std::hint::black_box(true) {
                panic!("boom {}", 7);
            }
        })
        .await;
        assert_eq!(r, Err("boom 7".to_owned()));
        assert_eq!(catch(async { 5 }).await, Ok(5));
    }

    #[tokio::test]
    async fn a_panicking_task_is_started_again_until_it_returns() {
        let runs = Arc::new(AtomicU32::new(0));
        let r = runs.clone();
        supervise_with("test", Duration::ZERO, move || {
            let r = r.clone();
            async move {
                // Panics twice, across an await, then returns.
                let n = r.fetch_add(1, Ordering::SeqCst);
                tokio::task::yield_now().await;
                assert!(n >= 2, "run {n} fails");
            }
        })
        .await;
        assert_eq!(runs.load(Ordering::SeqCst), 3);
    }
}
