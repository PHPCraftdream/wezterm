use anyhow::{anyhow, Result};
use async_executor::Executor;
use flume::{bounded, unbounded, Receiver, TryRecvError};
use std::future::Future;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, Mutex};
use std::task::{Poll, Waker};

pub use async_task::{Runnable, Task};
pub type SpawnFunc = Box<dyn FnOnce() + Send>;
pub type ScheduleFunc = Box<dyn Fn(Runnable) + Send + Sync + 'static>;

fn no_scheduler_configured(_: Runnable) {
    panic!("no scheduler has been configured");
}

lazy_static::lazy_static! {
    static ref ON_MAIN_THREAD: Mutex<ScheduleFunc> = Mutex::new(Box::new(no_scheduler_configured));
    static ref ON_MAIN_THREAD_LOW_PRI: Mutex<ScheduleFunc> = Mutex::new(Box::new(no_scheduler_configured));
    static ref SCOPED_EXECUTOR: Mutex<Option<Arc<Executor<'static>>>> = Mutex::new(None);
}

static SCHEDULER_CONFIGURED: AtomicBool = AtomicBool::new(false);

/// Test-only scaffolding for the reproductions in `mod tests` below.
///
/// The lost-wakeup defect lives in a window a handful of instructions wide,
/// so it cannot be hit reliably by racing threads and hoping. This turns the
/// window into a rendezvous: `poll` announces that it has entered it and then
/// waits there until the worker thread has completely finished, which is the
/// exact interleaving the defect needs. Nothing here changes what either side
/// *does* -- only when it does it -- and it is compiled out entirely unless
/// `cfg(test)`, and inert at runtime unless a test arms it.
///
/// This module and its two call sites are identical in the "before" and
/// "after" commits of this branch; the only thing that differs between them
/// is the production code under test.
#[cfg(test)]
mod repro {
    use std::sync::atomic::{AtomicBool, Ordering};

    /// Armed by the lost-wakeup reproduction; leaves every other test, and
    /// every non-test caller, running unchanged.
    pub static ARMED: AtomicBool = AtomicBool::new(false);
    /// Set by `poll` once it has entered the window between its `try_recv`
    /// and the waker being stored.
    static POLL_IN_WINDOW: AtomicBool = AtomicBool::new(false);
    /// Set by the worker thread once it has run to completion -- including
    /// the send and whatever waking it does or does not perform.
    static WORKER_FINISHED: AtomicBool = AtomicBool::new(false);

    pub fn reset() {
        ARMED.store(false, Ordering::SeqCst);
        POLL_IN_WINDOW.store(false, Ordering::SeqCst);
        WORKER_FINISHED.store(false, Ordering::SeqCst);
    }

    /// Called by `poll` on entering the `Empty` branch, before it takes the
    /// waker mutex.
    pub fn poll_entered_window() {
        if !ARMED.load(Ordering::SeqCst) {
            return;
        }
        POLL_IN_WINDOW.store(true, Ordering::SeqCst);
        while !WORKER_FINISHED.load(Ordering::SeqCst) {
            std::thread::yield_now();
        }
    }

    /// Held by the worker thread as its outermost local, so that it is
    /// dropped last -- after the send and after any wake.
    pub struct WorkerFinishedOnExit;

    impl Drop for WorkerFinishedOnExit {
        fn drop(&mut self) {
            if ARMED.load(Ordering::SeqCst) {
                WORKER_FINISHED.store(true, Ordering::SeqCst);
            }
        }
    }

    /// Called by the reproduction's closure: holds the worker back until
    /// `poll` is parked inside the window.
    pub fn wait_for_poll_window() {
        while !POLL_IN_WINDOW.load(Ordering::SeqCst) {
            std::thread::yield_now();
        }
    }
}

fn schedule_runnable(runnable: Runnable, high_pri: bool) {
    let func = if high_pri {
        ON_MAIN_THREAD.lock()
    } else {
        ON_MAIN_THREAD_LOW_PRI.lock()
    }
    .unwrap();
    func(runnable);
}

pub fn is_scheduler_configured() -> bool {
    SCHEDULER_CONFIGURED.load(Ordering::Relaxed)
}

/// Set callbacks for scheduling normal and low priority futures.
/// Why this and not "just tokio"?  In a GUI application there is typically
/// a special GUI processing loop that may need to run on the "main thread",
/// so we can't just run a tokio/mio loop in that context.
/// This particular crate has no real knowledge of how that plumbing works,
/// it just provides the abstraction for scheduling the work.
/// This function allows the embedding application to set that up.
pub fn set_schedulers(main: ScheduleFunc, low_pri: ScheduleFunc) {
    *ON_MAIN_THREAD.lock().unwrap() = Box::new(main);
    *ON_MAIN_THREAD_LOW_PRI.lock().unwrap() = Box::new(low_pri);
    SCHEDULER_CONFIGURED.store(true, Ordering::Relaxed);
}

/// Spawn a new thread to execute the provided function.
/// Returns a JoinHandle that implements the Future trait
/// and that can be used to await and yield the return value
/// from the thread.
/// Can be called from any thread.
pub fn spawn_into_new_thread<F, T>(f: F) -> Task<Result<T>>
where
    F: FnOnce() -> Result<T>,
    F: Send + 'static,
    T: Send + 'static,
{
    let (tx, rx) = bounded(1);

    // Holds the waker that may later observe
    // during the Future::poll call.
    struct WakerHolder {
        waker: Mutex<Option<Waker>>,
    }

    let holder = Arc::new(WakerHolder {
        waker: Mutex::new(None),
    });

    let thread_waker = Arc::clone(&holder);
    std::thread::spawn(move || {
        #[cfg(test)]
        let _repro_worker = repro::WorkerFinishedOnExit;

        // Run the thread
        let res = f();
        // Pass the result back
        tx.send(res).unwrap();
        // If someone polled the thread before we got here,
        // they will have populated the waker; extract it
        // and wake up the scheduler so that it will poll
        // the result again.
        let mut waker = thread_waker.waker.lock().unwrap();
        if let Some(waker) = waker.take() {
            waker.wake();
        }
    });

    struct PendingResult<T> {
        rx: Receiver<Result<T>>,
        holder: Arc<WakerHolder>,
    }

    impl<T> std::future::Future for PendingResult<T> {
        type Output = Result<T>;

        fn poll(self: std::pin::Pin<&mut Self>, cx: &mut std::task::Context) -> Poll<Self::Output> {
            match self.rx.try_recv() {
                Ok(res) => Poll::Ready(res),
                Err(TryRecvError::Empty) => {
                    #[cfg(test)]
                    repro::poll_entered_window();

                    let mut waker = self.holder.waker.lock().unwrap();
                    waker.replace(cx.waker().clone());
                    Poll::Pending
                }
                Err(TryRecvError::Disconnected) => {
                    Poll::Ready(Err(anyhow!("thread terminated without providing a result")))
                }
            }
        }
    }

    spawn_into_main_thread(PendingResult { rx, holder })
}

fn get_scoped() -> Option<Arc<Executor<'static>>> {
    SCOPED_EXECUTOR.lock().unwrap().as_ref().map(Arc::clone)
}

/// Spawn a future into the main thread; it will be polled in the
/// main thread.
/// This function can be called from any thread.
/// If you are on the main thread already, consider using
/// spawn() instead to lift the `Send` requirement.
pub fn spawn_into_main_thread<F, R>(future: F) -> Task<R>
where
    F: Future<Output = R> + Send + 'static,
    R: Send + 'static,
{
    if let Some(executor) = get_scoped() {
        return executor.spawn(future);
    }
    let (runnable, task) = async_task::spawn(future, |runnable| schedule_runnable(runnable, true));
    runnable.schedule();
    task
}

/// Spawn a future into the main thread; it will be polled in
/// the main thread in the low priority queue--all other normal
/// priority items will be drained before considering low priority
/// spawns.
/// If you are on the main thread already, consider using `spawn_with_low_priority`
/// instead to lift the `Send` requirement.
pub fn spawn_into_main_thread_with_low_priority<F, R>(future: F) -> Task<R>
where
    F: Future<Output = R> + Send + 'static,
    R: Send + 'static,
{
    if let Some(executor) = get_scoped() {
        return executor.spawn(future);
    }
    let (runnable, task) = async_task::spawn(future, |runnable| schedule_runnable(runnable, false));
    runnable.schedule();
    task
}

/// Spawn a future with normal priority.
pub fn spawn<F, R>(future: F) -> Task<R>
where
    F: Future<Output = R> + 'static,
    R: 'static,
{
    let (runnable, task) =
        async_task::spawn_local(future, |runnable| schedule_runnable(runnable, true));
    runnable.schedule();
    task
}

/// Spawn a future with low priority; it will be polled only after
/// all other normal priority items are processed.
pub fn spawn_with_low_priority<F, R>(future: F) -> Task<R>
where
    F: Future<Output = R> + 'static,
    R: 'static,
{
    let (runnable, task) =
        async_task::spawn_local(future, |runnable| schedule_runnable(runnable, false));
    runnable.schedule();
    task
}

/// Block the current thread until the passed future completes.
pub use async_io::block_on;

pub struct SimpleExecutor {
    rx: Receiver<SpawnFunc>,
}

impl SimpleExecutor {
    pub fn new() -> Self {
        let (tx, rx) = unbounded();

        let tx_main = tx.clone();
        let tx_low = tx.clone();
        let queue_func = move |f: SpawnFunc| {
            tx_main.send(f).ok();
        };
        let queue_func_low = move |f: SpawnFunc| {
            tx_low.send(f).ok();
        };
        set_schedulers(
            Box::new(move |task| {
                queue_func(Box::new(move || {
                    task.run();
                }))
            }),
            Box::new(move |task| {
                queue_func_low(Box::new(move || {
                    task.run();
                }))
            }),
        );
        Self { rx }
    }

    pub fn tick(&self) -> anyhow::Result<()> {
        match self.rx.recv() {
            Ok(func) => func(),
            Err(err) => anyhow::bail!("while waiting for events: {:?}", err),
        };
        Ok(())
    }
}

pub struct ScopedExecutor {}

impl ScopedExecutor {
    pub fn new() -> Self {
        SCOPED_EXECUTOR
            .lock()
            .unwrap()
            .replace(Arc::new(Executor::new()));

        Self {}
    }

    pub async fn run<T>(&self, future: impl Future<Output = T>) -> T {
        get_scoped()
            .expect("SCOPED_EXECUTOR to be alive as long as ScopedExecutor")
            .run(future)
            .await
    }
}

impl Drop for ScopedExecutor {
    fn drop(&mut self) {
        SCOPED_EXECUTOR.lock().unwrap().take();
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::atomic::Ordering;
    use std::time::Duration;

    /// `ScopedExecutor` publishes itself into the process-global
    /// `SCOPED_EXECUTOR` for the duration of its lifetime, and the `repro`
    /// scaffolding is process-global too, so these tests must not run
    /// concurrently with each other.
    static TEST_MUTEX: Mutex<()> = Mutex::new(());

    /// Sanity check on the ordinary path, so that a change which breaks the
    /// channel or the wake outright shows up here rather than as a puzzling
    /// hang somewhere else. Passes both before and after the fix; it is here
    /// to show the harness itself is sound.
    #[test]
    fn happy_path_returns_the_closures_result() {
        let _guard = TEST_MUTEX.lock().unwrap_or_else(|p| p.into_inner());
        repro::reset();
        let scoped = ScopedExecutor::new();
        let result = block_on(scoped.run(async { spawn_into_new_thread(|| Ok(42)).await }));
        assert_eq!(result.unwrap(), 42);
    }

    /// Defect (2): the lost wakeup.
    ///
    /// `poll` does one `try_recv`, and only *afterwards* stores the waker. A
    /// worker that sends in between takes the waker mutex, finds nothing to
    /// wake, and exits; `poll` then stores a waker that nobody will ever use
    /// and returns `Pending`. The result is sitting in the channel the whole
    /// time -- one more poll would deliver it -- but no poll is ever
    /// scheduled.
    ///
    /// The `repro` scaffolding forces exactly that interleaving, so the
    /// outcome is not a matter of timing:
    ///
    /// * before the fix this test never returns, and has to be killed;
    /// * after the fix it returns `7` immediately, because `poll` registers
    ///   the waker first and then re-checks the channel.
    #[test]
    fn a_result_sent_inside_the_poll_window_is_not_lost() {
        let _guard = TEST_MUTEX.lock().unwrap_or_else(|p| p.into_inner());
        repro::reset();
        repro::ARMED.store(true, Ordering::SeqCst);

        let scoped = ScopedExecutor::new();
        let result = block_on(scoped.run(async {
            spawn_into_new_thread(|| {
                // Hold the result back until `poll` is parked in the window.
                repro::wait_for_poll_window();
                Ok(7)
            })
            .await
        }));

        repro::reset();
        assert_eq!(
            result.unwrap(),
            7,
            "a result sent while poll was between its try_recv and storing \
             the waker must still be delivered"
        );
    }

    /// Defect (1): the worker thread panics when its consumer has gone away.
    ///
    /// `tx.send(res).unwrap()` treats a disconnected channel as impossible.
    /// It isn't: the future that owns the receiving end is dropped whenever
    /// the caller is cancelled, and the worker cannot know that. Nothing
    /// reads the result at that point, so discarding it is the whole of the
    /// correct response -- taking the thread down instead is not.
    ///
    /// A panic on a spawned thread does not fail the test that spawned it,
    /// so the panic is observed through a hook rather than assumed.
    ///
    /// * before the fix this test fails, printing the captured panic;
    /// * after the fix it passes.
    #[test]
    fn a_cancelled_consumer_does_not_panic_the_worker() {
        let _guard = TEST_MUTEX.lock().unwrap_or_else(|p| p.into_inner());
        repro::reset();

        let (release_tx, release_rx) = bounded::<()>(1);
        let (panic_tx, panic_rx) = bounded::<String>(1);

        let previous_hook = std::panic::take_hook();
        std::panic::set_hook(Box::new(move |info| {
            let _ = panic_tx.try_send(format!("{}", info));
        }));

        let scoped = ScopedExecutor::new();
        let task = spawn_into_new_thread(move || {
            // Stay alive until the consumer is definitely gone.
            let _ = release_rx.recv();
            Ok(())
        });

        // Let the executor poll the pending future once, so that this models
        // a consumer that was already awaiting rather than one that never
        // started.
        block_on(scoped.run(async_io::Timer::after(Duration::from_millis(50))));

        // The consumer is cancelled: dropping the task, and then the
        // executor holding it, drops the future that owns the receiving end.
        drop(task);
        drop(scoped);

        release_tx.send(()).unwrap();

        let panicked = panic_rx.recv_timeout(Duration::from_secs(5));
        std::panic::set_hook(previous_hook);

        assert!(
            panicked.is_err(),
            "the worker thread panicked after its consumer was cancelled: {}",
            panicked.unwrap_or_default()
        );
    }

    /// Defect (3): a panic in `f` unwinds past both the send and the wake.
    ///
    /// `poll` already knows how to report a worker that produced no result --
    /// it turns the disconnected channel into an error -- but it only gets
    /// the chance if something wakes it up to look.
    ///
    /// Left to its own timing this repro would have a hole: a worker that
    /// finishes unwinding before the first poll leaves a disconnected
    /// channel, which `poll` reports as an error without needing any wake.
    /// The same rendezvous as the lost-wakeup test closes it -- the worker
    /// holds its panic until `poll` is parked inside the window, so the
    /// waker is always stored *after* the unwind, on both sides of the fix:
    ///
    /// * before the fix nothing ever wakes that waker, so this test never
    ///   returns and has to be killed;
    /// * after the fix the re-check behind the waker store sees the
    ///   disconnected channel and reports the error deterministically.
    #[test]
    fn a_panicking_worker_reports_an_error_instead_of_hanging() {
        let _guard = TEST_MUTEX.lock().unwrap_or_else(|p| p.into_inner());
        repro::reset();
        repro::ARMED.store(true, Ordering::SeqCst);

        let scoped = ScopedExecutor::new();
        let result: anyhow::Result<()> = block_on(scoped.run(async {
            spawn_into_new_thread(|| -> anyhow::Result<()> {
                // Hold the panic back until `poll` is parked in the window,
                // so the unwind cannot win the race against the first poll.
                repro::wait_for_poll_window();
                panic!("intentional test panic, exercising the unwind path")
            })
            .await
        }));

        repro::reset();
        assert!(
            result.is_err(),
            "a thread that panics before producing a result must report an \
             error, not hang forever"
        );
    }
}
