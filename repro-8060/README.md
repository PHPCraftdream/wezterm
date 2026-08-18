# Reproducing the three defects in PR #8060

Branch `repro-8060`, based on `fe3006aef` (upstream main at the time of the PR).

The working tree holds the **unfixed** `promise/src/spawn.rs` plus four tests and
two small test-only hooks. `apply-fix.patch` changes **production code only** --
the tests and the hooks are byte-identical in both states. One variable moves
between "before" and "after", and it is the code under test.

The patched production code matches PR #8060 line for line (checked with `diff`
against branch `promise-spawn-lost-wakeup`).

## Everything at once

```bash
bash repro-8060/verify.sh
```

The script checks its preconditions, runs all eight outcomes (four before, four
after), applies and reverts the patch, and prints a verdict. It leaves the tree
exactly where it found it, including if you interrupt it partway.

The rest of this file is the same thing by hand, if you would rather watch each
step.

## About the hooks

Defect (2) lives in a window a handful of instructions wide, so racing threads
and hoping will not hit it. The `repro` module in `promise/src/spawn.rs` turns
that window into a rendezvous: `poll` announces that it has entered the window
and waits there until the worker thread has finished completely. That is exactly
the interleaving the defect needs.

The test for defect (3) uses the same rendezvous: the worker holds its panic
until `poll` is parked in the window, so the waker is always stored *after* the
unwind. Without that, the repro would have a hole -- a worker that finishes
unwinding first leaves a disconnected channel, which `poll` reports as an error
without needing any wake.

The hooks change nothing about **what** either side does, only **when**. They
compile out entirely unless `cfg(test)`, and stay inert at runtime until a test
arms them. Both insertion points -- in `poll` and in the worker thread body --
are identical before and after.

## Before (the working tree as it stands)

Two of these tests are supposed to hang, so they have to be killed. Run them as
the built binary rather than through `cargo test`: killing cargo mid-run can
leave the build directory locked, and then the *next* cargo invocation blocks on
that lock instead of on anything to do with the test. That looks exactly like a
flaky repro and isn't one.

```bash
git status --short        # promise/src/spawn.rs modified, not committed

# Build once, and note where the test binary landed.
BIN=$(cargo test -p promise --lib --no-run 2>&1 \
      | sed -n 's/.*Executable unittests[^(]*(\(.*\))/\1/p' | tail -1)
echo "$BIN"

# 1. Control. Passes both before and after; it is here to show the harness
#    is not simply breaking everything it touches.
"$BIN" --exact spawn::tests::happy_path_returns_the_closures_result

# 2. Defect (2), the lost wakeup -- HANGS, has to be killed
timeout 25 "$BIN" --exact spawn::tests::a_result_sent_inside_the_poll_window_is_not_lost

# 3. Defect (1), worker panic once its consumer is cancelled -- FAILS
"$BIN" --exact spawn::tests::a_cancelled_consumer_does_not_panic_the_worker

# 4. Defect (3), a panic in f() -- HANGS
timeout 25 "$BIN" --exact spawn::tests::a_panicking_worker_reports_an_error_instead_of_hanging
```

One at a time (`--exact`) on purpose: the tests share a mutex, so a hanging one
would block the others.

Observed outcomes -- exit code, then what it printed:

* (1) `0` -- `test result: ok. 1 passed`
* (2) `124` -- killed by `timeout`, nothing printed after `running 1 test`
* (3) `101` --
  ```
  the worker thread panicked after its consumer was cancelled: panicked at promise\src\spawn.rs:149:22:
  called `Result::unwrap()` on an `Err` value: "SendError(..)"
  test spawn::tests::a_cancelled_consumer_does_not_panic_the_worker ... FAILED
  ```
* (4) `124` -- killed by `timeout`

## After

```bash
git apply repro-8060/apply-fix.patch
touch promise/src/spawn.rs
cargo test -p promise --lib
```

Observed output:

```
running 4 tests
test spawn::tests::a_cancelled_consumer_does_not_panic_the_worker ... ok
test spawn::tests::a_panicking_worker_reports_an_error_instead_of_hanging ... ok
test spawn::tests::a_result_sent_inside_the_poll_window_is_not_lost ... ok
test spawn::tests::happy_path_returns_the_closures_result ... ok

test result: ok. 4 passed; 0 failed; 0 ignored; 0 measured; 0 filtered out; finished in 5.08s
```

The five seconds are a bounded wait on the panic hook in test (1), on the path
where there is no longer a panic to catch.

All three defect tests are deterministic: (2) and (3) rest on the rendezvous in
the `repro` module, (1) on a channel that keeps the worker alive until its
consumer is definitely gone.

## Getting back to "before"

```bash
git apply -R repro-8060/apply-fix.patch
```

That, and **not** `git checkout -- promise/src/spawn.rs`: the repro state is not
committed, so a checkout would take the tests with it.

## If cargo did not rebuild

Check that the output contains `Compiling promise v0.2.0`. If it goes straight to
`Finished`, cargo decided the file was unchanged by mtime and ran the previous
binary (this caught me while preparing these). Fix:

```bash
touch promise/src/spawn.rs
```

## If cargo complains about incremental

The environment may still carry `CARGO_INCREMENTAL=0`; then:

```bash
env -u CARGO_INCREMENTAL cargo test -p promise --lib
```
