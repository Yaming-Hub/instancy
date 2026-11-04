# Running tests with cargo-nextest

CI runs the unit and integration tests with
[cargo-nextest](https://nexte.st/) 0.9.150 under the `ci` profile in
`.config/nextest.toml`. Each test runs once, in its own process.

| Job | Command |
|---|---|
| Test | `cargo nextest run -p instancy --all-features --lib --tests --profile ci --no-tests fail` |
| No default features | `cargo nextest run -p instancy --no-default-features --features test-utils --lib --tests --profile ci --no-tests fail` |
| Schedule tests (Shuttle) | See [Schedule tests](#schedule-tests-shuttle-and-loom) |
| Model-checked tests (Loom) | See [Schedule tests](#schedule-tests-shuttle-and-loom) |

Doc tests, the full conformance matrix and the release flight-recorder check
keep their own `cargo test` commands.

## The `ci` profile

- **No retries.** A test that fails once has failed.
- **No fail-fast.** The run continues after a failure, so it reports every
  failing test.
- **Timeouts.** A test still running after 120 s is terminated, which fails
  it. `cluster_teardown_adversarial::writer_only_failure_never_hangs_or_passes_silently`
  runs three fault variants and gets 180 s. A test that needs more time gets
  its own exact-test override, never a larger global limit.
- **Four tests at a time**, and at most two that open TCP connections (the
  `tcp-cluster` group). The group's filter names whole binaries where TCP
  tests are mixed with local ones, and the library's transport tests. The
  audit repros (`tests/audit_repros.rs`) are in the group too, so they run
  two at a time. Check what it selects with
  `cargo nextest show-config test-groups -p instancy --all-features --profile ci`.
- `--no-tests fail` fails a run that selects no tests.

## Running locally

```sh
cargo nextest run -p instancy --all-features --lib --tests --profile ci
```

Plain `cargo test` still works.

## Schedule tests (Shuttle and Loom)

Some library tests run real synchronization code under a scheduler that
explores thread interleavings: [Shuttle](https://docs.rs/shuttle/0.9.9/shuttle/)
in modules named `*_shuttle`, [Loom](https://docs.rs/loom/0.7.2/loom/) in
modules named `*_loom`. A crate cfg swaps the tool's models in for the
synchronization under test, so they build only on Linux. CI runs each tool
in its own job:

```sh
RUSTFLAGS="-Dwarnings --cfg instancy_shuttle" \
  cargo nextest run -p instancy --locked --lib \
  --no-default-features --features tracing,transport,test-utils --profile ci \
  --test-threads 1 --no-tests fail --failure-output immediate-final \
  -E 'test(/_shuttle::/)'

RUSTFLAGS="-Dwarnings --cfg instancy_loom" \
  cargo nextest run -p instancy --locked --lib \
  --no-default-features --features tracing,transport,test-utils --profile ci \
  --test-threads 1 --no-tests fail --failure-output immediate-final \
  -E 'test(/_loom::/)'
```

- Run only that tool's tests with its cfg: outside a model run, the swapped
  primitives panic when used.
- Each test states how it explores: every interleaving (Shuttle's
  depth-first scheduler, or Loom without a preemption bound), or a fixed
  number of executions sampled from a fixed seed (Shuttle's PCT scheduler,
  which fails if `SHUTTLE_RANDOM_SEED` is set). Loom tests set their bounds
  in code and fail if a `LOOM_*` variable would change them.
- Neither tool's condition variable times out, so a missed notification
  fails the test as a deadlock instead of waiting out a timeout.
- A Shuttle failure prints the schedule that reproduces it (pass it to
  `shuttle::replay`).

## Differences from `cargo test`

Every test has its own process, so process-wide state (the progress
checker's mode, the trace directory, `OnceLock`s, panic hooks) is fresh for
each test, and a `static` mutex that serialised tests inside one `cargo test`
binary no longer serialises anything. A test must not depend on another
test having run, or not run, in the same process; under `cargo test` it must
still pass alongside the others.