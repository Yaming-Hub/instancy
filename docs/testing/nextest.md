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
| Multi-process harness | `cargo nextest run -p instancy-integration --profile ci --no-tests fail` (the job installs `protoc`, see below) |

Doc tests, the full conformance matrix, the release flight-recorder check and
the exhaustive lifecycle check keep their own `cargo test` commands.

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
  two at a time, as is every test of the multi-process harness
  (`instancy-integration`). Check what it selects with
  `cargo nextest show-config test-groups -p instancy -p instancy-integration --all-features --profile ci`
  (the harness needs `protoc`, see below).
- `--no-tests fail` fails a run that selects no tests.

## Running locally

```sh
cargo nextest run -p instancy --all-features --lib --tests --profile ci
```

The multi-process harness builds dactor, whose build script needs `protoc`
on `PATH` or named by `PROTOC`:

```sh
cargo nextest run -p instancy-integration --profile ci
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

## The lifecycle machine, checked exhaustively

`runtime::cluster_lifecycle::exhaustive` drives the cluster lifecycle
coordinator through every order in which its events can reach it, for one
peer (see `docs/design/cluster.md`, §5.5.4). Each configuration is a test:
Shared mode on one connection or on two, or Dedicated mode; one or two local
workers; one or two progress channels; transport faults (a write error or a
reset) or inbound ones (records lost on their way in, corrupt frames). A
debug build is too slow for them, so they are ignored. CI's `lifecycle` job
runs eleven of them in release, in two parts that run at the same time; each
part's checking takes about six minutes, not counting its build. A part runs
two lanes at once, and a lane runs its configurations one after another,
each in its own process, so that the log shows each one's time and peak
memory. The lanes, in `.github/workflows/ci.yml`, are balanced by time. The
job checks that every configuration is in exactly one lane or is listed
there as run by hand, so a new configuration goes in one or the other. The
same eleven, locally:

```sh
cargo test -p instancy --release --all-features --lib -- --include-ignored \
  --nocapture --test-threads 2 cluster_lifecycle::exhaustive:: \
  --skip dedicated_two_workers --skip shared_split_two_workers \
  --skip two_channels_inbound --skip two_workers_one_channel_inbound
```

To run the others too, by hand, drop the `--skip`s and run one test at a time
(`--test-threads 1`): the largest, split Shared mode with two workers and two
channels and transport faults, visits 423 million states in about 16 minutes
and needs about 20 GB. `--nocapture` prints, for each configuration, its
counts and a shortest counterexample of every way it breaks a property.

What each configuration breaks is pinned in
`instancy/src/runtime/cluster_lifecycle/exhaustive/<configuration>.pins`: for
each property, the number of violations, the set of their identities and a
shortest trace. A test that finds anything else fails and says whether it
found an identity that is not pinned (a new counterexample) or no longer
finds a pinned one. Once the change is meant, run the test again with
`INSTANCY_EXHAUSTIVE_PIN=1` to rewrite the file: a fix shows in its diff as
identities removed, none added.

## Differences from `cargo test`

Every test has its own process, so process-wide state (the progress
checker's mode, the trace directory, `OnceLock`s, panic hooks) is fresh for
each test, and a `static` mutex that serialised tests inside one `cargo test`
binary no longer serialises anything. A test must not depend on another
test having run, or not run, in the same process; under `cargo test` it must
still pass alongside the others.