# Running tests with cargo-nextest

CI runs the unit and integration tests with
[cargo-nextest](https://nexte.st/) 0.9.150 under the `ci` profile in
`.config/nextest.toml`. Each test runs once, in its own process.

| Job | Command |
|---|---|
| Test | `cargo nextest run -p instancy --all-features --lib --tests --profile ci --no-tests fail` |
| No default features | `cargo nextest run -p instancy --no-default-features --features test-utils --lib --tests --profile ci --no-tests fail` |

Doc tests, the audit repros, the full conformance matrix and the release
flight-recorder check keep their own `cargo test` commands.

## The `ci` profile

- **No retries.** A test that fails once has failed.
- **Timeouts.** A test still running after 120 s is terminated, which fails
  it. `cluster_teardown_adversarial::writer_only_failure_never_hangs_or_passes_silently`
  runs three fault variants and gets 180 s. A test that needs more time gets
  its own exact-test override, never a larger global limit.
- **Four tests at a time**, and at most two that open TCP connections (the
  `tcp-cluster` group). The group's filter names whole binaries where TCP
  tests are mixed with local ones, and the library's transport tests. Check
  what it selects with
  `cargo nextest show-config test-groups -p instancy --all-features --profile ci`.
- `--no-tests fail` fails a run that selects no tests.

## Running locally

```sh
cargo nextest run -p instancy --all-features --lib --tests --profile ci
```

Plain `cargo test` still works.

## Differences from `cargo test`

Every test has its own process, so process-wide state (the progress
checker's mode, the trace directory, `OnceLock`s, panic hooks) is fresh for
each test, and a `static` mutex that serialised tests inside one `cargo test`
binary no longer serialises anything. A test must not depend on another
test having run, or not run, in the same process; under `cargo test` it must
still pass alongside the others.