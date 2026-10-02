# Replay recovery validation

Scope: the two reported differences in the older `BatchReplayStore.RunAsync` executor used by Replay loaded messages, Restore filtered backups, and Resume latest batch. Base main was verified as `1b618ea7ee92e6113ea0f8e1f38e63075d8269fa` (v1.5.9).

## Retained red evidence

Before modifying production code, local test commit `f9b4636c6c44e9f9acaf0ebf9d67c1e037b8d3d7` added regressions over unchanged main. The targeted run compiled successfully and finished with **9 failed, 10 passed, 0 skipped**:

- Two missing-identity JSON snapshots were sent instead of being blocked, with and without retained namespace metadata.
- The Resume command confirmed the operation, then sent a legacy item after a configuration revision change under the same profile ID. This is an inconsistent recovery guard; confirmation was never bypassed.
- All three replay commands and the actual RabbitMQ adapter's structured mandatory-return exception persisted `Uncertain` instead of `Rejected` (four failures).
- Two additional persistence-boundary tests failed because the older executor did not use the existing state-write injection hook. These are boundary coverage for the fix, not separate confirmed transport defects.

The raw red log and TRX remain outside the repository in `ServiceBusExplorer-Test/replay-recovery-evidence/red.log` and `red/`. The test-only commit is retained separately before the fix in PR history.

## Changes and compatibility

Both recovery executors now use the same configuration guard, before transport and before each subsequent send. Missing, empty, or whitespace identity blocks recovery without rewriting history. A missing namespace is compatible when the saved configuration identity establishes the original configuration and matches the connection. Legacy metadata remains readable. Unidentified snapshots require review and preparation of a new operation; current connection information is never retroactively attached as proof of the old environment.

Only an exception from the transport send can classify delivery as `Rejected`. Other transport failures remain `Uncertain`. Saving `Sending` must succeed before transport. Saving an acknowledged `Sent` state occurs outside the send catch: even a rejection-typed persistence exception leaves `Sending`, which cannot be retried. Rejection details use the existing sensitive-data summarizer. Resume never automatically retries `Rejected`; users choose the existing selective Retry proven failures action after route repair, preserving the saved message ID.

Existing cancellation and Kafka envelope tests now prepare identified batches, as the application already does. Explicit legacy tests cover unsupported recovery rather than allowing missing identity in unrelated fixtures.

## Validation and limits

Local Windows validation using the retained .NET 10.0.401 SDK and NuGet cache:

- Targeted regression/adapter tests: **19 passed, 0 failed, 0 skipped**.
- Release solution build: **0 warnings, 0 errors**.
- Full suite: **814 unit tests and 53 UI tests passed**; **59 emulator integration tests skipped** because local emulator endpoints were not configured.
- `git diff --check` passed.

Reproduce from the repository using `dotnet restore QueueLoom.slnx --locked-mode`, `dotnet build QueueLoom.slnx -c Release --no-restore`, and `dotnet test QueueLoom.slnx -c Release --no-build --logger trx`. Filter `FullyQualifiedName~ReplayRecovery|FullyQualifiedName~RabbitPublishRecoveryTests` for the targeted tests.

View-model command tests use an in-memory workspace and real disk-backed replay store. Adapter tests use the actual RabbitMQ send implementation over a protocol-interface fake. The new RabbitMQ integration test uses `RunAsync` against an isolated emulator virtual host, reopens the saved operation, repairs the exchange binding, selectively retries the same generated ID, and verifies no confirmed duplicate. It is executed by the existing Linux CI emulator job when `QUEUELOOM_RABBITMQ` is set.

The PR's exact-head CI provides Windows/Linux/macOS builds and tests, all five broker emulators, four native packages with MCP/rendering smoke checks, Windows updater smoke validation, and downloaded archive/checksum verification. Check its completed jobs before review and squash merge. No real user broker or live cloud resources are mutated by this work.
