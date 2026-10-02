# Post-v1.5.6 stability validation

Baseline: main `d23a81e6f9ccc09a727c35b4f05ad70d3b97ba2a`, PR47 merged, [main CI36963796595](https://github.com/bitcodepro/QueueLoom/actions/runs/36963796595) successful, and v1.5.6 published. This is one bounded stability cycle after that release.

## Confirmed regressions and fixes

1. **Incomplete scheduled activation could become resumable.** The activation loop changed items to `Pending` individually. An injected failure before the second state write left the first item selectable in Activity after reopening. A snapshot interrupted after its last item became Pending could also send through the replay executor. Activation now records its requirement in the plan and atomically publishes an ID-bound completion marker only after every state write. History hides pending selection until that proof exists, and both executors independently enforce it. Partial activation remains blocked for manual inspection. Old scheduled snapshots without completion proof are also blocked; known confirmed/uncertain states remain visible and unchanged. A successfully activated new snapshot still supports selective pending continuation and provider-proven rejection retry with its saved IDs.
2. **A damaged Activity display cutoff prevented startup from loading environments.** Empty, truncated and invalid timestamp files made journal loading throw before profile loading. Invalid cutoff text now acts as no display filter. The malformed file and all durable records remain intact; no journal data is deleted.

The first retained regression run against unchanged main failed five unit cases: two activation cases and three damaged-cutoff cases. The scheduled-history checkbox test also failed against unchanged main. The final expanded regression files were copied to a separate baseline checkout without changing production code: six unit cases failed and one passed, and the UI case failed. The extra failure checks the newly required completion-publication boundary, which the baseline did not have. These same tests pass with the fixes.

## Local validation

- Release solution: **775 unit tests and 38 Avalonia UI tests passed**. All 58 provider integration tests were explicitly skipped because local emulator endpoints were not configured.
- Release lab build: zero warnings/errors.
- Windows compressed, self-contained single-file publish and ZIP/checksum packaging passed. Packaged read-only MCP initialization/tool discovery passed.
- Real packaged Windows update smoke passed: old-process delayed exit, paths with spaces, confirmed GUI startup, receipt/download/backup cleanup, and preservation of unrelated files. The installed executable was built from the same changed production files in a separate source export, so runtime-specific restore did not modify the review checkout's lock files.
- Sixteen added unit cases cover activation interruptions before the first/second state and final proof publication, corrupt/foreign proof, legacy snapshots, competing activation owners, selective rejection recovery with stable IDs, corrupt Activity cutoff startup, and cancellation at all four pre-install updater stages followed by a fresh verified retry. Two added UI cases cover blocked scheduled-history selection/repeated continuation and cancel/repeated clear/restore with operation history retained.
- Existing full-suite checks revalidated lost acknowledgements, confirmed-send/deletion crash boundaries, changing connection/write access, competing replay owners, failed-attempt/retry pacing, the RabbitMQ return-versus-nack/timeout distinction, metadata-cache reconstruction, stale body selection, actual backup body limits, updater stale progress/repeated clicks, receipt recovery and transaction ownership.
- The existing RabbitMQ broker recovery test now also checks the scheduled activation gate before its proven-rejection/binding-repair/stable-ID recovery flow. It is compiled locally and executed by the CI emulator job.

All data, caches, downloads, builds and test results belong to `E:\Temp\polymarket-hl-strategy\ServiceBusExplorer-Test\stability-20261002`. The existing SDK was used read-only. The initial solution restore used locked dependencies with online vulnerability metadata; later disposable offline exports used cached dependencies with NuGet auditing disabled for those local commands only. CI retains its normal restore/audit policy. Local Codex's memory directory contained no historical files; earlier task checkpoints and repository validation documents were read as context.

## Evidence and limits

Retained files in the isolated cycle directory:

- `baseline-full.log`: unchanged main's 759 unit/36 UI passes.
- `stability-red.log`: initial five failing unit regressions before any production edit.
- `stability-final-tests-red.log`, `stability-ui-red.log`, and `baseline-main`: final regression sources tested against unchanged production.
- `stability-expanded.log`, `stability-ui.log`, and `final-head-full.log`: passing focused and complete suites; TRX files under `evidence`.
- `lab-build.log`, `publish-win.log`, `package-win.log`, `smoke-package.log`, `smoke-update.log`: build/package/process evidence.
- `evidence/screenshots`: actual Avalonia-rendered Activity, backups and updater windows, including blocked scheduled history and restored Activity; changed-screen captures were visually inspected.

Workspace/AMQP fakes drive deterministic failure tests; the UI harness uses in-memory profiles, vault, backup repository and demo broker workspace with offline update discovery. Updater download tests use an in-process HTTP handler and real ZIP/checksum/extraction code. The package smoke launches the actual Windows executable and helper with isolated data directories and a disposable old-process fixture. No live broker resources or credentials were mutated.

Native desktop input on the changed screens could not be performed because the computer-use skill's required `node_repl` tool is unavailable. Headless Avalonia rendering and real packaged Windows process smoke are separate checks and do not claim manual native interaction. Docker Desktop was found, but its read-only CLI check was unresponsive and was cancelled; its internal WSL distribution does not support Docker CLI usage. The final PR workflow must supply five-provider emulator integration/provider UI, Linux/macOS builds and native package-host rendering, and all four package/checksum checks; its exact-head result is recorded in the PR description and task handoff.

Confirmed or uncertain sends remain non-retriable. Source deletion recovery stays manual; unclaimed/incompletely activated schedules and legacy selective recovery without configuration identity stay blocked. Crash tests inject persistence failures or reconstruct the exact persisted boundary; they do not simulate machine power loss. No universal exactly-once delivery claim is made. Independent review, merge and release remain with the parent workflow.
