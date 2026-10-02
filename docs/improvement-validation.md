# Improvement validation

Baseline: main `1fe2a53086ed60446b2c95c0ae6d2328b8b5eb27` (PR46, v1.5.5).

The local Release solution run passes 743 unit tests and 36 Avalonia UI tests. The 57 emulator integration tests are skipped locally because Docker/emulator endpoints are unavailable; the PR workflow runs the five-provider emulator suite. No live cloud mutation or credential changes are used for this phase. The lab Release build and Windows single-file publication are also checked locally; the PR workflow packages all four runtime targets and verifies downloaded checksums.

Deterministic coverage includes lost send acknowledgements, cancellation before/after acknowledgement, proven rejection with stable IDs, repeated/competing owners, storage failures before Sending/Sent/Deleting/Moved, failed or unknown source deletion, configuration/access changes, failed scheduled snapshot preparation, stale backup selections, corrupt/unwritable metadata caches, and actual body-size validation. Real-process updater tests retain the existing receipt, rollback, lock and startup acknowledgement coverage. UI tests exercise selective continuation, Activity clear/restore with a newer concurrent entry, explicit lazy body opening, update retry, repeated clicks and attempted close during installation.

Regression evidence retained in the isolated E-drive task directory:

- `updater-red.log` → `updater-green.log`: generic failure claimed the current files were unchanged; queued progress overwrote terminal state.
- `backup-baseline-red.log` → `improvement-tests-green.log`: main allocated 33,657,856 bytes listing an 8 MiB legacy body placed before metadata; the streaming implementation passes a 4 MiB allocation ceiling without materializing that body.
- `rollback-red.log` → `new-flows-green.log`: missing previous files could still produce a successful-restoration claim; recovery now checks recorded backups and retains the receipt on failure.

Recovery scope is explicit. Pending continuation and proven-rejection retry are separate actions. Generic SDK errors remain uncertain, including errors that may follow internal retries; current adapters do not infer rejection proof from an exception's wording or HTTP status. Confirmed sends never resend. Source deletion recovery is manual. A scheduled snapshot starts blocked (`AwaitingScheduleClaim`), and only the window that consumes the exact scheduled job activates it. Incomplete claim/activation remains blocked and requires inspection; startup never sends operation history automatically. Legacy selective recovery without a configuration identity is blocked. Purge/standalone deletion retain their existing Activity records rather than acquiring send-style retry.

Backup JSON remains the durable authority. Metadata cache files are disposable, bounded, and outside the backup tree. Listing scans at most 512 MiB per legacy file, retains at most 256 KiB of metadata, and never authorizes settlement. Body viewing has a 64 MiB file limit; replay validates actual body bytes against its 32 MiB batch limit. Activity clearing persists only a reversible display cutoff and never deletes operation history, schedules, backups, messages or journal files.
