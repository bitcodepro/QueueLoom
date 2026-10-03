# Cycle three: profile consistency, imports and monitor counts

Baseline main and v1.5.15: `70f64a19b9f2f89e7b91719363dbdaa75bd41f86`.
This bounded cycle audits profile editing/import/persistence, monitor alerts,
scheduled resends, pending-message removal and configuration changes. It changes
no dependencies and does not alter broker mutation permissions.

## Confirmed boundaries

| Finding | Retained baseline witness | Correction |
| --- | --- | --- |
| Import normalization | Case-variant JSON properties can override the fresh local identity/read-only values assigned before case-insensitive deserialization. Import can overwrite an existing profile and reuse its credential slot. | Assign a fresh ID, read-only access and an empty configuration revision after deserialization, before validation and persistence. Valid legacy imports still work and need their own credentials. |
| Profile transactions | Two actual repository instances can commit metadata A with synthetic credential B. A failed metadata save can also roll back over B's successful credential. An Azure connection can read the changed secret with old metadata before the edit commits. | Hold a separate cross-process profile lock through capture, credential changes, metadata commit and rollback. Reject stale edits/deletes. Stored-credential readers take the same lock and check committed configuration before retrieval; every provider and the Kafka registry use this reader. |
| Kafka DLQ counts | A DLT without a partition leader becomes successful zero in the shared snapshot path, clearing the prior notification and resetting its baseline. | Carry the failed DLQ count into runtime and return a failed, unknown snapshot without sampling or changing the previous count. Monitoring retains the notification and baseline; recovery to nine after seven reports an increase of two, and only a confirmed zero resolves it. |

Profile coordination uses `.profile-mutations.lock` before any individual
`.storage.lock` operation. The storage lock is never held recursively. Selection
and metadata retain their existing atomic repository commit. Imports hold the
profile lock while obtaining current names and adding new identities.
The lock is released before reloading the view or awaiting the import report.
An open report cannot prevent another window from editing profiles or reading
stored credentials through the coordinator.

Credential changes first persist a per-profile incomplete-update marker. If the
process dies, rollback fails, or marker cleanup fails, guarded readers remain
blocked. Recovery requires entering all credentials still used by the profile.
A failed recovery retains the marker. If metadata already committed, marker
cleanup failure does not roll credentials back underneath it. Existing vault
entry names and encrypted payloads remain compatible; no secret schema migration
or credential-bearing transaction journal is introduced.

## Reproduction and checks

The tests-only local commit `d1afa6002c6586a34c02a2e903980f0594b7e9c0`
contains the original fourteen cases with production unchanged from main.
Ten fail and four controls pass. The same fourteen pass after the fixes.
The witnesses use two real JSON repository instances, synthetic credentials,
explicit pause/release gates, a real encrypted vault with a synthetic master key,
and the real Azure connection entry point. Its deliberately unparsable synthetic
credential prevents any remote broker request on the baseline. Kafka uses the
real topology index, leased-workspace snapshot path and view-model monitor check
with an isolated workspace and no timer or external service.

Four additional controls cover both legacy credential kinds, incomplete-update
reader rejection, explicit repair and refusal to clear the marker without
reviewed replacement credentials. The legacy registry rollback tests remain
unchanged and cover before/after write/remove failures, cancellation and metadata
failure. Run focused validation with:

```powershell
dotnet test tests/QueueLoom.Tests/QueueLoom.Tests.csproj -c Release --filter FullyQualifiedName~CycleThree
```

Retained local evidence under `cycle-three-20261003/evidence` includes baseline
red and green TRX, locked audited restores, full unit/UI logs and solution/Lab
Release builds. The first concurrent full run hit the existing 100 ms redactor
timeout in two logging/redaction assertions; its failure log is retained. Neither
the fail-closed timeout behavior nor those assertions was weakened. Final counts
and exact-head CI/package evidence are recorded in the handoff JSON.

Independent review reproduced an overly broad import lock on PR head
`b3f8634af917cc684a2fa45f82cfa7356ba02530`. The tests-only local commit
`10ff207db20f249b41be0fee6433b2111f750c57` retains production unchanged at
that head: mixed-validity and invalid-only imports both prevent a second real
repository instance from acquiring the coordinator while the report stays open;
the valid-only control passes (two red, one green). The same three tests pass
when lock ownership ends immediately after the commits. Each witness uses an
explicit dialog gate, bounded cancellation and isolated synthetic files; it
actually saves another profile before dismissing the report. The retained
`review-modal-red-final` and `review-modal-green` TRX record this follow-up.

## Audited coverage and limits

Existing tests and code were inspected for scheduled job configuration identity,
atomic consumption across windows, crash activation/recovery, pending Azure
message identity checks and backup-before-removal, monitor cancellation and
connection restoration, partial snapshot baselines, profile secret rollback,
legacy registry repair and production read-only loading. No further defect was
claimed without a reachable reproduction.

The cross-process guarantees apply to the production JSON repository and
encrypted vault. In-memory test implementations remain optional adapters.
Already-connected clients keep their established broker connection; this change
does not remotely revoke credentials or reconnect them automatically. Interrupted
updates require explicit repair rather than reconstructing unknown credentials.
Older app versions and arbitrary external writers do not participate in the new
profile lock. This is a bounded audit, not exhaustive verification.

RabbitMQ selected Delete/Move remains disabled. Copy and confirmed backed-up
purge remain supported; separated Type/AppId metadata and documented legacy
ambiguity are unchanged. Kafka paging, Avro bounds, staged message exports,
diagnostic fail-closed behavior, history crash-tail recovery, Azure partial
acknowledgement reporting and updater ownership protections remain covered by
the full suite. LocalStack 4.14's documented SNS exists:false divergence is
unchanged. Local broker integration tests are skipped without endpoints; CI uses
isolated emulators and their UI checks. No live user broker data is used.
