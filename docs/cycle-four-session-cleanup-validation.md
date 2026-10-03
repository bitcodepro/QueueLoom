# Cycle four: Azure session cleanup

Baseline main and v1.5.16: `9f689cfce3fb12c57dd8bf2c6e5dacb6788d4049`.
Main CI `37115908475` succeeded; the published release has the matching tag
and eight assets. This is the fourth and final authorized audit cycle.

## Confirmed defect

Active session browsing accepts and holds multiple Azure session receivers to
produce a globally ordered page. On the baseline, the first receiver whose
`DisposeAsync` throws stops the cleanup loop. Later accepted sessions never get
a close attempt and can block their consumers until the locks expire. That
cleanup exception also replaces a peek failure, caller cancellation or the
explicit limit error for more than 100 available sessions.

Cleanup now attempts every accepted receiver. If browsing failed, its original
exception, stack and cancellation token survive; the collected cleanup errors
are attached as an `AggregateException` in `Exception.Data["SessionCleanupErrors"]`.
If browsing succeeded but cleanup failed, it throws an aggregate containing all
close errors and warns that some session locks may remain until expiry. A failed
close is not reported as a proven release. No messages are settled or changed.

The UI displays the separate operator warning for successful browsing with failed
cleanup. General exception summaries unwrap a single-inner aggregate, which would
otherwise discard this warning. The display still uses the existing bounded,
fail-closed redactor; it does not include SDK close details. Global exception
summarization and redaction semantics are unchanged.

## Reproduction and regression evidence

Tests-only local commit `ce5724e372e4da2705693ba473dbfd4d0dff4f8a` leaves all
production files identical to main. Its retained baseline run has **9 failing
unit cases and 2 passing controls**, plus **1 failing actual-window UI case**.
Every original assertion passes after the correction.

The witnesses call the public `AzureServiceBusWorkspace.BrowseMessagesAsync`
entry point using the real Azure workspace with virtual Azure SDK clients and
session receivers. Only connection/topology setup and injected SDK failures are
synthetic; no network transport or user broker is used. Queue and subscription
cases cover multiple close failures, peek failure, cancellation followed by a
successful retry, the session limit, global ordering and repeated successful
browsing. An explicit close gate proves disconnect waits through cleanup and
checks the later receiver's close attempt before disconnect completes.

The UI witness opens the real Avalonia main window and executes its actual
async command. It cancels a blocked session peek twice, checks every accepted
receiver's close attempt and the normal cancelled UI state, then successfully
browses again. It captures dispatcher errors. The unchanged baseline fails
because the second receiver never receives a close attempt.

Run focused checks with:

```powershell
dotnet test tests/QueueLoom.Tests/QueueLoom.Tests.csproj -c Release --filter FullyQualifiedName~CycleFourSessionCleanup
dotnet test tests/QueueLoom.UiTests/QueueLoom.UiTests.csproj -c Release --filter FullyQualifiedName~CycleFourSessionCleanup
```

Logs and TRX are retained under `cycle-four-20261003/evidence` on E:, including
`red-baseline.log`, `red-ui.log`, `green-focused.log` and `green-ui.log`.
The initial test compile failed on nullable-reference diagnostics; its setup log
is retained separately and is not counted as a red runtime witness.
Full local and exact-head CI counts are recorded in the PR and handoff evidence.

Independent review of initial PR head `e414eb73edfdb3580da72eedaf3763592c31926e`
found the single-close display gap. Tests-only local commit
`cc7764c92e95576a7cd0eb56375db7cb707a0862` retains production identical to that
head. Both actual-window cases fail: one close loses the lock-expiry warning;
two closes retain it but also show SDK detail text. The correction retains the
original aggregate and all close errors for diagnosis while showing only the
separate warning through the UI's existing sanitizer. The same assertions pass
after the fix, including omission of a synthetic password. Review red/green logs
and TRX are retained separately in the evidence directory.

## Bounded coverage and limits

This pass inspected active-session paging and lock ownership; Azure,
multi-provider and leased-workspace lifecycle gates; MCP profile/configuration
validation, approval and access restoration; topic-routing dialog lifetime and
write-expiry checks; and provider queue-management/rule operations. Existing
regressions cover routing expiry, profile coordination, replay recovery and
provider lifecycle draining. No second defect is claimed without reproduction.
No dependencies, broker mutation permissions or release workflow are changed.

Virtual SDK failures prove cleanup control flow, not the Azure service's actual
network fault timing or a failed receiver close's remote outcome. Local UI uses
Avalonia headless rendering; native interactive desktop behavior is not claimed.
Local broker tests are skipped without endpoints; CI runs isolated emulators
and their UI tests. No live user broker or credential is used. This bounded pass
is not exhaustive verification of every provider or failure combination.

PR56-58 safeguards remain intact: RabbitMQ selected Delete/Move stays disabled;
confirmed backed-up purge and Copy remain; Kafka sparse paging and failed-count
monitoring, metadata separation, atomic exports, Avro bounds, fail-closed
redaction, history-tail recovery, Azure confirmed acknowledgements, updater
ownership and profile/credential coordination remain covered by the full suite.
Existing connected clients retain their established configuration; older clients
and external writers remain outside the profile coordinator.

Denied historical lab-data backup/replay directories were left untouched. The
user approved one Git invocation trusting only this new checkout for the baseline
fetch; no persistent or broader trust setting was written. All current checkout,
build, cache, temporary and evidence paths are under the authorized E: test root.
