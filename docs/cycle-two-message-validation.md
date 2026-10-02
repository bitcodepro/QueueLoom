# Cycle two: message and diagnostic failure boundaries

Baseline: main and v1.5.14 at `f134ab6bb10487c5bd57ff0d69e059b773b44861`.
This cycle covers browse/paging/search, inspector/composer, exports and persisted
message copies, and logging/error handling. It makes no dependency changes.

## Confirmed behavior and retained witnesses

| Area | Baseline failure | Correction and controls |
| --- | --- | --- |
| Diagnostic failure boundaries | A deterministic redactor timeout escapes file logging; a throwing logger escapes the actual async UI search command's error handler. | Redaction omits the entire input on timeout; file formatting and UI logging remain best effort. Tests exercise the real regex runner, logger recovery, secret omission, subsequent operations and the Avalonia dispatcher. |
| Export integrity | JSON and CSV truncate an existing file before observing cancellation, including cancellation after a row, or before a later enumeration failure. | Write and flush an owned sibling temporary file, then replace only after success. Tests retain old bytes, require no partial new destination, preserve unrelated temporary files and verify successful replacement. |
| RabbitMQ metadata | Basic Type/AppId projections collide with legal headers named `amqp-type`/`amqp-app-id`, breaking composer/property serialization and CSV export; a header alone is promoted into a basic property. | Keep basic metadata separately in editable properties and exports, leaving user headers independent. Retained tests cover real composer sends, wire mapping, visible broker properties, JSON/CSV, backups, scheduled jobs and durable replay/copy. |
| Avro bounds | Compressed single records and JSON escaping expand beyond the viewer's 16 MiB limit; registry-framed null arrays also expand beyond it. | Bound container input, cumulative inflation, JSON writes and escaped token materialization, including registry output. Small null/deflate records remain readable, while oversized JSON search safely fails closed. |
| Kafka newest paging | Offset spans count transaction markers/compacted gaps as messages, producing a short page and false exhaustion. Premature Consume(null) also appears to be EOF. Producer timestamps can cause multipart continuation to skip a newer offset permanently. | Widen offset windows until enough surviving records or the low watermark, keeping only bounded tails. Require completed partition boundaries. Merge eligible partition heads in descending offset order, using timestamps between partitions. Tests prove eventual completeness, no overlap, cancellation, real EOF and explicit retry after incomplete reads. |
| Nonpositional browse cap | After 1,000 messages, Load more repeats the same capped request without progress. | Stop offering continuation and show the request cap explicitly. Tests cover RabbitMQ, SQS and Pub/Sub without claiming the broker is exhausted. |

The read-only inspector binds `PropertiesJson`; the Rabbit collision witness concerns
composer/property serialization and export, not a claimed crash of that inspector tab.

## Reproduction and validation

The initial tests-only commit retained 34 unit assertions on unchanged production:
25 failed and 9 controls passed. The actual UI command witness failed independently
with a captured dispatcher exception. Four subsequent tests were run with their
affected production files restored verbatim from main: registry Avro expansion,
premature Kafka termination and mid-write cancellation for JSON and CSV; all four
failed. Both nonmonotonic Kafka timestamp cases were also run against unchanged
KafkaWorkspace and failed. After the first sparse-paging correction, the single
partition timestamp control passed while the multipart witness still failed (4 of 6
records); the final partition-head merge makes both pass.

All 44 focused unit assertions pass after the correction, together with the actual
UI command witness. The complete local Release run passes 1,026 unit tests and 62 UI
tests, with all 65 broker integration tests compiled and skipped without endpoints.
Solution and Lab Release builds complete with zero warnings and errors.
The remaining controls include durable/scheduled RabbitMQ copies
and compatibility with historical files. The isolated integration suite adds real
RabbitMQ collision/copy/backed-up-purge and Kafka transaction-marker paging tests.
Use the complete Release solution tests and CI emulator/package jobs for final counts.

Run the retained focused checks with:

```powershell
dotnet test tests/QueueLoom.Tests/QueueLoom.Tests.csproj -c Release --filter FullyQualifiedName~CycleTwo
dotnet test tests/QueueLoom.UiTests/QueueLoom.UiTests.csproj -c Release --filter FullyQualifiedName~CycleTwo
```

Local evidence is retained outside the checkout in the cycle-two `evidence` directory,
including TRX files, audited locked restore logs, baseline red logs and final green logs.
An intermediate bounded stream override recursed and caused a test-host crash; it
was corrected before validation. That failed run is retained and is not counted as
passing. Oversize fallback assertions use a 400,000-character bound because the
existing 64 KiB hex fallback exceeds 100,000 characters; JSON rejection and the
16 MiB decoding cap remain unchanged assertions.

## Compatibility and limits

New Rabbit backups use schema 3. Older Rabbit backups appended basic metadata after
headers: loading extracts the final projection while retaining an earlier colliding
header. Historical schedules and replay payloads lack a separated-metadata marker;
they retain their prior AMQP interpretation when sent through RabbitMQ. New payloads
carry that marker and retain legal headers independently. A historical single
`amqp-type`/`amqp-app-id` projection cannot establish whether it was originally a
header or a basic property. That ambiguity is not recoverable; old duplicate-property
drafts may still be rejected by existing validation. This change proves no source
identity, and Rabbit selected Delete/Move remains disabled everywhere. Copy and
confirmed, backed-up source purge remain available.

No user broker data is used. Local integration tests are skipped without emulator
endpoints; CI provides five isolated broker emulators and their UI checks. This is a
bounded audit, not exhaustive verification of every provider or malformed schema.
PR55 lifetime draining, raw routing text and protobuf oneof ordering, and PR56
acknowledgement/history/updater ownership behavior remain covered by the full suite.
The documented LocalStack SNS exists:false divergence is unchanged.
