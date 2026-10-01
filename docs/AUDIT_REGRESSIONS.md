# Audit regression evidence

Base: `ec86a7000987689e73358efe37f6412bdc2588d3`. Tests use isolated temporary storage and in-process fakes. No live broker or user data was used. Every candidate below failed at a behavioral assertion before its corresponding production path was changed. Test source was later separated by PR scope; method names are unchanged except where noted.

| Candidate | Reusable regression test | Original failed assertion and cause | Fix |
| --- | --- | --- | --- |
| 1 | `ScheduledJob_ProfileEditedToDifferentNamespaceDoesNotSend(true)` | Expected no sends; one send occurred after editing and reconnecting the same profile ID to another namespace. The schedule checked only profile ID. | Persist a configuration fingerprint and edit revision; block mismatched and legacy jobs in due execution and Run now. |
| 2 | `ScheduledStoreFailure_PreservesPendingJobAndPreventsSendOrCancelSuccess(false/true)` | Throwing Save: expected no sends but one occurred; cancellation expected one pending UI job but found none. Storage errors were swallowed after UI removal. | Save desired remaining jobs before changing UI or sending; preserve pending state and report failure. |
| 3 | `ScheduledSnapshot_CancelledSecondJobDoesNotSendOrMove` | Gate A's send, cancel B using its command, release A. Expected one send/delete request but saw two sends. The snapshot retained B and execution ignored collection membership. | Check membership at execution before taking ownership. |
| 4 | `KafkaReplay_PreservesBinaryKey`, `KafkaReplay_PreservesBinaryHeader`, `KafkaReplay_PreservesDuplicateHeaders`, `KafkaReplay_PreservesNullVersusEmptyValue(true)` | Expected `[255]`, got `[47,119,61,61]` (UTF-8 `/w==`); expected 2 headers, got 1; expected null, got empty bytes. Mapping discarded raw/null/duplicate information. Empty-value control passed. | Carry a raw Kafka envelope through drafts, rewriting, backups and scheduled storage; preserve untouched metadata and reconcile explicit edits. |
| 4 backup | `KafkaBackup_ReloadDraftReplayPreservesRawEnvelope` | Backup/reload/replay expected null, got empty bytes. | New Kafka backup schema 2 preserves raw envelope. |
| 5 | `Resolver_PrefersExactCase`, `Resolver_RejectsAmbiguousUntypedName`, `Resolver_ResolvesProviderSpecificTypedNames` | Expected `orders`, got `Orders`; ambiguous queue/exchange did not throw; typed queue/exchange names were not found. Resolver selected the first case-insensitive match. | Prefer exact names, reject ambiguity, accept provider-specific type prefixes; expose typed identities in get_entities. Non-Azure topology names are case-sensitive. |
| 6 | `SchemaRegistry_RejectsCredentialBearingUrl`, `EnvironmentExport_RejectsLegacyCredentialBearingRegistryUrl` | Userinfo and dummy token query URLs validated as valid; export threw no exception and included the URL. | Reject userinfo/query/fragment in validation and defensively reject legacy credential-bearing export URLs. |
| 7 | `ProtobufSchemaPath_SurvivesRestartAndUnrelatedPreferenceUpdate` | Expected fixture path after reopening store, got null. JSON settings document omitted the property. | Persist/read the path and preserve it during unrelated updates. |
| 8 | `Purge_DeletesDistinctDeliveriesWhenTagsRestartOrApplicationIdsRepeat(null/duplicate-id)` | Actual RabbitChannel + shared purge over AMQP interface fake: expected 2 deleted, got 1. Tags restarted after idle closure; repeated application MessageId also collapsed records. | Operation-lifetime channel and per-delivery identity, separate from application MessageId. |
| 9 | `EmptyRead_ClosesChannelAtEndOfOperation`, `FailedOperation_ReturnsAllAcquiredUnackedDeliveries(mapping/cancellation/backup)` | AMQP interface fake: expected 0 open channels, got 1; expected 0 unacked deliveries after each injected failure, got 1. Empty cleanup returned early; acquired deliveries were missing from purge cleanup. | Unconditional async disposal and cleanup of all outstanding acquired deliveries. |
| 10 | `PurgeCompleted_PreservesOtherEnvironmentRowAndPreviousCount` | Expected B/orders row to remain, but collection was empty after A/orders purge. Matching omitted profile ID. | Match result profile ID before removing rows or zeroing previous counts. |

## Runtime evidence and limitations

- Original logs: `audit-red.trx` (9 failed, 1 passed), `schedule-purge-red.trx` (5 failed), `resolver-red.trx` (4 failed, 1 passed), `kafka-backup-red.trx` (1 failed), `rabbit-fake-red.trx` (6 failed).
- First combined red/green closure: `all-regressions-green.trx`, 29 passed. Later tests add provider case sensitivity, migration and legacy Run now checks.
- Full solution validation initially passed 580 unit and 30 headless UI tests; after added case-sensitivity coverage, 581 unit and 30 UI tests passed. Latest results are in `review-validation.trx` under each test project's TestResults directory.
- Broker integration was not executed: Docker is unavailable and no broker environment variables were configured. 53 integration cases, including two new RabbitMQ batch-size-one no-ID/duplicate-ID cases, were discovered and skipped. AMQP fakes do **not** prove real broker protocol or requeue behavior.
- The unchanged baseline had 550 unit passes and one intermittent existing failure: `ResendMarked_MoveSendsThenRemovesTheOriginals` expected status containing `2 originals removed`, but a posted progress callback left `Sent 2 of 2`. Later full runs passed without altering that production path. This is distinct from the audit regressions.
- Environment failures were corrected before asserting RabbitMQ reproduction: required SDK 10.0.401 was installed inside the task workspace; disk-full xUnit launch failure and a proxy fixture's void-return bug were not counted as defect evidence. Only task-owned disposable SDK components were cleaned.

## Persistence and migration

- Old Kafka backups cannot recover already-lost tombstones, raw bytes or duplicate headers. New Kafka backups use schema 2; old readers reject them. Other-provider backups remain schema 1.
- Scheduled storage uses `scheduled-resends.v2.json`. The v1 file is moved once rather than leaving an executable stale copy for older versions. Legacy jobs lack a configuration identity, remain visible, and require cancellation/review/re-scheduling.
- Editing a profile updates its non-secret configuration revision, including edits to separately stored credentials. Renaming via Edit also conservatively blocks prior schedules. No secrets are hashed or persisted in the schedule identity.
- Schema Registry query strings/fragments are rejected entirely, including non-secret ones, to make the no-secrets export guarantee defensive and predictable. Use the registry's base URL and separate credential fields.
- Kafka dead-letter diagnostic headers continue to be excluded from clean replay. Raw key and other header occurrences, including null header values, survive unedited replay. Explicit edits replace that header name's raw occurrences. An unchanged tombstone remains null; nonempty body edits create a value.

Run local suites with `dotnet test QueueLoom.slnx --logger trx`. Run broker suites only against disposable fixtures configured through the existing emulator workflow.

## Independent review follow-ups

Two independent read-only reviews used gpt-6.1-sol with high reasoning. Each repair was re-reviewed with no remaining actionable findings in its scope.

| Follow-up | Runtime red assertion | Green behavior |
| --- | --- | --- |
| Kafka durable batch replay | KafkaBatchReplay_RetainsRawEnvelopeInDurablePayload(true/false): expected null, got []; expected binary key [255], got UTF-8 /w==. BatchReplayStore reconstructed drafts and payloads without the new envelope. | Preserve optional envelope in preparation, payload, validation and send; reopen the durable store and assert null/empty, binary key/header and duplicate header order. |
| Rabbit cleanup after acknowledgement | Purge_CloseFailurePreservesAcknowledgedCountAndReportsCleanupFailure: expected no exception, got IOException from CloseAsync after one acknowledged deletion. Dispose escaped the purge finally and replaced its result. | Return acknowledged count with a cleanup warning, always attempt transport disposal. |
| Existing CI resend status race | ResendCompleted_LateProgressCannotReplaceTerminalStatus(Copy/Move): expected terminal Resend status, got Sent 1 of 1 after deliberately draining queued progress. Initial CI runs #36 Windows, #38 Ubuntu and #39 macOS also failed existing copy/move status assertions. | Stop accepting progress before terminal status is published. Separate shared prerequisite PR; deterministic late-progress regressions cover copy and move. |

The new follow-up red run contains 3 failed cases; the resend progress red run contains 2 failed cases. A combined focused green run passed 21/21, including original Kafka/Rabbit regressions and existing resend status checks. The additional generated replay message-ID header is intentional; raw header assertions filter original header names.

Legacy batch payloads without the optional KafkaEnvelope field remain readable, but lost information cannot be recovered. Recreate fidelity-sensitive pending Kafka batches from source records or schema-2 backups. Downgrading to an older application can ignore the new field and lose fidelity; faithful replay across downgrade is not guaranteed.

## Review order

The resend progress repair is a shared prerequisite for the six otherwise independent audit PRs. Those PRs are stacked on codex/audit-resend-progress to keep their diffs focused while exercising the CI repair. Kafka is additionally stacked on codex/audit-scheduled-safety because it extends scheduled persistence. Review the progress prerequisite first, then the independent fixes, then Kafka after scheduling. Retarget after approved prerequisite merges; no merges or releases were authorized or performed.
Latest complete local Release validation: 589 unit tests and 30 headless UI tests passed; 53 broker integration cases were discovered and skipped because Docker is unavailable locally. Results: final-validation.trx in each test project. This supersedes historical suite counts above.
