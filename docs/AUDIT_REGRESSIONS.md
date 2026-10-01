# Audit regression evidence

Base: `ec86a7000987689e73358efe37f6412bdc2588d3`. Initial red reproductions used isolated temporary storage and in-process fakes. Final CI also used ephemeral broker containers and test-specific virtual hosts. No user broker or user data was used. Every candidate below failed at a behavioral assertion before its corresponding production path was changed. Test source was later separated by PR scope; method names are unchanged except where noted.

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
- Local broker integration was skipped because Docker is unavailable and no broker environment variables were configured. 53 integration cases, including two new RabbitMQ cases, were discovered locally. Final CI broker-backed execution is documented below. AMQP fakes do **not** prove real broker protocol or requeue behavior.
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

## Published draft heads

Every remote tree matched the independently tested local scoped tree. Focused branch validation passed 40/40 cases; final combined Release validation passed 589 unit and 30 UI tests.

| Draft PR | Scope | Head SHA | Base | CI run |
| --- | --- | --- | --- | --- |
| [#36](https://github.com/bitcodepro/QueueLoom/pull/36) | settings | `9e64c1474dbdc9f49e3c832c0ecedf37d9d52fd5` | `codex/audit-resend-progress` | [run 36907859584](https://github.com/bitcodepro/QueueLoom/actions/runs/36907859584) |
| [#37](https://github.com/bitcodepro/QueueLoom/pull/37) | `8a526a188eb3228410bb56ac8cc1274d46860f40` | [36918223095](https://github.com/bitcodepro/QueueLoom/actions/runs/36918223095) | completed / success |
| [#38](https://github.com/bitcodepro/QueueLoom/pull/38) | mcp-identities | `8309de1cf06486551c30eb3a513ccec2d44618c7` | `codex/audit-resend-progress` | [run 36908019485](https://github.com/bitcodepro/QueueLoom/actions/runs/36908019485) |
| [#39](https://github.com/bitcodepro/QueueLoom/pull/39) | scheduled-safety | `197f4279ff556748802f27d6285c6c8daf05e65b` | `codex/audit-resend-progress` | [run 36908123721](https://github.com/bitcodepro/QueueLoom/actions/runs/36908123721) |
| [#41](https://github.com/bitcodepro/QueueLoom/pull/41) | purge-profile | `292d4c6c4f2e7f49a1971046cf986d3506dfbe79` | `codex/audit-resend-progress` | [run 36908220797](https://github.com/bitcodepro/QueueLoom/actions/runs/36908220797) |
| [#42](https://github.com/bitcodepro/QueueLoom/pull/42) | resend-progress | `a50b78207b0b186c2da8474290df6a99c4dca5af` | `main` | [run 36907724975](https://github.com/bitcodepro/QueueLoom/actions/runs/36907724975) |
| [#43](https://github.com/bitcodepro/QueueLoom/pull/43) | rabbit-lifecycle | `f6ae5bc2f98d38165844e92c4fdeca3b90c4d625` | `codex/audit-resend-progress` | [run 36908243546](https://github.com/bitcodepro/QueueLoom/actions/runs/36908243546) |
| [#44](https://github.com/bitcodepro/QueueLoom/pull/44) | kafka-envelope | `2df29190cc2699c7466d253eba5ab19078994b5d` | `codex/audit-scheduled-safety` | [run 36908445296](https://github.com/bitcodepro/QueueLoom/actions/runs/36908445296) |

Earlier CI snapshot: all eight preceding heads completed successfully. PR37 and PR44 were subsequently updated by the late reviews below; their new-head run states supersede the prior snapshot. No main branch, merge, tag or release changed.

## Final broker-backed coverage

The RabbitMQ PR's [emulator job](https://github.com/bitcodepro/QueueLoom/actions/runs/36908243546/job/110524118501) at head `f6ae5bc2f98d38165844e92c4fdeca3b90c4d625` started `rabbitmq:4-management`, set `QUEUELOOM_RABBITMQ=localhost:5673`, and ran the complete integration namespace. Its log reports **53 integration tests passed, 0 failed, 0 skipped**, plus **3 broker UI tests passed**. Each RabbitMQ fixture creates a unique virtual host and deletes it on disposal.

Exactly two broker-backed regressions were added in [RabbitMqWorkspaceTests.cs](https://github.com/bitcodepro/QueueLoom/blob/f6ae5bc2f98d38165844e92c4fdeca3b90c4d625/tests/QueueLoom.IntegrationTests/RabbitMqWorkspaceTests.cs):

- `QueueLoom.IntegrationTests.RabbitMqWorkspaceTests.Purge_with_batch_size_one_deletes_both_messages_without_application_ids`
- `QueueLoom.IntegrationTests.RabbitMqWorkspaceTests.Purge_with_batch_size_one_deletes_both_messages_with_the_same_application_id`

Both publish two distinct records, reject them into the dead-letter queue, purge with batch size 1, and assert DeletedCount 2, no failures, zero ready messages remaining and two persisted backups. Their passing execution is established by the exact-head test source, the all-integration filter, configured RabbitMQ environment and the complete 53-pass/zero-skip result. The default logger does not print individual successful test names, and successful-run TRX files are not uploaded by the current workflow.

The following seven broker-backed tests already existed in that class and also belong to this successful run; they are prior coverage, not newly added audit regressions:

- `Topology_follows_dead_letter_exchanges_and_marks_the_shared_queue`
- `Dead_letters_of_a_shared_queue_are_read_per_source_and_stay_in_place`
- `Deleting_and_moving_dead_letters_touch_only_the_chosen_ones`
- `Reading_a_quorum_queue_does_not_use_up_its_delivery_limit`
- `Sending_to_an_exchange_uses_the_subject_as_routing_key_and_reports_unroutable_messages`
- `Snapshot_counts_dead_letters_per_queue`
- `Queues_are_created_as_quorum_queues_with_a_dead_letter_queue_and_deleted`

The original candidate-8 failures were reproduced against unchanged production code through the actual RabbitChannel over an interface fake; an unchanged-code broker red run was not performed. Mapping, cancellation, backup and CloseAsync fault-injection regressions remain application/fake tests. The two new broker cases verify fixed purge behavior; they do not directly assert channel counts or unacked state after those injected failures.

The [Kafka PR emulator job](https://github.com/bitcodepro/QueueLoom/actions/runs/36908445296/job/110524802374) passed 51 integration and 3 broker UI tests with zero skips at head `2df29190cc2699c7466d253eba5ab19078994b5d`. Its Kafka broker tests predate this audit. New raw-key/header/tombstone and durable replay regressions use the production mapper/store with isolated fakes; no new raw-envelope broker roundtrip test was added.

## Late review closure: repository upgrade and actual Composer

Root review identified a new upgrade blocker introduced by strict registry URL validation: JsonProfileRepository validated every saved profile during List/Get/Upsert/Delete, so one previously accepted URL hid the entire mixed environment list. `LegacyRegistryUrl_MixedProfilesRemainAccessibleAndRepairableWithoutDataLoss` reproduced three failed `Assert.Null` assertions (userinfo/query/fragment) on InvalidDataException; an unrelated-invalid-metadata control passed. Red log: `legacy-registry-upgrade-red.trx`.

Persisted loading now uses `ProfileValidator.ValidatePersistedProfile`: it reuses all existing checks and permits only the legacy HTTP(S) registry URL decorations at load. It preserves the raw URL, profile IDs, selected profile, registry user and encrypted vault bytes; it does not strip data or move credentials automatically. Create/edit/import/default validation and defensive export remain strict. Users can reach and repair the existing profile explicitly; the old credential-bearing URL cannot be exported. The mixed-profile test covers List/Get, unrelated Upsert/Delete, selection/reopen, rejected unsafe Upsert/export, explicit URL repair and unchanged retrievable dummy vault credentials. Other malformed metadata remains rejected. The exact isolated PR37 branch passed **8/8** cases.

Actual Composer commands also discarded untouched whitespace key/Subject projections before the mapper. `KafkaComposer_UneditedDraftPreservesRawKeySubjectAndNullValue` expected key [32] or [9], got null; `KafkaComposer_UneditedWhitespaceSubjectSurvivesWithNullKey` found no Subject header; `KafkaComposer_ExplicitWhitespaceEditsReplaceOriginalMetadata` expected a literal tab key, got null. Red log `kafka-composer-red.trx`: **8 failed, 12 controls passed**. The analogous MessageId path expected raw [32]/[9], got a generated UUID, including deliberate whitespace edits; `kafka-composer-message-id-red.trx`: **6 failed, 2 explicit-clear controls passed**.

Kafka source drafts now keep the original property projection when editor text is unchanged, including null/empty/whitespace; changed empty clears it, and changed whitespace remains literal. MessageId uses the same rule with the existing generated-ID fallback after explicit clear. Other providers retain their existing normalization. The **28 actual Composer command cases** cover raw null/empty/space/tab/binary key and Subject, tombstone versus empty values, independent Subject preservation, explicit clears, literal whitespace edits, and MessageId preservation/edit/clear. Together with the original Kafka cases, the exact isolated PR44 branch passed **38/38**. These command/store/mapper tests use fake sends and are not new broker roundtrip evidence.

Both late repairs were re-reviewed independently using gpt-6.1-sol high with no remaining actionable findings. Latest integrated Release validation passed **621 unit and 30 UI tests**; **53 broker cases skipped locally**. Logs: `late-review-full-validation.trx` in each test project, `late-branch-credential-export.trx`, `late-branch-kafka-envelope.trx`, and task-root `late-review-branch-validation.log`. Integrated code commit: `11375b1e43c64e7e1ae1ac3af86e95326873b814`.

### Second PR37 review: failed legacy edit rollback

The actual `EditEnvironmentCommand` reproduced the remaining P2 at preceding head `67b3d5bce136c47d04fdae074a6d72bd8e185f6c`: `LegacyRegistryEdit_VaultFailurePreservesPersistedProfileSelectionAndPassword` expected the original persisted profile, but reopening returned the repaired name/URL/configuration after vault Store failed. Strict rollback Upsert rejected the old legacy URL. The unchanged-code red run had **6 failures and 3 successful-edit controls** (three userinfo/query/fragment forms; failures before/after secret mutation). Log: `legacy-registry-edit-red.trx`. An initial control-only assertion expecting null instead of the UI's empty error string was corrected before the retained red run; it was a test expectation error, not a product defect.

Credentials now finish before metadata commit. `IAtomicProfileRepository.UpsertAndSelectAsync` uses the existing strict validation and a single atomic document write for profile metadata plus selected ID. Vault operations occur outside the repository's shared storage lock. If credentials or metadata fail, attempted connection-string/registry changes are restored with cancellation disabled; the original legacy metadata remains intact, so strict legacy rollback is unnecessary. Normal Upsert/import/export validation is unchanged. This handles exceptions; metadata and vault files are not jointly crash-atomic.

**22 new actual-command cases pass:** Store/Remove failure before and after mutation, cancellation after successful secret storage, real metadata-file access failure after successful secret storage, real encrypted-vault successful repairs, and a repair without credential changes. Failures assert original profile, byte-identical metadata, selected ID, unrelated profile and old password; strict legacy write/export rejection still holds. The exact isolated PR37 scope passed **31/31** including prior credential tests and the existing selection/connection-string rollback regression. Its uploaded Git tree `0625bfcb03f70434bf349810a818647434f34922` equals the locally tested tree. Log: `credential-rollback-review/tests/QueueLoom.Tests/TestResults/legacy-registry-edit-isolated-green.trx`.

Independent gpt-6.1-sol high read-only review found no actionable code issue. Latest integrated Release validation passed **643 unit and 30 UI tests**; **53 broker tests skipped locally**. Log: `legacy-registry-edit-full-green.trx` in each project. Integrated code commit: `33a18c084818ea8eb639f05341deae6841f4062c`. Only PR37 changed; PR44 and the six other draft heads retain their preceding successful CI results. Final PR37 run [36918223095](https://github.com/bitcodepro/QueueLoom/actions/runs/36918223095) completed successfully at exact head `8a526a188eb3228410bb56ac8cc1274d46860f40`: all three platform build/test jobs, all four packages and downloaded-package verification passed. Its [emulator job 110557432688](https://github.com/bitcodepro/QueueLoom/actions/runs/36918223095/job/110557432688) passed **51 integration tests and 3 broker UI tests with zero failures/skips**. All eight current draft heads have successful CI. Release/version jobs were skipped; no merge or release occurred.

### Current PR heads and CI

Checked at 2026-10-01 20:04:47 UTC. A successful preceding-head run is retained as history in the machine-readable evidence and is never attributed to a new head.

| PR | Current head | CI run | State |
| --- | --- | --- | --- |
| [#36](https://github.com/bitcodepro/QueueLoom/pull/36) | `9e64c1474dbdc9f49e3c832c0ecedf37d9d52fd5` | [36907859584](https://github.com/bitcodepro/QueueLoom/actions/runs/36907859584) | completed / success |
| [#37](https://github.com/bitcodepro/QueueLoom/pull/37) | `67b3d5bce136c47d04fdae074a6d72bd8e185f6c` | [36914079127](https://github.com/bitcodepro/QueueLoom/actions/runs/36914079127) | completed / success |
| [#38](https://github.com/bitcodepro/QueueLoom/pull/38) | `8309de1cf06486551c30eb3a513ccec2d44618c7` | [36908019485](https://github.com/bitcodepro/QueueLoom/actions/runs/36908019485) | completed / success |
| [#39](https://github.com/bitcodepro/QueueLoom/pull/39) | `197f4279ff556748802f27d6285c6c8daf05e65b` | [36908123721](https://github.com/bitcodepro/QueueLoom/actions/runs/36908123721) | completed / success |
| [#41](https://github.com/bitcodepro/QueueLoom/pull/41) | `292d4c6c4f2e7f49a1971046cf986d3506dfbe79` | [36908220797](https://github.com/bitcodepro/QueueLoom/actions/runs/36908220797) | completed / success |
| [#42](https://github.com/bitcodepro/QueueLoom/pull/42) | `a50b78207b0b186c2da8474290df6a99c4dca5af` | [36907724975](https://github.com/bitcodepro/QueueLoom/actions/runs/36907724975) | completed / success |
| [#43](https://github.com/bitcodepro/QueueLoom/pull/43) | `f6ae5bc2f98d38165844e92c4fdeca3b90c4d625` | [36908243546](https://github.com/bitcodepro/QueueLoom/actions/runs/36908243546) | completed / success |
| [#44](https://github.com/bitcodepro/QueueLoom/pull/44) | `03a7d1d51865ffae6c003a3eb322934945cdf755` | [36914173949](https://github.com/bitcodepro/QueueLoom/actions/runs/36914173949) | completed / success (attempt 2; infrastructure retry) |

PR44 attempt 1 passed Windows, Linux and macOS build/tests, all four packages and downloaded-package verification. Its emulator job [110543933812](https://github.com/bitcodepro/QueueLoom/actions/runs/36914173949/job/110543933812) failed in **Wait for the emulators**, before tests, because Docker could not obtain HTTP headers from `https://mcr.microsoft.com/v2/` within the timeout (exit 125). Only failed jobs were requested for retry. Attempt 2 completed successfully at the same head: [emulator job 110549471734](https://github.com/bitcodepro/QueueLoom/actions/runs/36914173949/job/110549471734) passed **51 integration tests and 3 broker UI tests, with zero failures or skips**. The first attempt was an infrastructure failure, not an assertion failure. That snapshot had eight successful draft-head CI results; PR37 was subsequently updated by the failed-edit repair below. Kafka broker tests in this run predate the audit.

All PRs remain drafts. Review #42 first, then independent fixes; #44 remains stacked on #39. No merge, release or live user-broker action was performed.

