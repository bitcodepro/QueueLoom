# Five findings: regression evidence

Baseline: `63c8ae18aa0fd0e24d0b0373783d0449323d8677` (main, v1.5.10).
Tests were added and executed before any production code changed. Local fixtures use temporary storage, AMQP interface fakes and independent child processes. Broker integration tests create a unique virtual host in the CI RabbitMQ container. No user broker or data is used.

| Finding | Regression | Red result on unchanged production code |
| --- | --- | --- |
| Retained RabbitMQ dead-letter attribution | `SourcePurge_LeavesForeignAndUnattributedMessages` | Deleted/reconfigured source cases expected 1 deletion, got 5; current shared-topology control passed. Fixtures include foreign, absent attribution, missing queue name and an older matching death after a newer foreign death. |
| RabbitMQ headers | `Headers_RoundTripUnchangedThroughDraftAndBackup` | Invalid UTF-8 `[255]` became `[239,191,189]`; two other malformed sequences changed too. Ordinary byte-text controls also exposed a separate string-header quoting defect: an ordinary string gained JSON quote bytes. Assertions verify draft resend and persisted backup restoration. |
| Expired management grants | `QueueManagement_ExpiryInsideDialogMakesZeroMutations`, `Routing_ExpiryInsideEditorOrConfirmationMakesZeroMutations` | All 3 queue and 3 routing cases recorded provider mutations after the expiry timestamp was set in an open dialog/editor/confirmation, while the provider still had read/write mode. Expected mutation lists were empty. |
| Settings transactions | `TwoProcesses_PreserveBothIndependentSettingsUpdates`, `ContendingStore_CancellationDoesNotRunTheUpdateOrOverwriteSettings` | Process A holds its read/modify transaction while B updates another setting: expected interval 321, got 60. A second store ran its callback instead of waiting and observing cancellation. |
| Malformed schemas | `InvalidHexNumbers_AreControlledSchemaErrors`, `DescriptorWithoutMessageName_IsAControlledSchemaError`, desktop import, real MCP startup and headless UI import cases | Missing descriptor name leaked `InvalidOperationException`; invalid/oversized hex leaked `FormatException`/`OverflowException`; bare `0x` leaked `ArgumentOutOfRangeException`. Desktop and UI imports escaped their handlers. MCP descriptor/overflow processes exited before initialization; the invalid-hex startup timed out. |

Local red results: `five-findings-red.trx` (26 failed, 1 passed) and `five-findings-ui-red.trx` (3 failed). Logs and TRX files are kept in the task evidence directory on E:, outside the repository.

The locked restore succeeded with the existing .NET 10.0.401 SDK and package cache on E:, with NuGet vulnerability checking enabled. Local Docker is unavailable; actual isolated broker execution is delegated to the existing GitHub Actions emulator job. Integration tests compile locally; compilation is not broker execution.

Green, full-suite, packaging, review, exact-commit CI and release evidence will be recorded after execution.
