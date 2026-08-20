# QueueLoom

QueueLoom is a cross-platform desktop client for inspecting and operating Azure Service Bus queues, topics, subscriptions, and dead-letter queues.

Built with .NET 10 and Avalonia UI 12.1.1. Licensed under the [MIT License](LICENSE).

> **Status:** QueueLoom 0.2.5 is an early preview. It is suitable for testing and controlled operator workflows, but it is not a replacement for Azure Monitor or a production audit system.

## Download

- [QueueLoom 0.2.5 for Windows 11 x64](../../releases/download/v0.2.5/QueueLoom-0.2.5-windows-11-x64-self-contained.zip)
- [SHA-256 checksum](../../releases/download/v0.2.5/QueueLoom-0.2.5-windows-11-x64-self-contained.zip.sha256)
- [Release notes](../../releases/tag/v0.2.5)

The Windows package is self-contained and does not require a separate .NET installation. It is not Authenticode-signed, so Windows may show an unknown-publisher warning.

Verify the downloaded archive in PowerShell:

```powershell
(Get-FileHash .\QueueLoom-0.2.5-windows-11-x64-self-contained.zip -Algorithm SHA256).Hash.ToLowerInvariant()
Get-Content .\QueueLoom-0.2.5-windows-11-x64-self-contained.zip.sha256
```

## Features

- Save multiple Development, Test, Production, and custom environments.
- Connect with a namespace-level connection string or Microsoft Entra ID.
- Use `DefaultAzureCredential`, interactive browser, Azure CLI, or managed identity authentication.
- Browse queues, topics, and subscriptions with runtime message counters.
- Peek active, dead-letter, and transfer dead-letter messages without settling them; DLQ results are paged and displayed oldest first.
- Copy a message body or its complete broker, runtime, and application-property view from the inspector.
- Scan dead-letter counts in one entity or every saved environment, then filter global results by environment.
- Search dead letters across the selected environment scope by Correlation ID, Message ID, subject, body, or application property and view matches as an oldest-first timeline.
- Back up messages as full local JSON files and then purge both DLQs for one queue/subscription, every subscription under a topic, or one connected environment.
- Compose new messages with text, JSON, or Base64 bodies and typed application properties.
- Open a peeked message as an editable draft and send a copy to a queue or topic.
- Monitor dead-letter counts on a timer from 15 seconds to 24 hours.
- Restore the last monitor interval from local application settings on the next launch.
- Review the latest 500 actions in the in-memory Activity view.
- Check GitHub tags at startup and offer to open the QueueLoom GitHub page when a newer version is available.

## Security and safety

- Connection strings are kept outside environment profiles and encrypted with AES-256-GCM.
- The random encryption key is protected by Windows DPAPI, macOS Keychain, or Linux Secret Service. It is not stored in the source code.
- Production profiles are saved as read-only. Sending or purging requires a temporary 10-minute write unlock.
- Peek does not settle messages. Resubmitting a dead-letter message sends a new copy and leaves the original in the DLQ.
- The inspector retains at most 1 MiB of a peeked body, and truncated messages cannot be opened as editable drafts.
- DLQ search skips empty sources, searches up to 12 non-empty sources concurrently, inspects up to 1,000 messages per source, returns up to 500 matches, and clearly marks limits or timeouts.
- Purge starts immediately when a backup-and-purge button is clicked. Only non-empty sources from the latest scan are processed, one DLQ at a time, in bounded batches of 10. Every message in a batch is backed up before any message in that batch is settled.
- Backups are stored under the operating system's local application-data directory in `QueueLoom/backups`, grouped by UTC date, environment, entity, and DLQ type.
- Backup JSON files contain full message bodies and properties in plain text/Base64. Protect and remove them according to your data-retention policy.

These safeguards do not replace Azure RBAC or SAS permissions. Use least-privilege credentials and review every destination before sending.

## First run

1. Open `Environments` and create a profile.
2. Enter an Azure Service Bus namespace such as `example.servicebus.windows.net`.
3. Select an Entra ID method or provide a namespace-level connection string without `EntityPath`.
4. Connect and select a queue, topic, or subscription in `Explorer`.
5. Use Peek, scan dead letters, compose a message, or start a monitor.

Microsoft Entra ID is recommended for normal use. QueueLoom currently performs namespace and topology discovery, so the selected identity must have the corresponding read permissions in addition to any required receive or send data actions.

## Build from source

Requirements:

- .NET SDK 10.x;
- Windows, macOS, or desktop Linux supported by Avalonia;
- access to an Azure Service Bus namespace;
- `secret-tool` and an unlocked Secret Service keyring when storing connection strings on Linux.

```bash
dotnet restore QueueLoom.slnx
dotnet build QueueLoom.slnx -c Release --no-restore
dotnet test QueueLoom.slnx -c Release --no-build
dotnet run --project src/QueueLoom.App/QueueLoom.App.csproj
```

## Project structure

- `src/QueueLoom.App` — Avalonia desktop UI.
- `src/QueueLoom.Core` — domain models, contracts, and validation.
- `src/QueueLoom.Infrastructure` — Azure Service Bus integration and secure persistence.
- `tests/QueueLoom.Tests` — Core and Infrastructure tests.

## Current limitations

- The published preview is available only for Windows 11 x64.
- Only one environment connection is active at a time.
- Monitoring runs only while the desktop application is open.
- There are no desktop notifications, automatic updates, or persistent audit history.
- QueueLoom cannot create, edit, or delete queues, topics, or subscriptions.
- Automatic restore from local purge backups is not implemented; backup JSON files can be inspected or used to reconstruct messages manually.
- Session, partitioning, and duplicate-detection scenarios require environment-specific testing.

## License

QueueLoom is open-source software available under the [MIT License](LICENSE). It is provided **as is**, without warranty; use it at your own risk. Third-party notices are listed in [THIRD-PARTY-NOTICES.md](THIRD-PARTY-NOTICES.md).
