# QueueLoom

QueueLoom is a cross-platform desktop client for inspecting and operating Azure Service Bus queues, topics, subscriptions, and dead-letter queues.

Built with .NET 10 and Avalonia UI 12.1.1. Licensed under the [MIT License](LICENSE).

> **Status:** QueueLoom 0.1.0 is an early preview. It is suitable for testing and controlled operator workflows, but it is not a replacement for Azure Monitor or a production audit system.

## Download

- [QueueLoom 0.1.0 for Windows 11 x64](../../releases/download/v0.1.0/QueueLoom-0.1.0-windows-11-x64-self-contained.zip)
- [SHA-256 checksum](../../releases/download/v0.1.0/QueueLoom-0.1.0-windows-11-x64-self-contained.zip.sha256)
- [Release notes](../../releases/tag/v0.1.0)

The Windows package is self-contained and does not require a separate .NET installation. It is not Authenticode-signed, so Windows may show an unknown-publisher warning.

Verify the downloaded archive in PowerShell:

```powershell
(Get-FileHash .\QueueLoom-0.1.0-windows-11-x64-self-contained.zip -Algorithm SHA256).Hash.ToLowerInvariant()
Get-Content .\QueueLoom-0.1.0-windows-11-x64-self-contained.zip.sha256
```

## Features

- Save multiple Development, Test, Production, and custom environments.
- Connect with a namespace-level connection string or Microsoft Entra ID.
- Use `DefaultAzureCredential`, interactive browser, Azure CLI, or managed identity authentication.
- Browse queues, topics, and subscriptions with runtime message counters.
- Peek active, dead-letter, and transfer dead-letter messages without settling them.
- Scan dead-letter counts in one entity, the current environment, or every saved environment.
- Compose new messages with text, JSON, or Base64 bodies and typed application properties.
- Open a peeked message as an editable draft and send a copy to a queue or topic.
- Monitor dead-letter counts on a timer from 15 seconds to 24 hours.
- Review the latest 500 actions in the in-memory Activity view.

## Safety

- Production profiles are always saved as read-only.
- Production write access requires a temporary 10-minute unlock and explicit confirmation.
- A dead-letter resubmission sends a new copy; the original message remains in the DLQ.
- Peeked message bodies retained by the inspector are limited to 1 MiB.
- A truncated or oversized message cannot be opened as an editable draft.
- QueueLoom does not purge, complete, defer, or automatically remove messages.

QueueLoom safety controls do not replace Azure RBAC or SAS permissions. Grant each identity only the access it requires.

## Secret storage

Environment metadata and credentials are stored separately. Connection strings are encrypted with AES-256-GCM using a random installation key protected by the operating system:

| Platform | Protected key store |
|---|---|
| Windows | DPAPI for the current user |
| macOS | User Keychain |
| Linux | Secret Service through `secret-tool` |

The encryption key is not derived from machine properties and is not stored in the source code. Cloning the repository does not provide access to saved credentials. Copying profiles to another computer does not create a portable credential backup.

See [SECURITY.md](SECURITY.md) for the full threat model and credential-handling guidance.

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
- Message settlement and destructive DLQ move operations are not implemented.
- Session, partitioning, and duplicate-detection scenarios require environment-specific testing.

## License and security

QueueLoom is available under the [MIT License](LICENSE). Third-party notices are listed in [THIRD-PARTY-NOTICES.md](THIRD-PARTY-NOTICES.md).

Report vulnerabilities privately according to [SECURITY.md](SECURITY.md). Never include connection strings, access tokens, or production message contents in a public issue.
