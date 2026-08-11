# QueueLoom

QueueLoom is a cross-platform desktop client for working safely with Azure Service Bus. It supports multiple environments, queue and topic discovery, message inspection, dead-letter queue (DLQ) monitoring, message composition, and safe resubmission by copying a message.

The project targets Windows, Linux, and macOS and uses [.NET 10](https://dotnet.microsoft.com/download/dotnet/10.0), [Avalonia UI 12.1.1](https://docs.avaloniaui.net/), `Azure.Messaging.ServiceBus` 7.20.2, and `Azure.Identity` 1.21.0 from the official [Azure SDK for .NET](https://github.com/Azure/azure-sdk-for-net). QueueLoom is distributed under the [MIT License](LICENSE), while the Avalonia base packages use [Avalonia's own MIT license](https://github.com/AvaloniaUI/Avalonia/blob/main/licence.md). Both licenses permit commercial use when their copyright and license notices are retained. Avalonia professional tooling and component tiers, including Avalonia XPF, are neither used nor required.

> **Status:** early working MVP. The desktop UI supports the primary workflows, from profile CRUD and connection management through Peek, DLQ scans, message composition, safe resubmission, and timer-based monitoring. The application does not yet have signed releases, an immutable audit log, or an operational track record. Do not use the current build as the sole production message-recovery tool.

## Download

- [QueueLoom 0.1.0 for Windows 11 x64 (self-contained ZIP)](../../releases/download/v0.1.0/QueueLoom-0.1.0-windows-11-x64-self-contained.zip)
- [SHA-256 checksum](../../releases/download/v0.1.0/QueueLoom-0.1.0-windows-11-x64-self-contained.zip.sha256)
- [Release notes](../../releases/tag/v0.1.0)

The package is self-contained and does not require a separately installed .NET Runtime. It is an unsigned preview, so Windows may display an unknown-publisher warning.

## Core principles

- **Safe by default:** a Production profile is always persisted as `ReadOnly`. Write access can be enabled only temporarily and after explicit confirmation. Access mode is configurable for other environments.
- **Entra ID before SAS:** where possible, use Microsoft Entra ID, MFA, and Azure RBAC instead of long-lived connection strings.
- **Secrets stay separate from settings:** a connection string is never written to a profile, source code, or repository configuration.
- **The algorithm is not a secret:** the cryptographic design is documented. Protection relies on a random key and the operating system credential store, not on hiding the implementation.
- **Safe replay:** a DLQ message is inspected with Peek, while resubmission creates a copy and does not delete the original.
- **The environment is always visible:** Development, Test, Production, or a custom label is part of every profile. Production is visually distinct and requires stricter write confirmation.

## Features and current readiness

| Area | Implemented behavior | Desktop UI |
|---|---|---|
| Environment profiles | Multiple Dev/Test/Prod/Custom profiles, add/edit/delete, and current-profile selection | Ready |
| Authentication | Connection string, `DefaultAzureCredential`, interactive browser, Azure CLI, and managed identity | Ready |
| Topology | Queues, topics, subscriptions, search, status, and runtime counters; five-minute cache and force refresh | Ready |
| Message browsing | Non-mutating Peek for active, DLQ, and transfer DLQ messages; the UI requests up to 10 messages at a time and retains no more than 1 MiB of each body for the inspector | Ready |
| DLQ discovery | DLQ/transfer-DLQ snapshots for one entity, the current profile, or all saved environments; deltas between scans and visible per-source errors | Ready |
| Message composition | Text, JSON, or Base64; core broker properties and typed application properties; strict validation | Ready |
| Sending | Send to a queue or topic; temporary write unlock, confirmation, and size validation through a Service Bus batch | Ready |
| DLQ resubmission | Edit a draft and send only a copy while preserving the original; supported broker properties are carried forward without silently truncating the body | Ready |
| Monitoring | Timer for all environments, the current environment, or `Selected queue / subscription`; the operator explicitly chooses an Explorer or DLQ target, and an alert is raised when any successfully measured source grows or a check is incomplete | Ready while the application is running |
| Secret protection | AES-256-GCM, a master key in DPAPI, macOS Keychain, or Linux Secret Service, plus a cross-process lock and atomic writes | Ready |

### Interface

- `Overview` shows the current namespace and summary counters.
- `Explorer` searches queues, topics, and subscriptions and performs Peek on active, DLQ, or transfer-DLQ messages.
- `Dead letters` combines scan results for the current environment or all environments, shows rows for sources with messages **or errors**, and opens messages for the selected source. A partial scan explicitly shows the error and only the known total; it never disguises an unavailable source as zero.
- `Composer` provides a text/JSON/Base64 body, core and advanced broker properties, typed application properties, destination selection, and draft size. For a peeked message, `Open as draft` is unavailable if the body was truncated or the estimated editable payload exceeds 1 MiB.
- `Monitors` provides `All environments`, `Current environment`, and `Selected queue / subscription` scopes with intervals from 15 seconds to 24 hours. Single-source mode requires an explicit `Explorer selection` or `Dead letters selection`, displays the pinned target, and does not follow subsequent UI selections.
- `Environments` adds, edits, and deletes profiles and selects their authentication method.
- `Activity` shows the latest 500 events from the current session. It is a diagnostic feed, not a durable audit log.

The connection state and `READ ONLY`/`WRITE ENABLED` mode remain visible at the top. Production is persisted as read-only. A temporary 10-minute unlock requires the environment name, and Send additionally requires the destination name. The deadline is checked again after confirmation immediately before the Azure SDK call, so leaving a confirmation dialog open does not extend the unlock.

## Architecture

```text
QueueLoom.App (Avalonia)
    |-- depends on QueueLoom.Core
    `-- depends on QueueLoom.Infrastructure
                              `-- depends on QueueLoom.Core

QueueLoom.Tests
    `-- tests Core and Infrastructure
```

- `src/QueueLoom.Core` contains domain types, interfaces, profiles, Service Bus/DLQ models, and validation. It does not depend on Avalonia or the Azure SDK.
- `src/QueueLoom.Infrastructure` contains the Azure Service Bus adapter, Entra credential construction, JSON persistence, and platform-specific secret vault.
- `src/QueueLoom.App` is the cross-platform Avalonia desktop shell and presentation layer.
- `tests/QueueLoom.Tests` contains xUnit tests for models, profiles, monitoring, messages, and infrastructure.

This separation allows dangerous send scenarios to be tested independently of the UI and allows storage or UI implementations to be replaced later without changing the domain model.

### Data flow

1. The application loads only non-secret profile metadata from `profiles.v1.json`.
2. When connecting, Infrastructure retrieves a connection string from the vault or creates a `TokenCredential` through Azure Identity.
3. `ServiceBusAdministrationClient` reads topology and runtime counters, while `ServiceBusClient` performs Peek and Send operations.
4. The UI works only with Core domain models and has no access to the master key.

## Environment profiles

A profile contains a GUID, display name, environment type, fully qualified namespace, authentication method, and local access mode. A connection string is stored separately and is never serialized into the profile.

Example namespaces:

```text
acme-orders-dev.servicebus.windows.net
acme-orders-test.servicebus.windows.net
acme-orders-prod.servicebus.windows.net
```

`ReadOnly` is an additional QueueLoom safety guard, not an Azure authorization mechanism. Effective permissions are always determined by Azure RBAC or the SAS policy. Do not grant an identity more rights merely because its QueueLoom profile is marked read-only.

### First run

1. Open `Environments`, add an environment, and provide a clear name, a Dev/Test/Prod/Custom type, and a sign-in method.
2. For Entra ID, enter the fully qualified namespace. For SAS, paste a namespace-level connection string. Once saved, it is placed in the secret vault rather than the profile metadata.
3. Select the profile and connect. QueueLoom validates the namespace and loads its topology.
4. In `Explorer`, select a queue or subscription and run Peek. A topic supports sending and exposes its subscriptions.
5. In `Dead letters`, scan the current environment or all environments. An all-environments scan connects profiles sequentially in read-only mode, then restores the selected environment. Rows with errors mean that only a known partial result is shown.
6. Open a DLQ source, select a message, and create a draft. If the 1 MiB safety limit is exceeded, the message remains a read-only preview and no draft is created. Review the destination and broker properties, including advanced routing, partition, and scheduling fields; temporarily unlock writes if necessary; then confirm Send.
7. In `Monitors`, choose a scope and interval. For `Selected queue / subscription`, explicitly choose an `Explorer selection` or `Dead letters selection`; the preview shows the entity that the monitor will pin. Monitoring runs only while the application is open and does not replace Azure Monitor.

## Secure connection-string storage

QueueLoom does not use a shared password, hardware fingerprint, source-code key, or secret algorithm.

Storage design:

1. A random `installation.id` (GUID) is created on first use. It is an identifier, not a secret.
2. An independent random 256-bit master key is generated.
3. The master key is entrusted to the platform-protected store:

   | Platform | Backend | Binding |
   |---|---|---|
   | Windows | DPAPI `CurrentUser` | The current Windows user and that user's profile on this computer |
   | macOS | Default user Keychain, service `io.queueloom.master-key` | The current Keychain account and installation ID |
   | Linux | Secret Service through `secret-tool` | The current desktop keyring and installation ID |

4. Every connection string is encrypted with AES-256-GCM using a fresh random 96-bit nonce and a 128-bit authentication tag.
5. Additional authenticated data binds the ciphertext to the schema version, installation ID, profile ID, and secret type. Moving an entry between profiles causes authentication to fail.
6. `profiles.v1.json`, `secrets.v1.json`, and the Windows `vault-key.dpapi` file are written to a temporary file and atomically replaced. Profile and vault operations are serialized across application instances by a shared exclusive `.storage.lock` with a wait of up to 30 seconds. This prevents concurrent overwrites but does not turn the profile and secret into one cross-file transaction. On Unix, the directory, lock, and files are restricted to the current user.
7. Plaintext and key buffers are cleared after an operation where practical.

The data root is selected through `.NET Environment.SpecialFolder.LocalApplicationData` under a `QueueLoom` subdirectory. Typical paths are:

- Windows: `%LOCALAPPDATA%\QueueLoom`;
- Linux: `${XDG_DATA_HOME:-$HOME/.local/share}/QueueLoom`;
- macOS: the current user's Local Application Data directory, normally `~/Library/Application Support/QueueLoom`.

`profiles.v1.json`, `secrets.v1.json`, and `installation.id` are not sufficient to decrypt stored values; the master key from the credential store is also required. Cloning or forking the repository therefore exposes the algorithm but not saved secrets. The random key is unique to an installation and is not derived from public computer properties.

This design does not protect against an administrator/root user, malware, a debugger, or a process already running as the same user. A full operating-system profile or Keychain migration can also transfer access. Encrypted secrets are intentionally not a portable backup format; enter the connection string again on another computer. See [SECURITY.md](SECURITY.md) for details.

## Authentication and Azure permissions

### Recommended: Microsoft Entra ID

Supported credential modes:

- `DefaultAzureCredential` without implicit interactive fallback;
- `InteractiveBrowserCredential` with a process-memory-only token cache;
- `AzureCliCredential` after `az login`;
- system-assigned or user-assigned `ManagedIdentityCredential`.

QueueLoom deliberately does not enable a persistent cache for interactive-browser authentication. A token must be obtained again after the process restarts, and deleting a profile does not leave a second persistent QueueLoom credential store. The browser sign-in session and caches owned by external providers, such as Azure CLI or sources used by `DefaultAzureCredential`, are outside QueueLoom and must be managed with those tools.

Use a namespace in the form `name.servicebus.windows.net`, assign roles at the narrowest practical scope, and prefer separate principals for Dev, Test, and Production. Microsoft recommends Entra ID instead of SAS, and local SAS authentication can be disabled entirely on a namespace. See [Authenticate an application with Microsoft Entra ID to access Azure Service Bus entities](https://learn.microsoft.com/azure/service-bus-messaging/authenticate-application) and the [built-in Azure Service Bus roles](https://learn.microsoft.com/azure/role-based-access-control/built-in-roles/integration#azure-service-bus-data-owner).

For the current implementation:

- `Azure Service Bus Data Receiver` grants receive/peek data access and read access to queues, topics, and subscriptions.
- `Azure Service Bus Data Sender` grants send data access and read access to entities.
- `Azure Service Bus Data Owner` reliably covers topology discovery, the namespace probe, Peek, and Send, but also grants substantially broader management permissions.

The current `ConnectAsync` implementation calls `GetNamespacePropertiesAsync`. Consequently, `Data Receiver` alone may be insufficient for the connection probe. Until a narrower probe is implemented, use either `Data Owner` at namespace scope or a custom role containing only the required namespace/queue/topic/subscription read operations and the applicable `receive`/`send` data actions. For Production, prefer a custom role and separate read/write identities rather than a permanently assigned `Data Owner` role.

RBAC assignments can take several minutes to propagate. DNS and network access, HTTPS to the management endpoint, and AMQP over TLS to the namespace must also be allowed.

### Connection string / SAS

QueueLoom accepts only a namespace-level connection string without `EntityPath` because it must enumerate all queues, topics, and subscriptions. Required SAS rights depend on the operation:

- `Manage` for discovery and runtime properties;
- `Listen` for Peek on active, DLQ, and transfer-DLQ messages;
- `Send` for new messages and replay copies.

The complete workflow requires all applicable rights. Create a dedicated policy instead of using `RootManageSharedAccessKey`, rotate keys regularly, and never pass a connection string through command-line arguments, environment variables, screenshots, or issue logs. See the [official SAS documentation](https://learn.microsoft.com/azure/service-bus-messaging/service-bus-sas) for the associated risks.

## Safe DLQ replay semantics

QueueLoom intentionally does not describe the current operation as moving a message:

1. The message is read through `PeekMessagesAsync`. Peek acquires no lock and does not modify the DLQ.
2. The user creates and edits a draft copy. The body, `MessageId`, `CorrelationId`, `ContentType`, `Subject`, `SessionId`, TTL, application properties, and advanced `To`, `ReplyTo`, `ReplyToSessionId`, `PartitionKey`, `TransactionPartitionKey`, and `ScheduledEnqueueTime` fields are available in the editor and overlay the source copy's properties.
3. A new message is sent after the destination has been reviewed.
4. The original remains in the DLQ. The MVP rejects `CompleteAfterSuccessfulSend` as unsupported.

Important consequences:

- The operation is not atomic and has **at-least-once** semantics. Retrying after a timeout can create a duplicate.
- A preserved `MessageId` can be suppressed by Azure duplicate detection. Before replay, decide whether the original or a new identifier is appropriate.
- Advanced `To`/`ReplyTo*` fields, partition keys, and `ScheduledEnqueueTime` can be incompatible with a new destination or unexpectedly change routing, partitioning, or send time. Review them before replay.
- Broker-owned fields such as sequence number, delivery count, enqueue/expiry timestamps, lock metadata, and dead-letter reason/description are not reproduced as controllable properties of the new message.
- A successful Send does not prove that a consumer processed the message.
- Delete the original manually only after business-level verification and with a separate audit trail.

The inspector/editor safety limit for peeked messages is a hard 1 MiB. Infrastructure retains no more than the first 1 MiB of the body for the UI and marks truncation. `Open as draft` is disabled if the source body was truncated or if the estimated editable payload—the body plus the UTF-8 representation of application-property names and values—exceeds 1 MiB. This prevents silently sending a truncated copy; the source message remains available only as a read-only preview. The limit does not replace validation of the actual new-message size through a Service Bus batch before Send.

A DLQ scan preserves rows for sources that returned an error and marks the aggregate result as partial. Such a total represents only the known messages. The monitor stores a baseline for each environment/entity/subqueue combination, so growth in one queue is not hidden by a decrease in another. When a check is incomplete, QueueLoom raises an alert and does not accept partial counts as a new baseline.

A future destructive replay would be acceptable only as a bounded PeekLock workflow: exact message identification, Send, settlement of the original after confirmed Send, duplicate protection, and an explicit warning that full atomicity is impossible. Cross-entity transactions are disabled in the current implementation.

## Build and run

### Requirements

- .NET SDK 10.x;
- Windows, macOS, or desktop Linux supported by Avalonia;
- access to Azure Service Bus and the selected identity provider;
- on Linux, the Avalonia X11 dependencies and, for the connection-string vault, `secret-tool` with an unlocked Secret Service keyring.

Example for Debian/Ubuntu:

```bash
sudo apt install libx11-6 libice6 libsm6 libfontconfig1 libsecret-tools
```

The Wayland backend in Avalonia 12.1 is a separate opt-in configuration. The current `UsePlatformDetect()` setup uses the standard desktop backend. See [Avalonia's supported-platform list](https://docs.avaloniaui.net/docs/supported-platforms) for current requirements.

### Restore, build, test, and run

From the `ServiceBusExplorer` directory:

```bash
dotnet restore QueueLoom.slnx
dotnet build QueueLoom.slnx -c Release --no-restore
dotnet test QueueLoom.slnx -c Release --no-build
dotnet run --project src/QueueLoom.App/QueueLoom.App.csproj
```

For debugging, replace `Release` with `Debug`. Restore uses only `nuget.org`, as configured in `NuGet.Config`.

## Publishing

A self-contained [Windows 11 x64 preview](../../releases/download/v0.1.0/QueueLoom-0.1.0-windows-11-x64-self-contained.zip) is published as a GitHub Release asset rather than committed to the repository. It does not require a separately installed .NET Runtime. Its [SHA-256 checksum](../../releases/download/v0.1.0/QueueLoom-0.1.0-windows-11-x64-self-contained.zip.sha256) is published alongside it. This is an unsigned preview without an Authenticode signature, not a stable release.

Verify the archive in PowerShell:

```powershell
(Get-FileHash artifacts/QueueLoom-0.1.0-windows-11-x64-self-contained.zip -Algorithm SHA256).Hash.ToLowerInvariant()
Get-Content artifacts/QueueLoom-0.1.0-windows-11-x64-self-contained.zip.sha256
```

### GitHub Release

Commit the source without the ignored `artifacts` directory, create the `v0.1.0` tag, and push both the branch and tag:

```powershell
git add .
git commit -m "Initial QueueLoom release"
git tag -a v0.1.0 -m "QueueLoom 0.1.0"
git push -u origin main
git push origin v0.1.0
```

On GitHub, open **Releases → Draft a new release**, select tag `v0.1.0`, use `QueueLoom 0.1.0 Preview` as the title, upload the ZIP and `.sha256` files from the local `artifacts` directory, select **This is a pre-release**, and publish it.

The same release can be created with GitHub CLI:

```powershell
gh release create v0.1.0 `
  artifacts/QueueLoom-0.1.0-windows-11-x64-self-contained.zip `
  artifacts/QueueLoom-0.1.0-windows-11-x64-self-contained.zip.sha256 `
  --title "QueueLoom 0.1.0 Preview" `
  --notes "First public preview of QueueLoom for Windows 11 x64." `
  --prerelease
```

The following commands create self-contained builds, so the user does not need to install the .NET Runtime separately. Publish each runtime identifier (RID) independently.

### Windows

```powershell
dotnet publish src/QueueLoom.App/QueueLoom.App.csproj -c Release -r win-x64 --self-contained true -p:PublishSingleFile=true -p:IncludeNativeLibrariesForSelfExtract=true -p:PublishTrimmed=false -o artifacts/win-x64
dotnet publish src/QueueLoom.App/QueueLoom.App.csproj -c Release -r win-arm64 --self-contained true -p:PublishSingleFile=true -o artifacts/win-arm64
```

### Linux

```bash
dotnet publish src/QueueLoom.App/QueueLoom.App.csproj -c Release -r linux-x64 --self-contained true -p:PublishSingleFile=true -o artifacts/linux-x64
dotnet publish src/QueueLoom.App/QueueLoom.App.csproj -c Release -r linux-arm64 --self-contained true -p:PublishSingleFile=true -o artifacts/linux-arm64
```

After extraction, run `chmod +x QueueLoom` if needed. System X11, fontconfig, and Secret Service libraries are not included in self-contained output.

### macOS

```bash
dotnet publish src/QueueLoom.App/QueueLoom.App.csproj -c Release -r osx-x64 --self-contained true -p:PublishSingleFile=true -o artifacts/osx-x64
dotnet publish src/QueueLoom.App/QueueLoom.App.csproj -c Release -r osx-arm64 --self-contained true -p:PublishSingleFile=true -o artifacts/osx-arm64
```

These commands create publish output, not a finished signed `.app`/`.dmg`. Public distribution requires an application bundle, entitlements when applicable, Developer ID signing, and notarization. Windows and Linux releases should likewise be signed and published with a checksum and SBOM.

Do not include `profiles.v1.json`, `secrets.v1.json`, `installation.id`, the DPAPI key blob, user logs, or caches from external identity tools in artifacts. QueueLoom's interactive-browser token cache exists only in process memory and creates no persistent file. Do not enable trimming or NativeAOT without dedicated Avalonia and Azure Identity testing. Include [LICENSE](LICENSE), [THIRD-PARTY-NOTICES.md](THIRD-PARTY-NOTICES.md), and the notices from the bundled .NET runtime with every release.

## Known MVP limitations

- There is no installer, automatic update, code signing/notarization, or official release binary.
- Only one active profile connection is supported at a time.
- Transport uses AMQP over TCP; WebSockets/proxy fallback is not configured.
- “All DLQs” means collecting runtime counters for queues and subscriptions, not full-text searching every message body.
- An all-environments scan and monitor switch the single active connection sequentially. A check can take noticeable time across many namespaces.
- Monitoring runs inside the desktop process. There is no background service, desktop notification, long-term history, or Azure Monitor integration.
- The current UI requests up to 10 messages per Peek action (Core allows at most 1,000 in one request). Peek is not a consistent snapshot while a queue is being processed concurrently.
- The inspector retains and displays no more than 1 MiB of a body. A truncated message or one above the editable limit cannot be opened as a draft.
- Session-enabled, partitioned, and duplicate-detection scenarios require dedicated end-to-end testing.
- There are no purge, receive-and-delete, complete, defer, or dead-letter actions and no atomic DLQ move.
- Queues, topics, and subscriptions cannot be created, modified, or deleted.
- Local profiles and secrets are not synchronized between computers; secure export/import is unavailable.
- `Activity` exists only in memory for the current session and is limited to 500 rows. For Production, record approvals and replay results in an external immutable system.
- Azure Service Bus Emulator and sovereign-cloud suffixes are not currently declared supported.

## License and security

QueueLoom is distributed under the [MIT License](LICENSE). Third-party component notices are listed in [THIRD-PARTY-NOTICES.md](THIRD-PARTY-NOTICES.md).

Report vulnerabilities privately according to [SECURITY.md](SECURITY.md). Never publish connection strings, access tokens, or Production message content in a public issue.
