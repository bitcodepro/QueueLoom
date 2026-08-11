# QueueLoom Security Policy

QueueLoom handles credentials and Azure Service Bus message content. Treat the application as a privileged operator tool: run it only on a trusted computer and grant its identity only the permissions it needs.

## Supported versions

There are no public stable releases yet. Security fixes are applied only to the current development branch; early builds receive no guaranteed backports. Before Production use, build a reviewed commit, lock dependency versions, and retain the artifact checksum.

## Reporting a vulnerability

Do not open a public issue or attach real credentials or Production payloads.

1. If the repository is hosted on GitHub and Private vulnerability reporting is enabled, open **Security → Report a vulnerability**.
2. Otherwise, contact a maintainer through the private channel listed by the hosting profile and first request a secure way to transmit details.
3. Include the version or commit, operating system, threat model, minimal reproduction steps, impact, and a possible remediation.
4. Use synthetic namespaces, keys, and messages. If a secret was exposed accidentally, revoke or rotate it before submitting the report.

The project does not currently offer a formal response SLA or bug bounty. Maintainers ask reporters to allow a reasonable period for validation and remediation before public disclosure.

## Protected assets

- namespace-level SAS connection strings and keys;
- Entra access and refresh tokens in process memory, plus credential or session caches owned by external identity providers;
- permission to read DLQs and send messages, especially in Production;
- message bodies, broker properties, and application properties that may contain personal or commercially sensitive data;
- namespace, queue, topic, and subscription names and local environment metadata;
- destination and content integrity during replay.

## Trust boundaries

```text
User/UI
    -> QueueLoom process
        -> local profile and encrypted-secret files
        -> DPAPI | macOS Keychain | Linux Secret Service
        -> Azure Identity tokens (interactive cache in memory only)
        -> external identity/session stores (browser, Azure CLI, and other providers)
        -> TLS/AMQP + HTTPS
            -> Azure Service Bus
```

The operating system, system credential store, Azure Identity, and Azure Service Bus are separate trust boundaries. Treat every message as untrusted input.

## Threat model

| Threat | Primary mitigation | Residual risk |
|---|---|---|
| Public source code or a Git clone | No keys in source; random per-installation master key | A secret committed by a user must still be revoked |
| Copying `secrets.v1.json` without the credential store | AES-256-GCM; the key is absent from the file | A complete OS-profile backup or migration can carry the key |
| Ciphertext tampering or moving an entry between profiles | GCM tag and AAD containing installation, profile, and secret IDs | Rollback of a complete, internally consistent file set has no monotonic counter |
| Another unprivileged local user | OS account isolation; Unix modes `0700`/`0600`; platform vault | Incorrect ACL or keyring configuration weakens protection |
| Compromise of the current user or process | Not solved by local encryption at rest | Malware, a debugger, keylogger, administrator, or root can obtain plaintext |
| Operator sends to the wrong destination | Separate profiles, forced `ReadOnly` for Production, temporary unlock, and typed destination confirmation | Local guards do not replace RBAC and approval controls |
| Send retry or timeout | Original DLQ message remains; workflow is treated as at least once | Duplicates or suppression by duplicate detection remain possible |
| Malicious or very large message | UI Peek batch of at most 10; body retention and preview capped at 1 MiB; no draft for truncated or oversized payloads; Base64 for binary data | The Azure SDK still receives the message body before QueueLoom limits long-term UI retention; process resources must still be controlled |
| Concurrent application instances | Shared exclusive `.storage.lock` and atomic file replacement | There is no transaction spanning both profile and secret; a malicious same-user process is outside this boundary |
| Leakage through logs, crash dumps, clipboard, or screenshots | Policy forbids logging secrets and payloads | The OS and third-party diagnostic tools can retain memory or screen content |
| Supply-chain dependency compromise | Locked package versions and the official NuGet source | CI scanning, an SBOM, and signature/checksum verification are still required |

Outside the current protection model: a fully compromised operating system or Azure tenant, a malicious administrator/root user, physical access to an unlocked session, hardware attacks, and any guarantee that a downstream consumer receives or successfully processes a message.

## Secret storage

Profiles and secrets are separated:

- `profiles.v1.json` contains only non-secret settings;
- `installation.id` is a random public installation identifier;
- `secrets.v1.json` contains AES-GCM nonce, ciphertext, and tag values;
- the 32-byte master key is stored through Windows DPAPI `CurrentUser`, macOS Keychain, or Linux Secret Service;
- on Windows, the protected DPAPI blob is stored in `vault-key.dpapi`, but it cannot reveal the master key without the Windows user profile;
- each entry uses a fresh 12-byte nonce, a 16-byte tag, and AAD `QueueLoom|secret-v1|installationId|profileId|kind`.

Every profile-repository and encrypted-vault read or modification is serialized across processes with the shared exclusive `.storage.lock` file; waiting is limited to 30 seconds. `profiles.v1.json`, `secrets.v1.json`, and the Windows `vault-key.dpapi` file are written to a temporary file and then atomically replaced. This reduces lost updates and partially written files when two QueueLoom instances run, but it does not provide one transaction spanning profile metadata and secret data.

Security does not depend on hiding the algorithm or on a machine fingerprint. Uniqueness comes from a cryptographically secure random master key. Copying only the source code or local data files to another computer must not make the values decryptable.

### Vault limitations

- A secret exists in process memory while connecting; managed strings cannot be guaranteed erased from every copy.
- Current-user OS protection does not defend against another process with the same permissions.
- `installation.id` prevents accidental mixing of vaults but is not hardware attestation.
- Losing the Keychain, keyring, or Windows profile means losing access to the saved connection string; there is no recovery key.
- A full copy or restoration of the OS credential store can make a backup decryptable. Protect backups at the operating-system level.
- Linux requires `secret-tool` and an unlocked Secret Service. There is no unsafe plaintext fallback.
- A process holding `.storage.lock` can delay another instance until timeout. The lock protects operation consistency; it does not deny access to another process running as the same user.

## Credential-handling rules

- Prefer Entra ID with MFA, Conditional Access, and short-lived tokens.
- Use a separate identity and the narrowest RBAC scope for Production.
- Do not use `RootManageSharedAccessKey`. Create a dedicated policy and rotate it regularly.
- Do not store connection strings in `appsettings*.json`, `.env`, shell history, CLI arguments, test snapshots, or source code.
- Do not send secrets to telemetry, exceptions, support bundles, or issue attachments.
- Do not implement a fallback that stores the master key beside the ciphertext or uses a shared default password.
- Do not synchronize the QueueLoom data directory through Git or a cloud folder. Transfer only non-secret profile information and re-enter credentials.
- After suspected exposure, immediately revoke or regenerate the SAS key or Entra credential, terminate active sessions, delete the vault entry, and create it again.

`InteractiveBrowserCredential` is constructed without persistent token-cache options; its token state exists only in process memory. Restarting requires a new token acquisition, and deleting a profile leaves no persistent QueueLoom interactive cache. Browser/Entra SSO sessions and caches owned by Azure CLI or other `DefaultAzureCredential` providers are outside QueueLoom, may persist after a profile is deleted, and must be cleared through the corresponding provider.

## Authorization and ReadOnly mode

`ProfileAccessMode.ReadOnly` blocks Send inside QueueLoom but does not reduce the identity's permissions. A modified client, compromised process, or another tool using the same credentials receives the full Azure permissions.

A Production profile is always persisted as `ReadOnly`. The UI can create only a temporary in-memory `ReadWrite` connection for 10 minutes after the environment name is entered; this change is not saved to the profile. The absolute deadline is checked after modal confirmation immediately before the SDK Send call, and an expired temporary connection is treated as read-only even before reconnection completes. Sending to Production requires another confirmation with the destination name. These guardrails reduce operator error but are not a security boundary.

`Azure Service Bus Data Owner` is the simplest role for the current namespace probe and topology discovery, but it is broader than a typical operator needs. For Production, create a custom role with read actions for namespaces, queues, topics, and subscriptions and only the required `receive` and `send` data actions. Separate read and write identities and make privileged write access temporary.

A SAS connection string must be namespace-level and have only the required `Manage`, `Listen`, and/or `Send` rights. Network restrictions, Private Endpoints, firewalls, and DNS are additional layers, not replacements for authorization.

## Message and DLQ safety

- Peek is not settlement: another consumer can change or remove a message between its display and a later action.
- The current UI requests at most 10 messages per Peek. The inspector retains no more than the first 1 MiB of the body. If the body is truncated or the body-plus-application-properties estimate exceeds 1 MiB, `Open as draft` is disabled. This reduces retention and editor exposure but does not guarantee that the Azure SDK avoided allocating the original body when receiving it.
- Current resubmission always creates a copy and leaves the original in the DLQ.
- When creating a copy, edited body and properties overlay the source values. The advanced editor exposes `To`, `ReplyTo`, `ReplyToSessionId`, `PartitionKey`, `TransactionPartitionKey`, and `ScheduledEnqueueTime`; verify their compatibility with the destination before Send. Broker-owned sequence, delivery, lock, and dead-letter metadata are not reproduced as controllable properties of the new message.
- Send and subsequent business processing are not atomic. A client timeout has an indeterminate outcome; retrying can create a duplicate.
- Review the destination, body, content type, `MessageId`, correlation, session, and partition properties, TTL, and schedule.
- When duplicate detection is enabled, retaining the old `MessageId` can suppress replay; a new ID changes downstream idempotency.
- Never automatically delete the original immediately after Send without a bounded PeekLock workflow, confirmation, an audit record, and a documented recovery procedure.
- Do not execute payloads, open embedded links automatically, or render a message body as active HTML or Markdown. A JSON viewer must display text, while binary data must use bounded Base64 or hexadecimal output.
- A partial DLQ scan displays the error on the affected source and only the known total. Do not interpret it as zero or as a successful complete check. When a scan is incomplete, the monitor raises an alert and retains the latest complete baseline.
- The `Selected queue / subscription` scope requires an explicit Explorer or Dead letters target and pins it when monitoring starts. Subsequent UI selection changes do not redirect the active check. Baselines are tracked separately by environment, entity, and subqueue so offsetting changes in an aggregate total cannot hide growth in one source.
- Never copy a Production payload into a bug report or unit test. Use a redacted synthetic example.

## Logging and diagnostics requirements

QueueLoom does not send runtime telemetry. The transitive build-time `AvaloniaStats` target is disabled in project files; this does not affect the XAML compiler or runtime framework.

Timestamp, operation type, profile ID, entity path, result category, and Azure request/correlation ID may be logged when organizational policy permits. By default, never log:

- connection strings, SAS signatures, authorization headers, or tokens;
- the plaintext master key, a nonce together with plaintext, or decrypted vault contents;
- message bodies or application properties;
- complete exception or HTTP dumps without redaction.

Crash dumps can contain decrypted credentials and payloads. Treat them as secrets, restrict access, and delete them after the investigation. Any future telemetry must be opt-in, documented, and must never contain secrets or messages.

The built-in `Activity` view shows at most 500 events from the current session. It is not persisted, tamper-resistant, or an audit log. Use an external approval and audit system with access control and a retention policy for Production replay.

## Release security checklist

- run `dotnet test` and security-focused tests;
- inspect dependency vulnerabilities and licenses from the restored lock/assets graph;
- create an SBOM and retain checksums;
- build from a clean commit using the official `nuget.org` source;
- do not package local application data, test credentials, caches from external identity providers, or diagnostic logs; QueueLoom's interactive-browser cache is memory-only;
- sign Windows and macOS artifacts, notarize the macOS bundle, and protect signing keys;
- include `LICENSE`, `THIRD-PARTY-NOTICES.md`, and the self-contained .NET runtime notices;
- verify installation and vault behavior on every supported operating system and architecture;
- test rollback, duplicate Send, and network timeout without destructive DLQ settlement.
