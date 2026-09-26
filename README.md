# QueueLoom

Desktop client for Azure Service Bus queues, topics and dead-letter messages.

## Run

Download the Windows x64 ZIP from [Releases](../../releases), extract it and open `QueueLoom.exe`. No .NET installation is required.

1. Add a profile in **Environments** using a namespace connection string or Microsoft Entra ID.
2. Connect and select a queue or subscription in **Explorer**.
3. Use **View active** or **View DLQ** to inspect messages without locking them.

The last connected environment is selected on startup. DLQ browsing and search use one environment at a time.

## Features

- Browse and search active/dead-letter messages.
- Send text, JSON or Base64 messages with typed properties.
- Back up DLQ messages before purging, with scope review and a per-source limit.
- Restore backups or replay copies with rate limits and resumable progress.
- Monitor DLQ counts while the app is open and review local activity history.
- Light, dark or system theme; JSON bodies are highlighted and indented in the inspector.
- Test locally with the [Docker emulator lab](dev/emulator/README.md).

## Keyboard shortcuts

| Keys | Action |
|---|---|
| Ctrl+1 … Ctrl+8 | Open Overview, Explorer, Messages / DLQ, Backups, Composer, Monitors, Environments, Activity |
| Ctrl+F | Focus the current page's search or filter box |
| Ctrl+R | Refresh topology and counters |
| Ctrl+N | New message |
| Ctrl+Shift+F | Format the JSON body and jump to the first syntax error |
| Esc | Cancel the running operation |

Click an Explorer column header to sort by it; click **TYPE** to return to the topic hierarchy.

## Data and limits

Settings, backups and diagnostic logs (`logs\queueloom-yyyyMMdd.log`, kept 14 days, credentials redacted) normally live in `%LOCALAPPDATA%\QueueLoom`. Existing portable backups remain accessible. Override paths with `QUEUELOOM_DATA_DIRECTORY` or `QUEUELOOM_BACKUP_DIRECTORY`.

Connection strings are encrypted. Backups and replay snapshots contain message payloads in plain text/Base64. Production profiles require a temporary write unlock.

Peek displays up to 1,000 messages / 32 MiB of retained bodies. Replay sends copies; uncertain deliveries require investigation before retrying. Session-enabled message operations and queue/topic administration are not supported. Emulator counts are sampled; transfer DLQ is unavailable locally.

**1.0.0-rc.1 is a release candidate.** Real-Azure failure testing remains pending. The Windows package is unsigned.

## Build

Requires .NET SDK 10.

```powershell
dotnet restore QueueLoom.slnx
dotnet test QueueLoom.slnx -c Release   # unit tests and headless UI tests
dotnet publish src/QueueLoom.App/QueueLoom.App.csproj -p:PublishProfile=win-x64-single-file -o artifacts/publish
```

### CI and releases

- **Pull requests** build and test on Windows and Linux; the Windows build is attached to the workflow run.
- **Every merge to `main`** is tested again and then released automatically: the next version is taken from the latest `vX.Y.Z` tag, `QueueLoom.exe` is built with that version, and a GitHub Release with `QueueLoom-<version>-win-x64.zip`, its `.sha256` checksum and generated notes is published under the new tag.
  - The patch number is bumped by default (`1.0.0` → `1.0.1`).
  - Put `[minor]` or `[major]` in the merge commit message (the pull request title when squash-merging) for `1.1.0` or `2.0.0`.
  - Put `[skip release]` there to merge without releasing. Changes that only touch Markdown files, `LICENSE` or `dev/` do not release.
  - A release can also be started from **Actions → CI → Run workflow** on `main`, choosing the part to bump.
- **Pre-releases**: push a tag by hand, e.g. `git tag v1.2.0-rc.1 && git push origin v1.2.0-rc.1`.

`tests/QueueLoom.UiTests` drives the real window headlessly with in-memory data. Set `QUEUELOOM_SCREENSHOT_DIR` to also save screenshots of every page in both themes.

[MIT License](LICENSE) · [Third-party notices](THIRD-PARTY-NOTICES.md)
