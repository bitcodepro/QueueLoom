# Update restart and cleanup validation

Baseline: `ec86a7000987689e73358efe37f6412bdc2588d3` (main and v1.5.3, verified on 2026-10-01).

The baseline launches the updated executable before calling `Close()`. It does not wait for shutdown or confirm that startup succeeded. `Close()` also follows the tray preference, so it can hide the previous application instead of exiting. The Update button itself finishes at the Ready stage and requires a second restart click. Cleanup deletes every `*.old` entry in the installation folder.

## Retained red/green regressions

The following tests were run against unchanged baseline `AppUpdater`, `MainWindow`, `Program`, and `UpdateDialogWindow` production code in a separate source export. The new test fixture and helper types were included only to compile the retained tests; the baseline production methods did not call the new helper.

| Regression | Baseline result | Fixed result |
| --- | --- | --- |
| `ProductionRestart_DoesNotLaunchWhileTheCallingProcessOwnsStorage` | Fails: the replacement is launched immediately and records startup blocked by the caller's exclusive storage lock | Passes: the helper accepts the handoff and waits for the exact parent process |
| `UpdateClick_AutomaticallyRequestsRestartAfterSuccessfulInstall` | Fails: clicking Update does not complete the dialog with Restart | Passes: successful installation requests restart automatically |
| `Cleanup_PreservesUnrelatedOldAndBackupFiles` | Fails: `user-notes.old` is deleted | Passes: unrelated `.old` and `.bak` files are preserved |

These demonstrate the restart ordering and cleanup defects. They do not identify the exact executable/version or shutdown delay on the reporting user's machine. The known baseline uses `.old`, whereas the user reported `.bak`; untracked backup files are deliberately retained because their ownership cannot be established.

## Handoff protocol

Installation records exact destination and backup paths in `.queueloom-update.json`, using a unique transaction ID. A previous receipt is never overwritten. Downloads have separate transaction directories so one installation cannot remove another installation's download.

The updated executable starts in `--update-helper` mode before shutdown. This mode runs without Avalonia or application storage. The helper acknowledges that it has read the receipt and accepted the handoff before the caller exits. The caller requests a real shutdown even when tray mode is enabled.

The helper waits up to two minutes for the parent PID and start time, then starts the updated GUI. Arguments use `ProcessStartInfo.ArgumentList`; executable and working-directory paths are not shell commands. Windows launch failures caused by transient locks are retried for up to 30 seconds. The main window acknowledges startup only after local data and its view model initialize successfully. The helper waits up to two minutes for this signal, then retries cleanup for up to 30 seconds.

A missing helper restores the previous files while the original caller stays open. A failed or timed-out GUI startup stops only the child created by the helper, restores the previous files, and starts the previous executable. Failed replacement files remain under transaction-specific `.failed` names while they may still be mapped by the helper, with a diagnostic beside the receipt. A subsequent successful startup of a version that understands the receipt retries their cleanup. An older recovered version may leave these diagnostics and failed files for manual inspection. A parent-exit timeout preserves both versions and never kills the parent. Cleanup failure after successful startup does not roll back a healthy GUI.

## Reproduction commands

Use the SDK pinned in `global.json`, also on `PATH` for tests that launch `dotnet` subprocesses.

```powershell
dotnet restore QueueLoom.slnx --locked-mode
dotnet build QueueLoom.slnx -c Release --no-restore
dotnet test QueueLoom.slnx -c Release --no-build --logger trx
```

The normal unit suite builds `QueueLoom.UpdateFixture` and exercises real processes in random temporary installation paths containing spaces. It covers delayed exit, parent timeout, startup exit/failure, startup timeout, a missing helper, exclusive executable locks, cleanup retries, rollback, unrelated-file preservation, and separate download ownership. The fixture does not open QueueLoom application data or cloud services.

The Windows packaging action also builds the fixture and runs:

```powershell
./.github/scripts/smoke-update.ps1
```

This uses the actual self-contained `artifacts/publish/QueueLoom.exe`, a disposable old process, a unique temporary installation, and explicit `QUEUELOOM_DATA_DIRECTORY`, `QUEUELOOM_BACKUP_DIRECTORY`, and bundle-extraction overrides. It verifies a running updated GUI, delayed-exit ordering, cleanup of its own backup/download/receipt, and preservation of unrelated files. Every process termination is limited to the fixture or the exact random installation path. The local packaged Windows smoke passed.

Local cross-platform execution is limited to Windows; Linux and macOS use the existing CI matrix. Emulator-dependent tests require the CI emulator job and skip locally when emulators are not configured. An early local full-suite attempt ran out of disk space; only this task's generated build output and downloaded archives were removed, then the suite was rerun successfully.
