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

Installation records exact destination and backup paths in `.queueloom-update.json`, using a unique transaction ID. A previous receipt is never overwritten. Install, helper recovery, and startup cleanup retain the same exclusive transaction lock through receipt access and file mutations. The receipt also records the installing process identity, so a competing startup cannot clean the installed files between installation and helper acceptance while that installer is still alive. Downloads have separate transaction directories so one installation cannot remove another installation's download.

The updated executable starts in `--update-helper` mode before shutdown. This mode runs without Avalonia or application storage. The helper acknowledges that it has read the receipt and accepted the handoff before the caller exits. The caller requests a real shutdown even when tray mode is enabled.

The helper waits up to two minutes before starting the updated GUI. Windows uses PID and exact start time to recognize the parent. Unix can reconstruct start time with small differences between processes, so the helper conservatively waits for any live process at that PID rather than treating a differing timestamp as proof of exit. Arguments use `ProcessStartInfo.ArgumentList`; executable and working-directory paths are not shell commands. Windows launch failures caused by transient locks are retried for up to 30 seconds. The main window acknowledges startup only after local data and its view model initialize successfully. The helper waits up to two minutes for this signal, then retries cleanup for up to 30 seconds.

A missing helper restores the previous files while the original caller stays open. A failed or timed-out GUI startup stops only the child created by the helper, restores the previous files, and starts the previous executable. Failed replacement files remain under transaction-specific `.failed` names while they may still be mapped by the helper, with a diagnostic beside the receipt. A subsequent successful startup of a version that understands the receipt retries their cleanup. An older recovered version may leave these diagnostics and failed files for manual inspection. A parent-exit timeout preserves both versions and never kills the parent. Cleanup runs outside the startup-failure recovery handler, so failure after successful startup cannot kill or roll back a healthy GUI. Diagnostic writes are best-effort and never gate rollback. If cleanup already removed its owned download directory but could not remove a locked receipt, the next startup treats the absent directory as cleaned. An existing directory still requires the exact transaction marker.

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

## Independent review recovery regressions

Review of the initial published snapshot found additional recovery and ownership defects. Eight new cases failed before their fixes: cleanup after an owned temp directory disappeared; cleanup after directory deletion with a locked receipt; healthy-start cleanup and failed-start recovery with either a locked diagnostic file or an unwritable diagnostic destination; installation while another transaction owner holds the lock; and competing startup cleanup while the installer remains alive. A ninth safety case, preserving an existing directory whose ownership marker is absent, passed before and after. All **31 updater unit/process cases** pass after these fixes. The rebuilt Release solution reports zero warnings/errors; all 31 UI tests pass and 51 emulator tests skip locally. The full-solution unit run was 571 passed and 1 failed out of 572 (the unchanged resend-status race); a separately retained complete unit rerun passed all 572. Both results are reported rather than hiding the intermittent failure. The Windows self-contained package was rebuilt after the review fixes and its isolated real GUI smoke passed. The two diagnostic destination variants include real Windows exclusive locks and portable unwritable-directory equivalents. File-only cleanup tests explicitly simulate the installer's exit; real process tests cover the actual parent-exit and helper handoff.
## Recorded evidence and limits

Three retained red regressions fail against unchanged baseline updater production code, as listed above. Separately, all **22 updater unit/process tests** passed locally, and both update-dialog UI tests passed. These are distinct from the real self-contained Windows GUI smoke, which also passed in an isolated installation. The baseline user-reported `.bak` files have not been inspected, and native GUI restart/cleanup on Linux/macOS has not been tested locally.

The requested complete unit rerun of the tested snapshot produced **562 passed and 1 failed out of 563**: the existing resend-status race in `ResendMarked_CopiesByDefaultAndKeepsTheOriginals`. A preceding full run failed its move counterpart. An isolated pass of the move test does not make the full suite green. Baseline comparison ran both resend tests ten times in isolation (20 passes) and the entire non-updater unit set three times (541 passes each); those ordinary baseline runs did not reproduce the intermittent failure. The resend implementation is unchanged in this PR, and the independent audit confirmed a late progress callback can overwrite the final status on unchanged baseline code. A separate diagnostic test in the baseline export deliberately deferred queued progress until after completion and failed deterministically: expected "Resend: 2 of 2 sent", actual "Sent 2 of 2". The baseline and updater resend source files have the same SHA-256 (D82A14903624A865226E8E94CABD674C41EC8A7E9403860F897DB71EC3585699). This diagnostic is retained in task evidence, not added to the updater PR. The audit task owns that separate fix.

Initial published head `bcb059febfaa37bc69e97d0f51d03b096c630d84` had tree `012f739b47bda984b0b36f6f13738a6e95bc492d`, exactly matching tested local commit `d7eb020770cb2460420d8719202e982b79b07971` and all 15 changed blob hashes. Its CI passed Windows and macOS build/tests, but Linux failed two updater wait tests and the baseline resend race; dependent package jobs, including hosted Windows GUI smoke, were skipped. The updater-specific follow-up conservatively waits for a live Unix parent PID even when reconstructed start times differ, and the existing Unix process test now forces such a timestamp difference. Exact-head CI must verify that follow-up; the initial CI result is not presented as a cross-platform pass.

The task was relocated to `E:\Temp\polymarket-hl-strategy\ServiceBusExplorer-Test\updater-20261001-d7eb020`. All 12,165 files (5,529,379,957 bytes), Git object integrity, exact head and clean working tree were verified before removing redundant task-owned C: copies. Temp, SDK home and NuGet caches are redirected only for this task; global settings and unrelated files were not changed.