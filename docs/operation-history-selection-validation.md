# Operation history empty state and inspection validation

Baseline: v1.5.8 / `d3b13737f2e2cf2ae0fcdf29e1478e308b07d56a`, verified against remote main, tag and latest published release on 2026-10-02.

## Finding and change

A first launch, or an installation containing Activity records without durable resend/replay plans, showed a blank enabled operation picker. Clicking it opened an empty list without explaining what to do. The user confirmed that no operations had been performed: this is an empty-state presentation issue, not missing history data.

The panel now shows "No operation history yet" and names the existing actions that save outcomes: Resend on selected DLQ messages, Replay loaded messages, Restore filtered backups, and scheduled resends when they start. The picker, outcome list, detail view, recovery guidance and recovery buttons are hidden until records exist. Refresh operations remains available to discover saved plans from another window. A discovered plan becomes selectable; removal of the last plan restores the empty state. Ordinary Composer sends, scans and purges do not populate this list; persistence coverage is unchanged.

The ListBox now binds its inspected item to a read-only, scrollable TextBox containing the full description and outcome. Every outcome remains inspectable. Only Pending and Rejected items can be marked for recovery. Switching operations clears item inspection. Refresh retains the operation ID and item index when they still exist, loads updated outcomes, clears vanished selections and resets recovery marks.

## Regression flow

`OperationHistoryEmptyStateUiTests` uses the real MainWindow with isolated temporary stores. It covers fresh startup and a persisted Activity journal without plans, clicks Refresh operations while empty, discovers two published plans, opens the actual popup and selects an operation and its item through pointer input, then removes the fixture's plans and clicks Refresh to verify the empty state returns. Hidden controls, visible/enabled refresh, cleared details, binding warnings and no accidental Continue/Retry are verified. Three cases fail against the prior blank-picker UI and pass with the empty-state change.

`OperationHistorySelectionUiTests` opens the actual MainWindow with offline services and temporary durable operation snapshots. It uses Avalonia headless MouseMove/MouseDown/MouseUp at control coordinates and keyboard input, rather than setting SelectedItem or raising Click events.

- Open the actual ComboBox popup and click a different operation.
- Scroll to and click three separate outcome rows, then change selection with Arrow Up.
- Verify full, untruncated details for Pending, Rejected, Uncertain, Sent, Moved, DeleteUncertain and AwaitingScheduleClaim.
- Click recovery checkboxes and verify only eligible states are marked; row inspection never marks recovery by itself.
- Verify selection and marking do not invoke Continue or Retry, open confirmation dialogs or change persisted outcome states.
- Inspect with write access unavailable and recovery buttons disabled.
- Click Refresh operations: preserve the inspected item, show its updated outcome, reset marks, and clear details when an item, operation or all operations disappear.
- Check binding warnings and capture rendered PNGs.

The nine regression cases fail against the baseline production files at the missing detail view and pass with the fix. Baseline TRX, fixed TRX and rendered PNG evidence are retained beside the checkout in `selection-evidence`.

## Validation and limits

Locked restore, Release solution build and Release lab build use the retained .NET 10.0.401 SDK and NuGet cache. The full local unit and UI suites pass. Broker integration tests skip locally because no emulator endpoints are configured; the PR CI runs the existing five-emulator job and Windows, Linux and macOS builds/tests, all four packages, package smoke checks and archive checksums.

Windows self-contained single-file packaging uses the existing package action's publish options and scripts, including read-only MCP startup and update restart/cleanup smoke tests. No real broker resources are used by the new regression tests.

The supplied Library screenshot could not be materialized on this executor: the current Library transfer helper failed on missing Windows Python `os.setxattr`, including one fresh preparation and supported retry. Library image-file read returned a 266x105 image pointer, caption and OCR, but no pixels; downloading that reference failed authorization/resolution. No screenshot-content claim is made. The final empty-state scope is grounded in the user's explicit clarification and UI reproduction. Native desktop interaction is unavailable because this session exposes no Computer Use JavaScript runtime. Headless tests exercise the real Avalonia controls, popup, pointer hit testing and keyboard routing, but do not prove OS mouse delivery or native popup behavior.
