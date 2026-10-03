# Message comparison audit: incomplete previews and hidden property differences

This is cycle 2, the final one of the two requested audits after PR #60. The baseline is current `main` at `dbfce648512b65048adfd9f614facf6825ea2746`, the squash merge of PR #61. This bounded audit read comparison, consumer metrics, schema decoding, retention, reason grouping and dead-letter backup metadata/cache paths. Confirmed changes are confined to message comparison; this is not a claim that every repository path is defect-free.

The comparison dialog could claim that two messages were identical when:

- Their first 4,000 displayed body lines matched but their unexamined tails differed. Both UTF-8 text and binary previews reproduce this. `AreEqual` now requires a complete body comparison, and an identical truncated prefix is explicitly described as incomplete. The existing preview limit remains in place.
- Only `ReplyToSessionId`, `TransactionPartitionKey`, `ScheduledEnqueueTime`, `AmqpType` or `AmqpAppId` differed. These editable broker fields were absent from the comparison. All five are now listed; scheduled enqueue timestamps preserve fractional seconds.
- An application property changed type while retaining the same textual value, such as String `42` versus Int32 `42`. Values now include their type.
- An application property named `Message ID` overwrote the broker property's comparison row. Application rows now have an `Application: ` prefix, so broker and application properties remain distinct.

The test-only unit commit `3690fdc869ffb7827ed51508867b68525a8f2e46` preserves the unchanged-product reproductions: 10 failing cases and one passing control at the exact 4,000-line boundary. Test-only UI commit `7f51054` preserves two failing real-window cases: the incomplete body summary and the property tab showing broker IDs/application types. No assertion was suppressed. After the product fix, all 13 comparison unit cases and three dialog UI cases pass, including existing coverage. The UI regressions assert the visible summary and values and check for binding errors.

Full Windows validation with portable SDK 10.0.401 passes 1,098 unit tests and 71 UI tests, with zero skips in those suites. Locked restores and Release builds of the solution and Lab succeed; both builds report zero warnings and zero errors. The integration suite was invoked and reports 65 explicitly skipped tests because broker emulators are not configured locally. Cross-platform tests, emulator integration, native rendering and downloaded-package verification remain required on the exact PR head in CI.

All SDK/cache/profile/temp use, logs and TRX remain under `E:\Temp\polymarket-hl-strategy\ServiceBusExplorer-Test`. Cycle 2 evidence is in `post-pr60-cycle2-20261003/evidence`; previous artifacts and user changes are preserved. GitHub access uses the user-approved `bitcodepro` account. No merge, tag or release publication is performed by this local cycle.

## Release handoff

The user requested a new release containing both audit cycles. After independent review and clean exact-head PR CI, the parent should squash-merge this PR and verify the resulting main workflow and release. Under `.github/workflows/ci.yml`, successful main build/test and emulator jobs compute the next stable patch version, pass it to the reusable package workflow and publish the release for the merge SHA. `Directory.Build.props` is a development default; packaging stamps the computed version via `-p:Version`, so no source-version bump is required. Keep the squash title free of `[skip release]`, `[minor]` and `[major]` for the normal patch release.

At the 2026-10-03 remote-tag check, the latest stable tag was `v1.5.19`, making `v1.5.20` the next patch candidate. Recompute at merge time if another release has completed. Do not publish the PR's `0.0.0-pr.*` packages as the stable release.

Verify the actual downloadable release contains exactly four versioned archives and their four SHA-256 files: `win-x64.zip`, `linux-x64.tar.gz`, `osx-arm64.zip` and `osx-x64.zip`. Run `.github/scripts/verify-packages.ps1` against the downloaded assets on E: to verify hashes, archive readability and required executable/license/readme content. Verify the tag targets the squash merge SHA and that package native-rendering jobs passed. The parent owns final release publication/verification; no further audit cycle is requested.
