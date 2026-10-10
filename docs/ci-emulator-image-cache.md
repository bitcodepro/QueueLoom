# CI runtime emulator image cache

The Linux emulator job uses the same six prebuilt images as green main CI
[38043517980](https://github.com/bitcodepro/QueueLoom/actions/runs/38043517980)
at `1bc309441d027a0b3b59ae3e7a71f5102c1c25bf`. Their original pulled digests
are recorded as `source_digest` in `.github/emulator-images.lock.json`.
The lock resolves each index to its exact **linux/amd64** manifest, config ID
and rootfs identities. This freezes already-tested bytes; it does not silently
upgrade the moving tags.

| Emulator | Existing CI source tag |
| --- | --- |
| LocalStack | `localstack/localstack:4.14` |
| RabbitMQ | `rabbitmq:4-management` |
| Kafka | `apache/kafka:3.9.2` |
| Pub/Sub | `gcr.io/google.com/cloudsdktool/google-cloud-cli:emulators` |
| SQL Server | `mcr.microsoft.com/mssql/server:2022-latest` |
| Service Bus | `mcr.microsoft.com/azure-messaging/servicebus-emulator:latest` |

Public config version environment variables and selected labels are retained
in the lock as refresh evidence. Inherited OCI version labels can describe a
base OS rather than the emulator; digest/config identity is authoritative.
Existing broker integration and UI tests remain the behavioral version check.

## Restore and identity checks

The exact cache key is
`queueloom-runtime-images-v1-<runner OS>-<runner architecture>-<lock hash>`.
Application commits are not part of the key and there are no prefix restore
keys. Before the existing container steps, `actions/cache/restore` attempts to
restore a dedicated directory under `runner.temp` containing only `images.tar`
and `metadata.json`. Docker credentials, configuration, containers and
application build output are not cached. This is runtime image caching,
independent of BuildKit.

The helper checks format/platform/lock identity, the archive size and checksum,
the exact six archive tags and config contents before `docker load`. After load,
every digest-derived `localhost/queueloom-ci/*` tag must have the locked config
ID, `linux/amd64` platform and rootfs layer identities. Docker save/load can lose
`RepoDigests` on the classic image store, so these local tags and config IDs are
used instead of assuming the original digest reference survived. All six
container starts use `--pull=never` and the verified local tags.
Archive members must be regular files or their parent directories, with safe,
unique paths. Docker's legacy repositories and OCI index must describe only
the six locked tags; arbitrary sidecars, links and directories are rejected.
On a verified restore that will not save an entry, the tar and metadata are
removed before starting containers to reduce duplicate disk usage.

A cache miss, restore action failure, corrupt archive, failed load, wrong ID,
wrong platform or wrong rootfs falls back to six digest-pinned registry pulls.
Pulled identities are verified before tagging/running. Registry pull or identity
failure fails the job; tests are never silently skipped. Cache save failure
leaves verified runtime images usable and does not fail application tests.

## Trusted writes and bounds

Only successful `push`/`workflow_dispatch` runs on `refs/heads/main` save a new
GitHub cache, after the existing emulator tests pass. PRs restore and pull on a
miss; they do not write a GitHub cache or create a local save archive on a miss.
The save action runs only on an exact-key miss. Cache entries are immutable:
an invalid exact-key hit is pulled around, not overwritten or deleted. Repair
that situation by reviewing a format/key bump; this workflow never performs
cache cleanup or changes repository/account/billing settings.

The uncompressed Docker archive cap is **8 GiB**. Above that cap the archive is
discarded and caching is skipped; verified images still run. GitHub separately
compresses its cache upload, and hosted runners still download those bytes.
The [default repository cache limit](https://docs.github.com/en/actions/reference/workflows-and-actions/dependency-caching#usage-limits-and-eviction-policy)
is 10 GB; changing a lock creates another entry and normal GitHub eviction
applies. Existing limits and retention settings are not changed.

## Refresh

Use Python 3; these scripts require only the standard library. Verify immutable
registry metadata without changing the lock:

```sh
python3 -B .github/scripts/refresh-emulator-images.py --check
```

To deliberately resolve the existing source tags again:

```sh
python3 -B .github/scripts/refresh-emulator-images.py --refresh
git diff -- .github/emulator-images.lock.json
```

Review every manifest/config/version change and run the complete emulator CI
before merging. Changing a source version is an explicit edit of `SOURCES` in
the refresh script plus a reviewed lock refresh. Initial migration used the
six original digests from the linked green CI, not today's moving tag values.

## Measurements and bounded validation

`runtime-image-metrics.json` and the job summary record preparation, pull,
validation, Docker load/save durations and archive bytes. The GitHub restore
action is a separate timed step; its job timestamps include network download
and action extraction. It must be included when comparing total preparation
cost. The old CI startup step alone is not the total cost of all six pulls:
Service Bus was pulled later in the readiness step.

An explicit `workflow_dispatch` with `image_cache_benchmark=true` adds one
20-minute-bounded disposable Linux job. It pulls exactly the six pinned images,
saves and validates an archive, removes only its own six image references,
verifies those images are no longer resident, loads/validates the local archive
with zero pulls, and checks corrupt-cache fallback. It writes **no GitHub
cache**, prunes no other images, and uploads a small JSON report.

That local archive load is not a warm GitHub cache download measurement. Before
merge, main has not saved this new key, so a true warm PR restore cannot be
claimed. A safe post-merge check is: let one green main run save the bounded
entry, then rerun one PR emulator job on the same lock and record both the
restore step and load/validation timings, confirming `mode=archive` and
`pull_count=0`. No broader cache writes or changes to permissions are required.
Tests are not claimed to run faster without these measurements.
