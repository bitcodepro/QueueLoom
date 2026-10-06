# Stable launcher protocol v1

The normal launch path is a separate self-contained launcher. Automatic updates never rename, overwrite,
or delete that entry or its immutable bootstrap. The application runs from a verified version directory.
Preferences, credentials and Activity stay in the existing user data location. Default program backups
stay beside the stable installation, with the existing user data fallback for read-only locations.
Explicit backup overrides inside version directories are relocated beside the stable installation.

## Portable packages and first migration

Windows and Linux packages contain `QueueLoom(.exe)`, `QueueLoom.bootstrap.json`, and
`QueueLoom.bootstrap.zip`. The archive contains a manifest and the initial application payload.
This keeps migration compatible with the legacy installer, which copies only top-level package files.
On first launch the archive is verified, extracted to a private staging directory, verified again, and
published under `QueueLoom.versions/versions/<id>`.

The macOS package is one movable `QueueLoom.app`: the outer launcher bundle contains the initial signed
application at `Contents/Resources/initial/payload/QueueLoom.app`. Move the entire outer bundle to
Applications as before. Later payloads and state live in `QueueLoom.versions` beside that outer bundle;
automatic updates leave the sealed outer bundle intact. The package keeps the existing ad-hoc signing;
it adds no publisher certificate or Apple notarization.

A fresh installation in a read-only location uses
`<user-data>/launcher-installations/<hash-of-launcher-path>` instead of an adjacent store. An existing
store stays authoritative when permissions change. Moving a read-only installation changes its
identity and starts from its packaged bootstrap; it does not select another installation's versions.
The existing updater policy still requires a writable program directory for automatic updates.

The **first upgrade from a legacy executable still uses that legacy controller**. A new launcher cannot
retroactively remove the replacement gap in old code already running. The launcher forwards legacy
helper/startup arguments and preserves the old PID readiness handshake for this transition.

## Activation and recovery

1. Acquire the installation lock. Verify the downloaded payload archive against its descriptor.
2. Copy/extract into `staging/<id>`, flush files, verify the manifest digest, full file list, permissions
   and platform. Reject filesystem links, traversal and unsupported protocol versions.
3. Publish a new immutable `versions/<id>` directory, leaving the launcher and previous payload in place.
4. Append a checksummed activation record to `state/<20-digit-sequence>.json`. Its active and previous
   references pin manifest hashes. Each record is flushed before publication under a new name.
5. The first candidate launch records a unique attempt plus owner PID/start time. After local startup,
   the application publishes an acknowledgement bound to that version, manifest and attempt.
6. A valid acknowledgement confirms the candidate. A dead owner without acknowledgement, or a missing
   or changed active payload, selects a verified previous payload or the immutable bootstrap.

Malformed newest records are ignored in favor of earlier confirmed history or bootstrap. Missing records
never cause an executable-directory scan. Checksums detect corruption; they are not signatures and do
not defend against an attacker able to rewrite the entire installation under the same user account.
Downloaded release checksums retain their existing verification; GitHub attestations remain separate.

The launcher forwards caller arguments and standard streams, returns the payload exit code, and writes
diagnostics only to stderr. A pending MCP payload acknowledges after its host starts, before the launcher
forwards the client's first input or exposes protocol output. Startup failure can therefore fall back
without consuming that request. Generated MCP configuration and update restarts use the stable entry.
Concurrent launchers wait for the attempt owner to confirm or recover before starting another payload.
Repeating the already accepted acknowledgement for the same confirmed version is harmless; another
candidate or an unknown acknowledgement token is rejected.

## Startup verification cost

Each launch checks the selected version's pinned manifest, complete file list, filesystem links,
permissions and **all content hashes**, including dependencies. Length/last-write-time stamps are not
trusted as a content cache: corruption can preserve both, and an executable-only hash would miss changed
native libraries. The self-contained executable dominates the bytes read, so even the suggested
executable hash alone still has a measurable cost. Payload-context validation inside the app also verifies
its identity; timings below measure version selection, not total application or MCP initialization.

For an installed version, the launcher snapshots activation state under the installation lock, releases
that lock while verifying immutable retained files, and then reacquires it to compare the current state.
If activation, acknowledgement or recovery changed the snapshot, it verifies the new selection before
making a decision. Normal launches read only the selected payload. An unselected bootstrap is verified
when staging or recovery needs it, rather than hashed again on each confirmed start. Bootstrap publication,
activation, first candidate launch, acknowledgement and recovery retain full integrity validation.

Warm-cache Windows measurements on synthetic, actually written payloads of 80/160 MiB (64/128 MiB
executable plus 16/32 MiB dependency) measured five sequential selections and eight simultaneous callers:

| Selected payload | Previous sequential median | Current sequential median | Previous eight-call wall time | Current eight-call wall time |
| --- | ---: | ---: | ---: | ---: |
| Bootstrap, 80 MiB | 761 ms | 378 ms | 6.59 s | 0.74 s |
| Confirmed update, 80 MiB | 819 ms | 380 ms | 6.74 s | 0.78 s |
| Bootstrap, 160 MiB | 1652 ms | 752 ms | 13.74 s | 1.52 s |
| Confirmed update, 160 MiB | 1542 ms | 749 ms | 12.23 s | 1.36 s |

A native Windows MCP comparison used the **same packaged 129.5 MiB executable payload** with the old
and corrected launcher entries in separate synthetic installations. After bootstrap/bundle-cache warm-up,
the median of three initialize replies was 3.58 s versus 2.94 s; the last of eight simultaneous replies
arrived at 13.43 s versus 4.98 s. Initial extraction/startup measured 4.58 s versus 4.85 s in those runs
and is not an asserted improvement. Runtime initialization, payload-context verification and contention
remain additional costs beyond the selection-only table.

These are local E: filesystem measurements, not cold-reboot, Defender-specific or slow-disk guarantees.
Disk/cache/CPU contention can increase costs, and initial archive extraction is additional work. Full hashing
is intentional to retain detection of changed bytes; the optimization removes the extra bootstrap read
and serialized hash critical section instead of weakening integrity. The 30-second installation-lock wait
still applies to state publication/staging, but concurrent normal payload hash reads do not hold that lock.

## Retention, durability and launcher maintenance

Protocol v1 retains **all committed versions and state records**, including failed candidates. Automatic
cleanup removes only incomplete staging directories with their matching ownership marker. Storage grows
with updates; automatic version garbage collection is deliberately deferred. Keep the launcher, bootstrap,
store, and adjacent backups together when moving a writable portable installation. Close all running
instances before moving it. Backup overrides beneath the store are relocated outside replaceable payloads;
explicitly putting the main user data directory inside a payload is unsupported.

File contents are flushed before publication. Windows publication uses a new-name write-through rename;
Unix publication synchronizes affected directories. Restart tests terminate isolated processes at named
preparation/activation/acknowledgement/cleanup boundaries and corrupt/delete state and payload files.
These establish process-interruption recovery, **not hardware power-cut durability**. Actual persistence
depends on the filesystem, operating system, storage controller and hardware; native CI checks cannot
prove those guarantees. Network filesystems and unusual mount/link layouts are outside the tested setup.

A package requiring a newer launcher protocol is rejected before activation. Update the launcher/runtime
by extracting a **new portable installation side by side**, starting it, and switching shortcuts/MCP
configuration to its stable entry. Do not overwrite a running launcher in place. Automatic updates within
protocol v1 only update payloads; this avoids introducing a second automatic executable-replacement gap.
