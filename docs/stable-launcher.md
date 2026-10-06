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
