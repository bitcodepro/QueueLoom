# Release policy

Automatic releases still run in CI after a main push: three-OS build/test, emulator
tests, release policy tests, version selection, four native packages, archive/checksum
verification, then attestation and publication. The normal patch bump, `[minor]`,
`[major]`, `[skip release]`, and the existing benchmark input retain their behavior.

For an explicit stable or prerelease version, run **CI** with **main** selected and
set `release_version` to SemVer without `v` or build metadata, for example:

```sh
gh workflow run ci.yml --repo bitcodepro/QueueLoom --ref main -f release_version=1.2.3-rc.1
```

Leave it empty for the existing automatic bump. An explicit version takes precedence
over `bump` and the commit's skip marker. Do not create a tag beforehand. A dispatch
on another branch or on a tag can test code but cannot enter the release jobs.
An explicit stable version must be greater than the highest stable tag; prerelease
versions remain supported and are always published with `--prerelease --latest=false`.

## Commit and CI identity

Every checkout, including the reusable package workflow, uses the immutable
`github.sha` captured when that run starts. Advancing main while jobs run does not
change the packaged code. Immediately before publication the tested SHA must still
be an ancestor of the current main SHA (or identical); a reset/divergence fails.
It must also descend from (or equal) the commit of every published release, including
prereleases, and of the highest stable tag (including a reserved tag without a release).
Annotated tags are peeled to exact commits. Missing/unresolvable tags or failed API
checks refuse publication. The release's `target_commitish`, list position, commit
date and GitHub Latest designation are not evidence of commit ancestry.

Both the stable version ceiling and released commit history are read from GitHub
inside the publisher before tag creation and again after reservation, just before
release creation. This catches a newer tag/release created after the version job's
earlier checkout. Rerunning all jobs of X1 cannot label X1 with a new number after X2
has been released, even when X1 remains an ancestor of main. The operator must start
fresh CI on main, not keep incrementing the version of the stale run.

A new stable release also requires a SHA that has not already been published as a
stable release (`draft: false`, `prerelease: false`). Reissuing that SHA under a new
patch/minor/major number is refused, including when it appears only on a later release
page or an annotated tag. This is checked before tag creation and again after reservation.
The error requires a **new tested main commit**; a different version or a fresh run
on the same SHA cannot fix it, so no version-only recovery suggestion is printed.

Promoting a published prerelease to stable on the same SHA is intentional and remains
supported: for example `v1.2.3-rc.1` to `v1.2.3`, provided the stable version ceiling,
main ancestry and all other prerequisites pass. GitHub's published release flags
identify the channel, not merely the tag suffix. The first successful stable publication
consumes that SHA for future stable releases. A reserved tag or draft without a published
stable release does not consume the SHA, allowing recovery with a new version after an
upload failure. Existing tags/drafts still cannot be reused. Prerelease publications
retain their existing behavior and always stay outside Latest. Unknown release flags
fail closed; they cannot hide a possible prior stable publication.

Successful CI evidence is the **same run's job dependency graph**. The only job
with `contents: write` explicitly needs build/test, emulator tests, release policy
tests, version, release packages and archive verification. Missing, failed, cancelled,
skipped or unfinished prerequisites cannot start it under GitHub's implicit
`success()` condition. It does not accept check names, externally posted statuses,
another workflow's artifacts, a PR run, or a successful CI run for a different SHA.
It does not poll the overall completion status of its own unfinished run.

The publisher checks repository, main ref, event, workflow path/ref and exact SHA.
It uploads and attests archives without extracting or executing their contents.
Native package smoke tests execute only in the preceding jobs with read permissions.

## Tags, retries and failures

Publication refuses any existing tag, even if it already points at the tested SHA,
and any existing release, including a draft. It creates a lightweight tag with
GitHub's create-reference POST directly on the tested SHA. A concurrent creation
fails; there is no update, force push or delete fallback. It checks the new tag's
type and SHA again before `gh release create --verify-tag`.

A failed publication can leave a reserved tag or a partially uploaded release.
Rerunning the publisher with that same version explicitly fails, without altering
existing assets or tags. The failure message reads current remote tags/releases and
suggests a currently unused version for a **fresh CI dispatch on main** after the
failure is investigated. If those reads fail, it says it cannot determine a version;
it never assumes absence. The suggestion is not a reservation and must pass all
checks again. Prerelease suggestions skip occupied tag and draft-release names too.

The existing workflow concurrency group `ci-${{ github.ref }}` already serializes
entire main runs, including version selection, with cancellation limited to PRs.
The publisher also retains the shared `release` concurrency group. A newer pending
main run can replace a previous pending run under GitHub's queue policy; this is not
a promise to release every individual push. Ordering must not be used as proof of
code freshness. If two publishers nevertheless select one version (for example an
older workflow or an external publisher), the first atomic tag creation wins and
the other explicitly fails with recovery guidance. It never silently renumbers the
already built packages or reuses the winner's tag.

Tag creation and release publication are separate API operations, not one transaction.
The server restriction below closes tag movement after the final code check. Current
CI publishers share concurrency; the fresh post-reservation reads detect conflicting
state before publication. An independently authorized external writer can still race
after that last read, within the remaining trust boundary described below.

## Updater and GitHub Latest

The app's `GitHubUpdateChecker` requests `/releases?per_page=30`, ignores drafts and
prereleases, and selects the highest numeric stable version in that response. It
does **not** request `/releases/latest` or follow the Latest label. Tests verify that
a later-listed v1.0.1 does not displace v1.5.0 or downgrade a running v1.5.0. Thus a
lower version is a release-policy error, not automatically an updater downgrade;
the stale-code/new-higher-number scenario is still unsafe for that updater.

The [CLI manual](https://cli.github.com/manual/gh_release_create) describes the default
Latest choice as automatic based on date and version. The
[REST API](https://docs.github.com/en/rest/releases/releases#create-a-release) also
exposes explicit `make_latest` values; drafts and prereleases cannot be Latest.
This workflow avoids relying on defaults: after all monotonicity/ancestry checks,
stable releases use `--latest` and prereleases use `--latest=false`.

## Server state and remaining trust boundary

Removing `.github/workflows/release.yml` from main does not disable definitions in
historical tags. With separate owner approval, the following settings were applied
and confirmed by read-only API on 2026-10-10:

- Workflow **Release from tag**, ID `367618516`, path
  `.github/workflows/release.yml`: `disabled_manually`. CI (`367600266`) and
  Package (`370970207`) remain `active`.
- [Immutable release tags, ruleset 24839428](https://github.com/bitcodepro/QueueLoom/rules/24839428):
  target `tag`, enforcement `active`, include `refs/tags/v*`, no exclusions,
  rules `update` and `deletion`, empty bypass actors. Creation remains permitted.
- Existing default-branch ruleset `20718907` was preserved. No credentials, token
  scopes, environments, secrets or account settings were changed.

These measures close the historical tag-trigger route and tag movement/deletion.
They **do not fully protect against a compromised repository writer**: a writer
can create a different workflow granting itself `contents: write`, call the Releases
API directly with sufficient credentials, or create an arbitrary new tag. A code
check inside a writer-controlled workflow is not an independent trust boundary.
An administrator able to change rules or re-enable workflows is also trusted.
These guards apply to runs whose workflow definition contains this policy. Rerunning
a historical main run from before this change retains its old workflow and original
SHA/ref; repository code changes cannot retroactively constrain that old publisher.
The disabled legacy tag workflow remains disabled, but this PR does not disable CI
or change server settings to revoke historical main publishers. Independent server
publication authority remains necessary for that guarantee as well.

A full guarantee additionally requires server-enforced separation of publication
authority from ordinary writers, assessed together rather than as standalone fixes:

1. Restrict creation of `v*` tags to an independently trusted release GitHub App or
   service, and keep update/deletion restrictions without bypass. This would require
   replacing ordinary `GITHUB_TOKEN` tag creation with that publisher's credentials;
   it is not part of this PR or the approved ruleset.
2. Remove ordinary writers' ability to publish/edit Releases and prevent their
   arbitrary workflows from obtaining equivalent publication credentials. Standard
   repository write access and a default read-only token alone do not establish this
   boundary: workflows can request write permissions. If the account's available
   roles/policies cannot express it, use an independently controlled distribution
   repository/service and validate its release provenance on the consumer side.
3. Protect main and publishing workflow/scripts with required independent review
   and required CI, no writer bypass, and keep the privileged publisher in a protected
   location. An environment approval helps only if every publication credential is
   gated there and direct Releases API access is also constrained; writers can omit
   an environment from a new workflow.
4. Keep the legacy workflow disabled and tightly control permissions that can
   re-enable workflows or change rules, publisher credentials and protected settings.

GitHub references: [workflow dependency and success semantics](https://docs.github.com/en/actions/reference/workflows-and-actions/workflow-syntax#jobsjob_idneeds),
[create-reference API](https://docs.github.com/en/rest/git/refs#create-a-reference),
[ruleset restrictions](https://docs.github.com/en/repositories/configuring-branches-and-merges-in-your-repository/managing-rulesets/available-rules-for-rulesets),
[workflow token permissions](https://docs.github.com/en/actions/tutorials/authenticate-with-github_token).

## Validation

Release policy tests exercise main push/dispatch, exact SHA/ancestry, untrusted refs
and workflow identities, existing tags/releases (including paginated drafts), tag
creation races, moved tags, API failures and retry rejection. They also cover stable
numeric monotonicity, a stale X1 rerun after an X2 release, published prerelease/lower
version ancestry, annotated tags, state changes after reservation, two publishers
selecting one version and recovery after failed publication. Duplicate stable SHA,
prerelease-to-stable promotion, reserved/draft recovery and a stable publication
racing after reservation are covered too. Workflow graph tests
verify all required `needs`, failure/pending/absent prerequisite states, pinned
checkouts, current-run artifact selection, existing concurrency and the publisher's lack of package execution.
PowerShell tests cover automatic bumps, skip markers, explicit versions, prereleases,
input injection and invalid SemVer. Run them without publishing:

```sh
python3 -m pip install PyYAML==6.0.3
python3 -B -m unittest discover -s .github/scripts/tests -v
python3 -B -m unittest discover -s .github/scripts/workflow-tests -v
pwsh -File .github/scripts/tests/test-next-version.ps1
actionlint .github/workflows/ci.yml .github/workflows/package.yml
git diff --check
dotnet test tests/QueueLoom.Tests/QueueLoom.Tests.csproj -c Release --filter FullyQualifiedName~GitHubUpdateCheckerTests
```

The job-state tests are structural regression checks, not live release experiments.
No test tags or releases are required. Cross-platform package/emulator execution is
covered by the normal PR CI; the full privileged publish path is not dispatched for
validation.
