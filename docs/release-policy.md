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

## Commit and CI identity

Every checkout, including the reusable package workflow, uses the immutable
`github.sha` captured when that run starts. Advancing main while jobs run does not
change the packaged code. Immediately before publication the tested SHA must still
be an ancestor of the current main SHA (or identical); a reset/divergence fails.

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
existing assets or tags. Select a new version after investigating the failure.
Rerunning all automatic version-selection jobs may choose the next unused version;
all jobs still use the original run SHA and execute their checks again. Tag creation
and release publication are separate API operations, not one transaction. The
server restriction below closes the tag-movement race after the final code check.

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
creation races, moved tags, API failures and retry rejection. Workflow graph tests
verify all required `needs`, failure/pending/absent prerequisite states, pinned
checkouts, current-run artifact selection and the publisher's lack of package execution.
PowerShell tests cover automatic bumps, skip markers, explicit versions, prereleases,
input injection and invalid SemVer. Run them without publishing:

```sh
python3 -m pip install PyYAML==6.0.3
python3 -B -m unittest discover -s .github/scripts/tests -v
python3 -B -m unittest discover -s .github/scripts/workflow-tests -v
pwsh -File .github/scripts/tests/test-next-version.ps1
actionlint -shellcheck= .github/workflows/ci.yml .github/workflows/package.yml
git diff --check
```

The job-state tests are structural regression checks, not live release experiments.
No test tags or releases are required. Cross-platform package/emulator execution is
covered by the normal PR CI; the full privileged publish path is not dispatched for
validation.
