"""Publish only after ci.yml's required jobs succeed. Never execute package contents.

This is defense in depth, not an authorization boundary against a malicious writer.
Tag immutability and publisher credentials require independent server restrictions.
"""
import json
import os
from pathlib import Path
import re
import subprocess
import urllib.error
from urllib.parse import quote
import urllib.request


class DuplicateStableRelease(RuntimeError):
    pass


class GitHub:
    def __init__(self, repository, token):
        self.base = f"https://api.github.com/repos/{repository}/"
        self.token = token

    def request(self, method, path, data=None, *, missing_ok=False):
        request = urllib.request.Request(
            self.base + path, method=method,
            data=None if data is None else json.dumps(data).encode(),
            headers={"Authorization": f"Bearer {self.token}",
                     "Accept": "application/vnd.github+json",
                     "Content-Type": "application/json",
                     "X-GitHub-Api-Version": "2022-11-28"})
        try:
            with urllib.request.urlopen(request, timeout=60) as response:
                return json.load(response)
        except urllib.error.HTTPError as error:
            if missing_ok and error.code == 404:
                return None
            raise RuntimeError(f"GitHub {method} {path} failed: HTTP {error.code}") from error


def validate_context(env):
    if (env["GITHUB_REPOSITORY"] != "bitcodepro/QueueLoom" or
            env["GITHUB_REF"] != "refs/heads/main" or
            env["GITHUB_EVENT_NAME"] not in ("push", "workflow_dispatch") or
            env["GITHUB_WORKFLOW_REF"] !=
            "bitcodepro/QueueLoom/.github/workflows/ci.yml@refs/heads/main"):
        raise RuntimeError("Publishing requires this repository's CI workflow on main.")
    sha = env["GITHUB_SHA"]
    tag = env["RELEASE_TAG"]
    if not re.fullmatch(r"[0-9a-f]{40}", sha):
        raise RuntimeError("Publishing requires an exact commit SHA.")
    if not re.fullmatch(r"v(0|[1-9][0-9]*)\.(0|[1-9][0-9]*)\.(0|[1-9][0-9]*)(-[0-9A-Za-z-]+(\.[0-9A-Za-z-]+)*)?", tag):
        raise RuntimeError("Invalid release tag.")
    if "-" in tag and any(re.fullmatch(r"0[0-9]+", part) for part in tag.split("-", 1)[1].split(".")):
        raise RuntimeError("Numeric prerelease identifiers cannot have leading zeroes.")
    return sha, tag


def check_main(api, sha):
    # Pin main's current SHA before comparison; a branch name is not the build target.
    main = api.request("GET", "git/ref/heads/main")["object"]["sha"]
    comparison = api.request("GET", f"compare/{sha}...{main}")
    if (comparison["status"] not in ("ahead", "identical") or
            comparison["merge_base_commit"]["sha"] != sha):
        raise RuntimeError("The tested commit is no longer an ancestor of main.")


def read_state(api):
    references = api.request("GET", "git/matching-refs/tags/v")
    refs = {ref["ref"].removeprefix("refs/tags/"): ref for ref in references}
    releases = []
    page = 1
    while True:
        batch = api.request("GET", f"releases?per_page=100&page={page}")
        releases.extend(batch)
        if len(batch) < 100:
            return refs, releases
        page += 1


def stable_version(tag):
    match = re.fullmatch(r"v([0-9]+)\.([0-9]+)\.([0-9]+)", tag)
    return tuple(map(int, match.groups())) if match else None


def stable_tags(refs, releases, exclude=None):
    names = set(refs) | {item["tag_name"] for item in releases}
    return {name: stable_version(name) for name in names
            if name != exclude and stable_version(name) is not None}


def check_unused_release(tag, releases):
    if any(item["tag_name"] == tag for item in releases):
        raise RuntimeError(f"Release {tag} already exists (including drafts).")


def check_unused(api, tag, releases):
    if api.request("GET", f"git/ref/tags/{tag}", missing_ok=True) is not None:
        raise RuntimeError(f"Tag {tag} already exists; tags are never moved or reused.")
    check_unused_release(tag, releases)


def tag_commit(api, tag, refs):
    reference = refs.get(tag)
    if reference is None:
        # A published release with a missing/unresolvable tag must fail closed.
        reference = api.request("GET", f"git/ref/tags/{quote(tag, safe='')}")
    obj = reference["object"]
    for _ in range(16):
        if not re.fullmatch(r"[0-9a-f]{40}", obj["sha"]):
            raise RuntimeError(f"Invalid object SHA for {tag}.")
        if obj["type"] == "commit":
            return obj["sha"]
        if obj["type"] != "tag":
            break
        obj = api.request("GET", f"git/tags/{obj['sha']}")["object"]
    raise RuntimeError(f"Tag {tag} does not resolve to a commit.")


def check_release_order(api, sha, tag, refs, releases, *, reserved=False):
    # Ignore only our freshly reserved tag when rechecking the stable ceiling.
    stable = stable_tags(refs, releases, exclude=tag if reserved else None)
    highest = max(stable, key=stable.get) if stable else None
    version = stable_version(tag)
    if version is not None and highest is not None and version <= stable[highest]:
        raise RuntimeError(f"Stable {tag} must be greater than highest stable {highest}.")
    # Check every published release, including prereleases; never rely on Latest,
    # commit dates, release ordering, or target_commitish (which may be a branch).
    history = {highest} if highest else set()
    published_stable = set()
    for item in releases:
        if type(item["draft"]) is not bool:
            raise RuntimeError("Invalid release draft status.")
        if not item["draft"]:
            if type(item["prerelease"]) is not bool:
                raise RuntimeError("Invalid release prerelease status.")
            history.add(item["tag_name"])
            if not item["prerelease"]:
                published_stable.add(item["tag_name"])
    checked = set()
    for previous in sorted(history):
        base = tag_commit(api, previous, refs)
        if version is not None and base == sha and previous in published_stable:
            raise DuplicateStableRelease(
                f"Tested commit {sha} already has published stable release {previous}. "
                "A higher version alone cannot release this SHA again. Use a new tested main commit. "
                "Any reserved tag remains; do not move/delete tags or replace release assets.")
        if base in checked or base == sha:
            continue
        comparison = api.request("GET", f"compare/{base}...{sha}")
        if (comparison["status"] not in ("ahead", "identical") or
                comparison["merge_base_commit"]["sha"] != base):
            raise RuntimeError(
                f"Tested commit {sha} is older than or diverges from {previous} ({base}). "
                "Start fresh CI on main; do not rerun this old SHA.")
        checked.add(base)


def recovery_hint(api, tag, sha):
    try:
        refs, releases = read_state(api)
        occupied = set(refs) | {item["tag_name"] for item in releases} | {tag}
        if stable_version(tag) is not None:
            for item in releases:
                if type(item["draft"]) is not bool:
                    raise RuntimeError("Invalid release draft status.")
                if not item["draft"]:
                    if type(item["prerelease"]) is not bool:
                        raise RuntimeError("Invalid release prerelease status.")
                    if not item["prerelease"] and tag_commit(api, item["tag_name"], refs) == sha:
                        return (f"Tested commit {sha} already has published stable release {item['tag_name']}. "
                                "Inspect the existing release; publication may have succeeded despite the error. "
                                "For another stable release, use a new tested main commit, not another version of this SHA. "
                                "Do not move/delete tags or replace release assets.")
            major, minor, patch = max([stable_version(tag), *stable_tags(refs, releases).values()])
            suggestion = f"{major}.{minor}.{patch + 1}"
        else:
            suggestion = tag[1:] + ".1"
            while "v" + suggestion in occupied:
                suggestion += ".1"
        return (f"Do not move/delete {tag} or replace release assets. After investigating, "
                f"start fresh CI on main with release_version={suggestion} (or empty for automatic bump). "
                "This currently unused version is a suggestion, revalidated at publication; an old SHA remains rejected.")
    except (RuntimeError, OSError, KeyError, TypeError, ValueError):
        return ("Could not determine the next unused version. Inspect remote tags/releases, "
                "then start fresh CI on main. Do not move/delete tags or replace release assets.")


def publish(api, env, assets, run=subprocess.run):
    sha, tag = validate_context(env)
    if not assets:
        raise RuntimeError("No verified release assets.")
    try:
        check_main(api, sha)
        refs, releases = read_state(api)
        check_unused(api, tag, releases)
        check_release_order(api, sha, tag, refs, releases)
        # Create-only POST is atomic. A racing creator causes 422; never PATCH or force-push.
        api.request("POST", "git/refs", {"ref": f"refs/tags/{tag}", "sha": sha})
        # Re-read remote state after reservation, not the version job's earlier snapshot.
        refs, releases = read_state(api)
        check_unused_release(tag, releases)
        check_release_order(api, sha, tag, refs, releases, reserved=True)
        check_main(api, sha)
        reference = api.request("GET", f"git/ref/tags/{tag}")
        if reference["object"]["type"] != "commit" or reference["object"]["sha"] != sha:
            raise RuntimeError("The new tag changed before publication.")
        command = ["gh", "release", "create", tag, *assets, "--verify-tag", "--generate-notes",
                   "--title", tag, "--repo", env["GITHUB_REPOSITORY"]]
        command += ["--prerelease", "--latest=false"] if "-" in tag else ["--latest"]
        run(command, check=True)
    except DuplicateStableRelease:
        # Increasing the version cannot recover a duplicate stable SHA.
        raise
    except (RuntimeError, OSError, subprocess.CalledProcessError, KeyError, TypeError, ValueError) as error:
        raise RuntimeError(f"{error}\n{recovery_hint(api, tag, sha)}") from error


if __name__ == "__main__":
    publish(GitHub(os.environ["GITHUB_REPOSITORY"], os.environ["GH_TOKEN"]),
            os.environ, [str(path) for path in sorted(Path("packages").iterdir()) if path.is_file()])
