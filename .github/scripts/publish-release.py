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
import urllib.request


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


def check_unused(api, tag):
    if api.request("GET", f"git/ref/tags/{tag}", missing_ok=True) is not None:
        raise RuntimeError(f"Tag {tag} already exists; tags are never moved or reused.")
    # List releases too: include draft releases and paginate, not just the latest release.
    page = 1
    while True:
        releases = api.request("GET", f"releases?per_page=100&page={page}")
        if any(release["tag_name"] == tag for release in releases):
            raise RuntimeError(f"Release {tag} already exists (including drafts).")
        if len(releases) < 100:
            break
        page += 1


def publish(api, env, assets, run=subprocess.run):
    sha, tag = validate_context(env)
    if not assets:
        raise RuntimeError("No verified release assets.")
    check_main(api, sha)
    check_unused(api, tag)
    # Create-only POST is atomic. A racing creator causes 422; never PATCH or force-push.
    api.request("POST", "git/refs", {"ref": f"refs/tags/{tag}", "sha": sha})
    reference = api.request("GET", f"git/ref/tags/{tag}")
    if reference["object"]["type"] != "commit" or reference["object"]["sha"] != sha:
        raise RuntimeError("The new tag changed before publication.")
    command = ["gh", "release", "create", tag, *assets, "--verify-tag", "--generate-notes",
               "--title", tag, "--repo", env["GITHUB_REPOSITORY"]]
    if "-" in tag:
        command.append("--prerelease")
    run(command, check=True)


if __name__ == "__main__":
    publish(GitHub(os.environ["GITHUB_REPOSITORY"], os.environ["GH_TOKEN"]),
            os.environ, [str(path) for path in sorted(Path("packages").iterdir()) if path.is_file()])
