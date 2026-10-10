#!/usr/bin/env python3
"""Restore six locked runtime images; cache failure always falls back to pinned pulls."""
import argparse
import hashlib
import json
import os
from pathlib import Path, PurePosixPath
import re
import subprocess
import tarfile
import time

FORMAT = 1
PLATFORM = 'linux/amd64'
MAX_ARCHIVE_BYTES = 8 * 1024**3  # Conservative uncompressed-input cap; no repository settings change.
NAMES = ('localstack', 'rabbitmq', 'kafka', 'pubsub', 'sqlserver', 'servicebus')
DIGEST = re.compile(r'sha256:[0-9a-f]{64}\Z')

def local_tag(image):
    return f"localhost/queueloom-ci/{image['name']}:sha256-{image['image'].rsplit('@sha256:', 1)[1]}"

def load_lock(path):
    raw = Path(path).read_bytes()
    lock = json.loads(raw)
    if lock.get('format') != FORMAT or lock.get('platform') != PLATFORM:
        raise ValueError('Unsupported lock format/platform')
    images = lock.get('images', [])
    if len(images) != 6 or {image['name'] for image in images} != set(NAMES):
        raise ValueError('Lock must contain exactly the six named emulators')
    for image in images:
        repository, separator, digest = image['image'].partition('@')
        if not separator or not re.fullmatch(r'[a-z0-9][a-z0-9./_-]*', repository) or not DIGEST.fullmatch(digest):
            raise ValueError('Image must be an immutable manifest reference')
        if not DIGEST.fullmatch(image['config_id']):
            raise ValueError('Invalid expected config ID')
        if not image.get('rootfs_diff_ids') or not all(DIGEST.fullmatch(value) for value in image['rootfs_diff_ids']):
            raise ValueError('Expected rootfs layer identities are required')
    return lock, hashlib.sha256(raw).hexdigest()

class Docker:
    def run(self, *arguments, capture=False):
        result = subprocess.run(['docker', *arguments], text=True, capture_output=capture, check=True)
        return result.stdout if capture else None

    def platform(self):
        return self.run('version', '--format', '{{.Server.Os}}/{{.Server.Arch}}', capture=True).strip()

    def inspect(self, reference):
        try:
            return json.loads(self.run('image', 'inspect', reference, capture=True))[0]
        except subprocess.CalledProcessError:
            return None

    def pull(self, image):
        self.run('pull', '--platform='+PLATFORM, image['image'])

    def tag(self, image):
        self.run('image', 'tag', image['image'], local_tag(image))

    def load(self, path):
        self.run('image', 'load', '--input', str(path))

    def save(self, path, images):
        self.run('image', 'save', '--output', str(path), *(local_tag(image) for image in images))

    def remove_owned_refs(self, images):
        for image in images:
            self.run('image', 'rm', local_tag(image), image['image'])

def verify_identity(actual, expected):
    if not actual or actual.get('Id') != expected['config_id']:
        raise ValueError(f"Unexpected image config ID: {expected['name']}")
    if actual.get('Os') != 'linux' or actual.get('Architecture') != 'amd64':
        raise ValueError(f"Unexpected image platform: {expected['name']}")
    if actual.get('RootFS', {}).get('Layers') != expected['rootfs_diff_ids']:
        raise ValueError(f"Unexpected rootfs identities: {expected['name']}")
    # RepoDigests can legitimately be absent after docker save/load on the classic store.

def verify_images(docker, images):
    for image in images:
        verify_identity(docker.inspect(local_tag(image)), image)

def sha256_file(path):
    digest = hashlib.sha256()
    with path.open('rb') as stream:
        for chunk in iter(lambda: stream.read(1024 * 1024), b''):
            digest.update(chunk)
    return digest.hexdigest()

def verify_archive(path, images):
    """Reject extra tags/configs before loading; never extract archive members ourselves."""
    expected = {local_tag(image): image for image in images}
    with tarfile.open(path, mode='r:') as archive:
        members = {}
        def safe_name(name):
            if not isinstance(name, str) or not name or name.startswith('/') or '\\' in name or any(part in ('', '.', '..') for part in name.split('/')):
                raise ValueError('Unsafe archive member name')
            return name
        for member in archive.getmembers():
            name = safe_name(member.name.rstrip('/') if member.isdir() else member.name)
            if name in members or not (member.isfile() or member.isdir()):
                raise ValueError('Duplicate or unsafe archive member type')
            members[name] = member
        allowed = {'manifest.json'}
        def read_regular(name, limit):
            name = safe_name(name)
            member = members[name]
            if not member.isfile() or member.size > limit:
                raise ValueError('Unexpected archive metadata member')
            return archive.extractfile(member).read()
        manifest = json.loads(read_regular('manifest.json', 64 * 1024))
        if not isinstance(manifest, list) or len(manifest) != 6:
            raise ValueError('Archive must contain exactly six image manifests')
        seen = set()
        legacy_repositories = {}
        for entry in manifest:
            if not isinstance(entry, dict):
                raise ValueError('Archive manifest entry must be an object')
            tags = entry.get('RepoTags')
            if not isinstance(tags, list) or len(tags) != 1 or not isinstance(tags[0], str) or tags[0] not in expected or tags[0] in seen:
                raise ValueError('Unexpected/duplicate archive image tags')
            seen.add(tags[0])
            image = expected[tags[0]]
            raw_config = read_regular(entry['Config'], 2 * 1024 * 1024)
            allowed.add(entry['Config'])
            if 'sha256:'+hashlib.sha256(raw_config).hexdigest() != image['config_id']:
                raise ValueError('Archive image config differs from the lock')
            config = json.loads(raw_config)
            if not isinstance(config, dict) or not isinstance(config.get('rootfs'), dict) or config.get('os') != 'linux' or config.get('architecture') != 'amd64' or config['rootfs'].get('diff_ids') != image['rootfs_diff_ids']:
                raise ValueError('Archive image platform/rootfs differs from the lock')
            layers = entry.get('Layers', [])
            if not isinstance(layers, list) or len(layers) != len(image['rootfs_diff_ids']):
                raise ValueError('Archive layer count differs from config')
            for layer in layers:
                member = members[safe_name(layer)]
                if not member.isfile():
                    raise ValueError('Unsafe/missing archive layer')
                allowed.add(layer)
            repository, tag = tags[0].rsplit(':', 1)
            legacy_repositories.setdefault(repository, {})[tag] = image['rootfs_diff_ids'][-1].removeprefix('sha256:')

        # Docker 28 classic save includes a legacy repositories file and an OCI
        # index, manifests and hashed legacy config blobs alongside manifest.json.
        # Validate their tags too; accepting arbitrary sidecars could add images.
        if 'repositories' in members:
            if json.loads(read_regular('repositories', 64 * 1024)) != legacy_repositories:
                raise ValueError('Unexpected legacy repository tags/layers')
            allowed.add('repositories')
        if 'index.json' in members or 'oci-layout' in members:
            if json.loads(read_regular('oci-layout', 1024)) != {'imageLayoutVersion': '1.0.0'}:
                raise ValueError('Unsupported OCI layout')
            index = json.loads(read_regular('index.json', 64 * 1024))
            if not isinstance(index, dict) or index.get('schemaVersion') != 2 or not isinstance(index.get('manifests'), list) or len(index['manifests']) != 6:
                raise ValueError('Unexpected OCI index')
            oci_seen = set()
            for descriptor in index['manifests']:
                if not isinstance(descriptor, dict) or descriptor.get('mediaType') != 'application/vnd.oci.image.manifest.v1+json' or not isinstance(descriptor.get('annotations'), dict):
                    raise ValueError('Unexpected OCI descriptor')
                annotations = descriptor['annotations']
                tag = annotations.get('io.containerd.image.name')
                if not isinstance(tag, str) or tag not in expected or tag in oci_seen or annotations.get('org.opencontainers.image.ref.name') != tag.rsplit(':', 1)[1]:
                    raise ValueError('Unexpected OCI image tag')
                oci_seen.add(tag)
                digest = descriptor.get('digest')
                if not isinstance(digest, str) or not DIGEST.fullmatch(digest):
                    raise ValueError('Invalid OCI manifest digest')
                name = 'blobs/sha256/'+digest.removeprefix('sha256:')
                raw = read_regular(name, 2 * 1024 * 1024)
                if 'sha256:'+hashlib.sha256(raw).hexdigest() != digest or descriptor.get('size') != len(raw):
                    raise ValueError('OCI manifest content mismatch')
                oci = json.loads(raw)
                if not isinstance(oci, dict) or not isinstance(oci.get('config'), dict) or oci['config'].get('digest') != expected[tag]['config_id'] or not isinstance(oci.get('layers'), list) or len(oci['layers']) != len(expected[tag]['rootfs_diff_ids']):
                    raise ValueError('OCI manifest differs from locked image')
                allowed.add(name)
            allowed.update(('index.json', 'oci-layout'))
        for name, member in members.items():
            if name in allowed or member.isdir():
                continue
            # Moby writes otherwise unreferenced, content-addressed legacy image
            # JSON here. They contain no additional tag lookup mechanism.
            if re.fullmatch(r'blobs/sha256/[0-9a-f]{64}', name):
                raw = read_regular(name, 2 * 1024 * 1024)
                legacy = json.loads(raw)
                if hashlib.sha256(raw).hexdigest() == name.rsplit('/', 1)[1] and isinstance(legacy, dict) and isinstance(legacy.get('id'), str) and re.fullmatch(r'[0-9a-f]{64}', legacy['id']) and legacy.get('os') == 'linux':
                    allowed.add(name)
                    continue
            raise ValueError('Unexpected archive member: '+name)
        directories = {str(parent) for name in allowed for parent in PurePosixPath(name).parents if str(parent) != '.'}
        if any(member.isdir() and name not in directories for name, member in members.items()):
            raise ValueError('Unexpected archive directory')

def prepare(lock_path, cache_dir, write_archive=False, docker=None, max_bytes=MAX_ARCHIVE_BYTES):
    started = time.perf_counter()
    lock, lock_hash = load_lock(lock_path)
    docker = docker or Docker()
    if docker.platform() != PLATFORM:
        raise ValueError('Runtime image caching requires a linux/amd64 Docker server')
    images = lock['images']
    cache_dir = Path(cache_dir)
    cache_dir.mkdir(parents=True, exist_ok=True)
    archive_path = cache_dir / 'images.tar'
    metadata_path = cache_dir / 'metadata.json'
    metrics = {'format': FORMAT, 'platform': PLATFORM, 'mode': 'pull', 'pull_count': 0, 'archive_bytes': 0, 'archive_cap_bytes': max_bytes, 'validation_seconds': 0, 'load_seconds': 0, 'pull_seconds': 0, 'save_seconds': 0, 'save_ready': False, 'cache_error': None}
    if archive_path.exists() or metadata_path.exists():
        try:
            validation_started = time.perf_counter()
            metadata = json.loads(metadata_path.read_text(encoding='utf-8'))
            if not isinstance(metadata, dict):
                raise ValueError('Cache metadata must be an object')
            size = archive_path.stat().st_size
            if metadata.get('format') != FORMAT or metadata.get('platform') != PLATFORM or metadata.get('lock_sha256') != lock_hash:
                raise ValueError('Cache metadata does not match this lock/platform/format')
            if size > max_bytes or metadata.get('archive_bytes') != size or metadata.get('archive_sha256') != sha256_file(archive_path):
                raise ValueError('Cache archive is oversized, truncated or corrupt')
            verify_archive(archive_path, images)
            metrics['validation_seconds'] = time.perf_counter()-validation_started
            load_started = time.perf_counter()
            docker.load(archive_path)
            verify_images(docker, images)
            metrics.update(mode='archive', load_seconds=time.perf_counter()-load_started, archive_bytes=size, save_ready=True)
            if not write_archive:
                archive_path.unlink()
                metadata_path.unlink()
                metrics['save_ready'] = False
        except (OSError, ValueError, KeyError, TypeError, tarfile.TarError, subprocess.CalledProcessError) as error:
            metrics['cache_error'] = str(error)
            print(f'::warning::Runtime image cache rejected; using pinned registry pulls: {error}', flush=True)
    if metrics['mode'] != 'archive':
        pull_started = time.perf_counter()
        for image in images:
            docker.pull(image)
            metrics['pull_count'] += 1
            verify_identity(docker.inspect(image['image']), image)
            docker.tag(image)
        verify_images(docker, images)
        metrics['pull_seconds'] = time.perf_counter()-pull_started
        if write_archive:
            save_started = time.perf_counter()
            partial = cache_dir / 'images.partial.tar'
            try:
                docker.save(partial, images)
                size = partial.stat().st_size
                metrics['archive_bytes'] = size
                if size > max_bytes:
                    print(f'::warning::Runtime image archive {size} bytes exceeds cap {max_bytes}; cache save skipped.', flush=True)
                else:
                    verify_archive(partial, images)
                    metadata = {'format': FORMAT, 'platform': PLATFORM, 'lock_sha256': lock_hash, 'archive_bytes': size, 'archive_sha256': sha256_file(partial)}
                    os.replace(partial, archive_path)
                    metadata_path.write_text(json.dumps(metadata, indent=2)+'\n', encoding='utf-8')
                    metrics['save_ready'] = True
            except (OSError, ValueError, KeyError, TypeError, tarfile.TarError, subprocess.CalledProcessError) as error:
                print(f'::warning::Runtime image archive save failed; verified images remain usable: {error}', flush=True)
                metrics['archive_save_error'] = str(error)
            finally:
                partial.unlink(missing_ok=True)
                metrics['save_seconds'] = time.perf_counter()-save_started
    metrics['prepare_seconds'] = time.perf_counter()-started
    return lock, metrics

def export_images(lock, metrics):
    if os.environ.get('GITHUB_ENV'):
        with Path(os.environ['GITHUB_ENV']).open('a', encoding='utf-8') as stream:
            for image in lock['images']:
                stream.write(f"QUEUELOOM_{image['name'].upper()}_IMAGE={local_tag(image)}\n")
    if os.environ.get('GITHUB_OUTPUT'):
        with Path(os.environ['GITHUB_OUTPUT']).open('a', encoding='utf-8') as stream:
            stream.write(f"save-ready={str(metrics['save_ready']).lower()}\narchive-bytes={metrics['archive_bytes']}\nmode={metrics['mode']}\n")
    if os.environ.get('GITHUB_STEP_SUMMARY'):
        with Path(os.environ['GITHUB_STEP_SUMMARY']).open('a', encoding='utf-8') as stream:
            stream.write('\nRuntime images (linux/amd64):\n\n')
            stream.write('| Phase | Result |\n|---|---|\n')
            for name in ('mode', 'pull_count', 'archive_bytes', 'archive_cap_bytes', 'validation_seconds', 'load_seconds', 'pull_seconds', 'save_seconds', 'prepare_seconds'):
                stream.write(f'| {name} | {metrics[name]} |\n')
            stream.write('\nOnly public prebuilt images and integrity metadata are cached. GitHub cache restore/download time is a separate workflow step.\n')

def benchmark(lock_path, cache_dir, report_path):
    if os.environ.get('GITHUB_ACTIONS') != 'true' or os.environ.get('RUNNER_OS') != 'Linux' or os.environ.get('RUNNER_ARCH') != 'X64':
        raise ValueError('Benchmark is restricted to the disposable linux/x64 GitHub-hosted job')
    cache_dir = Path(cache_dir)
    if cache_dir.exists():
        raise ValueError('Benchmark requires a fresh, dedicated cache directory')
    docker = Docker()
    lock, _ = load_lock(lock_path)
    preexisting = [image['name'] for image in lock['images'] if docker.inspect(image['config_id'])]
    lock, cold = prepare(lock_path, cache_dir, write_archive=True, docker=docker)
    result = {'cold': cold, 'preexisting_locked_images': preexisting, 'github_network_restore_measured': False, 'github_network_restore_seconds': None}
    if not cold['save_ready']:
        result['warm'] = {'skipped': 'Archive unavailable or exceeds cap; no warm claim'}
    else:
        with tarfile.open(cache_dir / 'images.tar', 'r:') as archive:
            result['archive_member_names'] = [member.name for member in archive.getmembers()]
        result['docker_server_version'] = docker.run('version', '--format', '{{.Server.Version}}', capture=True).strip()
        docker.remove_owned_refs(lock['images'])
        retained = [image['name'] for image in lock['images'] if docker.inspect(local_tag(image)) or docker.inspect(image['config_id'])]
        if retained:
            raise ValueError('Locked images still resident; refuse a misleading empty-store load measurement')
        # Retain this local benchmark archive for the deliberate corruption test.
        # This job never invokes the GitHub cache save action.
        _, warm = prepare(lock_path, cache_dir, write_archive=True, docker=docker)
        if warm['mode'] != 'archive' or warm['pull_count'] != 0:
            raise ValueError('Local warm archive did not restore all six images without registry pulls')
        result['warm_local_archive'] = warm
        result['loaded_repo_digests'] = {image['name']: docker.inspect(local_tag(image)).get('RepoDigests', []) for image in lock['images']}
        metadata_path = cache_dir / 'metadata.json'
        metadata = json.loads(metadata_path.read_text())
        metadata['archive_sha256'] = '0' * 64
        metadata_path.write_text(json.dumps(metadata), encoding='utf-8')
        _, invalid = prepare(lock_path, cache_dir, write_archive=False, docker=docker)
        if invalid['mode'] != 'pull' or invalid['pull_count'] != 6 or not invalid['cache_error']:
            raise ValueError('Corrupt cache did not fall back to six verified pinned pulls')
        result['invalid_cache_fallback'] = invalid
    report_path.write_text(json.dumps(result, indent=2)+'\n', encoding='utf-8')
    print(json.dumps(result, indent=2), flush=True)

if __name__ == '__main__':
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('mode', choices=('prepare', 'benchmark'))
    parser.add_argument('--lock', type=Path, default=Path(__file__).resolve().parents[1] / 'emulator-images.lock.json')
    parser.add_argument('--cache-dir', type=Path, required=True)
    parser.add_argument('--write-archive', choices=('true', 'false'), default='false')
    parser.add_argument('--report', type=Path, required=True)
    args = parser.parse_args()
    if args.mode == 'benchmark':
        benchmark(args.lock, args.cache_dir, args.report)
    else:
        locked, measured = prepare(args.lock, args.cache_dir, write_archive=args.write_archive == 'true')
        args.report.write_text(json.dumps(measured, indent=2)+'\n', encoding='utf-8')
        export_images(locked, measured)
        print(json.dumps(measured, indent=2), flush=True)
