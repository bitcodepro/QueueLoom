import contextlib
import hashlib
import importlib.util
import io
import json
import os
from pathlib import Path
import subprocess
import shlex
import tarfile
import tempfile
import unittest
from unittest.mock import patch

spec = importlib.util.spec_from_file_location('emulator_image_cache', Path(__file__).resolve().parents[1] / 'emulator_image_cache.py')
cache = importlib.util.module_from_spec(spec)
spec.loader.exec_module(cache)

def digest(data):
    return 'sha256:' + hashlib.sha256(data).hexdigest()

def fixture():
    images, configs, layers = [], {}, {}
    for name in cache.NAMES:
        layer = f'fixture layer for {name}'.encode()
        layer_id = digest(layer)
        layers[layer_id] = layer
        config = json.dumps({'os': 'linux', 'architecture': 'amd64', 'rootfs': {'type': 'layers', 'diff_ids': [layer_id]}, 'config': {'Env': []}}).encode()
        config_id = digest(config)
        configs[config_id] = config
        images.append({'name': name, 'image': f'example/{name}@{digest(name.encode())}', 'config_id': config_id, 'rootfs_diff_ids': [layer_id]})
    return {'format': 1, 'platform': 'linux/amd64', 'images': images}, configs, layers

def actual(image):
    return {'Id': image['config_id'], 'Os': 'linux', 'Architecture': 'amd64', 'RootFS': {'Layers': list(image['rootfs_diff_ids'])}, 'RepoDigests': []}

class FakeDocker:
    def __init__(self, lock, configs, layers):
        self.lock, self.configs, self.layers = lock, configs, layers
        self.images = {}
        self.pulls = []
        self.loads = 0
        self.loaded_fault = None
        self.pull_fault = None
        self.save_error = False
        self.server_platform = 'linux/amd64'

    def platform(self):
        return self.server_platform

    def inspect(self, ref):
        return self.images.get(ref)

    def pull(self, image):
        self.pulls.append(image['image'])
        value = actual(image)
        if self.pull_fault:
            value['Id'] = 'sha256:' + '0'*64
        self.images[image['image']] = value

    def tag(self, image):
        self.images[cache.local_tag(image)] = self.images[image['image']]

    def save(self, path, images):
        if self.save_error:
            raise subprocess.CalledProcessError(1, ['docker', 'image', 'save'])
        manifest = []
        members = {}
        descriptors = []
        repositories = {}
        for image in images:
            config_name = 'blobs/sha256/'+image['config_id'].removeprefix('sha256:')
            layer_names = ['blobs/sha256/'+value.removeprefix('sha256:') for value in image['rootfs_diff_ids']]
            manifest.append({'Config': config_name, 'RepoTags': [cache.local_tag(image)], 'Layers': layer_names})
            members[config_name] = self.configs[image['config_id']]
            for name, diff_id in zip(layer_names, image['rootfs_diff_ids']):
                members[name] = self.layers[diff_id]
            tag = cache.local_tag(image)
            repository, version = tag.rsplit(':', 1)
            repositories[repository] = {version: image['rootfs_diff_ids'][-1].removeprefix('sha256:')}
            oci = json.dumps({'schemaVersion': 2, 'config': {'digest': image['config_id']}, 'layers': [{'digest': value} for value in image['rootfs_diff_ids']]}).encode()
            oci_digest = digest(oci)
            members['blobs/sha256/'+oci_digest.removeprefix('sha256:')] = oci
            descriptors.append({'mediaType': 'application/vnd.oci.image.manifest.v1+json', 'digest': oci_digest, 'size': len(oci), 'annotations': {'io.containerd.image.name': tag, 'org.opencontainers.image.ref.name': version}})
            legacy = json.dumps({'id': image['config_id'].removeprefix('sha256:'), 'os': 'linux'}).encode()
            members['blobs/sha256/'+digest(legacy).removeprefix('sha256:')] = legacy
        members['manifest.json'] = json.dumps(manifest).encode()
        members['repositories'] = json.dumps(repositories).encode()
        members['oci-layout'] = b'{"imageLayoutVersion":"1.0.0"}'
        members['index.json'] = json.dumps({'schemaVersion': 2, 'manifests': descriptors}).encode()
        with tarfile.open(path, 'w') as archive:
            for name in ('blobs', 'blobs/sha256'):
                info = tarfile.TarInfo(name)
                info.type = tarfile.DIRTYPE
                archive.addfile(info)
            for name, content in members.items():
                info = tarfile.TarInfo(name)
                info.size = len(content)
                archive.addfile(info, io.BytesIO(content))

    def load(self, path):
        self.loads += 1
        for image in self.lock['images']:
            value = actual(image)
            if self.loaded_fault == 'id':
                value['Id'] = 'sha256:'+'0'*64
            elif self.loaded_fault == 'arch':
                value['Architecture'] = 'arm64'
            elif self.loaded_fault == 'rootfs':
                value['RootFS']['Layers'] = ['sha256:'+'0'*64]
            self.images[cache.local_tag(image)] = value

class RuntimeImageCacheTests(unittest.TestCase):
    def setUp(self):
        self.temporary = tempfile.TemporaryDirectory()
        self.root = Path(self.temporary.name)
        self.lock, configs, layers = fixture()
        self.path = self.root / 'lock.json'
        self.path.write_text(json.dumps(self.lock), encoding='utf-8')
        self.directory = self.root / 'images'
        self.docker = FakeDocker(self.lock, configs, layers)

    def tearDown(self):
        self.temporary.cleanup()

    def prepare(self, write=False, **options):
        with contextlib.redirect_stdout(io.StringIO()):
            return cache.prepare(self.path, self.directory, write_archive=write, docker=self.docker, **options)[1]

    def make_archive(self):
        result = self.prepare(write=True)
        self.assertTrue(result['save_ready'])
        self.docker.pulls.clear()
        self.docker.images.clear()
        return result

    def change_archive(self, changes, extra_member=None):
        path = self.directory / 'images.tar'
        with tarfile.open(path, 'r:') as archive:
            members = [(member, archive.extractfile(member).read() if member.isfile() else None) for member in archive.getmembers()]
        with tarfile.open(path, 'w') as archive:
            for member, content in members:
                if member.name in changes:
                    content = changes[member.name]
                    member.size = len(content)
                archive.addfile(member, io.BytesIO(content) if content is not None else None)
            if extra_member:
                member, content = extra_member
                member.size = len(content)
                archive.addfile(member, io.BytesIO(content) if member.isfile() else None)
        metadata_path = self.directory / 'metadata.json'
        metadata = json.loads(metadata_path.read_text())
        metadata.update(archive_bytes=path.stat().st_size, archive_sha256=cache.sha256_file(path))
        metadata_path.write_text(json.dumps(metadata))

    def assert_safe_fallback(self):
        result = self.prepare()
        self.assertEqual(result['mode'], 'pull')
        self.assertEqual(result['pull_count'], 6)
        self.assertEqual(self.docker.pulls, [image['image'] for image in self.lock['images']])
        self.assertEqual(self.docker.loads, 0)
        self.assertTrue(result['cache_error'])
        cache.verify_images(self.docker, self.lock['images'])

    def test_array_null_and_scalar_metadata_fall_back_without_load(self):
        for value in ([], None, 12, 'metadata'):
            with self.subTest(value=value):
                self.make_archive()
                (self.directory / 'metadata.json').write_text(json.dumps(value))
                self.assert_safe_fallback()
                (self.directory / 'metadata.json').unlink()
                (self.directory / 'images.tar').unlink()

    def test_non_object_manifest_entries_fall_back_without_load(self):
        for value in ([], None, 12, 'entry'):
            with self.subTest(value=value):
                self.make_archive()
                with tarfile.open(self.directory / 'images.tar', 'r:') as archive:
                    manifest = json.load(archive.extractfile('manifest.json'))
                manifest[0] = value
                self.change_archive({'manifest.json': json.dumps(manifest).encode()})
                self.assert_safe_fallback()

    def test_unsafe_layer_paths_reject_before_lookup_or_load(self):
        for value in ('/outside.tar', '../outside.tar', 'blobs/../outside', 'blobs\\outside', None):
            with self.subTest(value=value):
                self.make_archive()
                with tarfile.open(self.directory / 'images.tar', 'r:') as archive:
                    manifest = json.load(archive.extractfile('manifest.json'))
                manifest[0]['Layers'][0] = value
                self.change_archive({'manifest.json': json.dumps(manifest).encode()})
                self.assert_safe_fallback()

    def test_extra_regular_directory_link_or_duplicate_member_rejected(self):
        for name, kind in (('unrelated', tarfile.REGTYPE), ('unexpected-dir', tarfile.DIRTYPE), ('link', tarfile.SYMTYPE), ('link', tarfile.LNKTYPE), ('manifest.json', tarfile.REGTYPE)):
            with self.subTest(name=name, kind=kind):
                self.make_archive()
                member = tarfile.TarInfo(name)
                member.type = kind
                member.linkname = 'manifest.json'
                self.change_archive({}, (member, b'{}'))
                self.assert_safe_fallback()

    def test_legacy_or_oci_sidecar_cannot_add_an_unlocked_tag(self):
        for sidecar in ('repositories', 'index.json'):
            with self.subTest(sidecar=sidecar):
                self.make_archive()
                with tarfile.open(self.directory / 'images.tar', 'r:') as archive:
                    value = json.load(archive.extractfile(sidecar))
                if sidecar == 'repositories':
                    value['unrelated/image'] = {'latest': '0'*64}
                else:
                    value['manifests'][0]['annotations']['io.containerd.image.name'] = 'unrelated/image:latest'
                self.change_archive({sidecar: json.dumps(value).encode()})
                self.assert_safe_fallback()

    def test_verified_archive_is_retained_only_when_save_is_requested(self):
        self.make_archive()
        self.assertEqual(self.prepare(write=True)['mode'], 'archive')
        self.assertTrue((self.directory / 'images.tar').exists())
        self.assertTrue((self.directory / 'metadata.json').exists())
        self.assertEqual(self.prepare()['mode'], 'archive')
        self.assertFalse((self.directory / 'images.tar').exists())
        self.assertFalse((self.directory / 'metadata.json').exists())

    def test_cold_pr_pulls_exact_six_digests_and_does_not_create_archive(self):
        result = self.prepare()
        self.assertEqual(self.docker.pulls, [image['image'] for image in self.lock['images']])
        self.assertEqual(result['pull_count'], 6)
        self.assertFalse(result['save_ready'])
        self.assertFalse((self.directory / 'images.tar').exists())
        cache.verify_images(self.docker, self.lock['images'])

    def test_warm_archive_works_without_repo_digests_or_any_pull(self):
        self.make_archive()
        result = self.prepare()
        self.assertEqual(result['mode'], 'archive')
        self.assertEqual(result['pull_count'], 0)
        self.assertEqual(self.docker.loads, 1)
        self.assertFalse(self.docker.pulls)
        cache.verify_images(self.docker, self.lock['images'])
        self.assertFalse(result['save_ready'])
        self.assertFalse((self.directory / 'images.tar').exists())

    def test_loaded_identity_platform_or_rootfs_mismatch_forces_verified_pulls(self):
        self.make_archive()
        for fault in ('id', 'arch', 'rootfs'):
            with self.subTest(fault=fault):
                self.docker.loaded_fault = fault
                self.docker.pulls.clear()
                result = self.prepare()
                self.assertEqual(result['mode'], 'pull')
                self.assertEqual(result['pull_count'], 6)
                self.assertTrue(result['cache_error'])
                cache.verify_images(self.docker, self.lock['images'])

    def test_corrupt_archive_rejected_before_docker_load(self):
        self.make_archive()
        with (self.directory / 'images.tar').open('ab') as stream:
            stream.write(b'corruption')
        result = self.prepare()
        self.assertEqual(self.docker.loads, 0)
        self.assertEqual(result['pull_count'], 6)
        self.assertTrue(result['cache_error'])

    def test_wrong_lock_metadata_is_not_loaded(self):
        self.make_archive()
        path = self.directory / 'metadata.json'
        metadata = json.loads(path.read_text())
        metadata['lock_sha256'] = '0'*64
        path.write_text(json.dumps(metadata))
        result = self.prepare()
        self.assertEqual(self.docker.loads, 0)
        self.assertEqual(result['pull_count'], 6)

    def test_extra_archive_image_is_rejected_even_with_matching_file_checksum(self):
        self.make_archive()
        path = self.directory / 'images.tar'
        with tarfile.open(path, 'r:') as archive:
            manifest = json.load(archive.extractfile('manifest.json'))
        manifest.append(dict(manifest[0], RepoTags=['unrelated/image:latest']))
        content = json.dumps(manifest).encode()
        with tarfile.open(path, 'a') as archive:
            member = tarfile.TarInfo('manifest.json')
            member.size = len(content)
            archive.addfile(member, io.BytesIO(content))
        metadata_path = self.directory / 'metadata.json'
        metadata = json.loads(metadata_path.read_text())
        metadata.update(archive_bytes=path.stat().st_size, archive_sha256=cache.sha256_file(path))
        metadata_path.write_text(json.dumps(metadata))
        result = self.prepare()
        self.assertEqual(self.docker.loads, 0)
        self.assertEqual(result['pull_count'], 6)

    def test_oversized_archive_is_not_saved_and_verified_images_stay_usable(self):
        result = self.prepare(write=True, max_bytes=10)
        self.assertGreater(result['archive_bytes'], 10)
        self.assertFalse(result['save_ready'])
        self.assertFalse((self.directory / 'images.tar').exists())
        self.assertFalse((self.directory / 'images.partial.tar').exists())
        cache.verify_images(self.docker, self.lock['images'])

    def test_archive_save_error_does_not_fail_already_verified_runtime_images(self):
        self.docker.save_error = True
        result = self.prepare(write=True)
        self.assertFalse(result['save_ready'])
        self.assertIn('archive_save_error', result)
        cache.verify_images(self.docker, self.lock['images'])

    def test_wrong_registry_image_fails_instead_of_running_unverified_image(self):
        self.docker.pull_fault = True
        with self.assertRaisesRegex(ValueError, 'config ID'):
            self.prepare()

    def test_non_amd64_server_rejected_before_pull(self):
        self.docker.server_platform = 'linux/arm64'
        with self.assertRaisesRegex(ValueError, 'linux/amd64'):
            self.prepare()
        self.assertFalse(self.docker.pulls)

    def test_mutable_refs_wrong_platform_and_missing_images_rejected(self):
        for mutation in ('mutable', 'platform', 'missing'):
            with self.subTest(mutation=mutation):
                value = json.loads(json.dumps(self.lock))
                if mutation == 'mutable':
                    value['images'][0]['image'] = 'example/localstack:latest'
                elif mutation == 'platform':
                    value['platform'] = 'linux/arm64'
                else:
                    value['images'].pop()
                self.path.write_text(json.dumps(value))
                with self.assertRaises(ValueError):
                    self.prepare()
        self.assertFalse(self.docker.pulls)

    def test_exports_only_six_digest_derived_local_tags(self):
        metrics = self.prepare()
        environment = self.root / 'environment'
        outputs = self.root / 'outputs'
        with patch.dict(os.environ, {'GITHUB_ENV': str(environment), 'GITHUB_OUTPUT': str(outputs), 'GITHUB_STEP_SUMMARY': ''}):
            cache.export_images(self.lock, metrics)
        lines = environment.read_text().splitlines()
        self.assertEqual(len(lines), 6)
        for image in self.lock['images']:
            self.assertIn(f"QUEUELOOM_{image['name'].upper()}_IMAGE={cache.local_tag(image)}", lines)
        self.assertIn('save-ready=false', outputs.read_text())

    def test_all_six_workflow_containers_use_verified_image_as_first_positional_argument(self):
        workflow = (Path(__file__).resolve().parents[2] / 'workflows' / 'ci.yml').read_text(encoding='utf-8')
        lines = iter(workflow.splitlines())
        commands = []
        for line in lines:
            if not line.strip().startswith('docker run '):
                continue
            command = line.strip()
            while command.endswith('\\'):
                command = command[:-1] + next(lines).strip()
            commands.append(shlex.split(command))
        self.assertEqual(len(commands), 6)
        value_options = {'--name', '--network', '-p', '-e', '-v'}
        seen = set()
        for tokens in commands:
            name = tokens[tokens.index('--name')+1]
            seen.add(name)
            self.assertIn('--pull=never', tokens)
            self.assertIn('--platform=linux/amd64', tokens)
            position = 2
            while tokens[position].startswith('-'):
                position += 2 if tokens[position] in value_options else 1
            self.assertEqual(tokens[position], f'$QUEUELOOM_{name.upper()}_IMAGE')
            if name != 'pubsub':
                self.assertEqual(position, len(tokens)-1, 'Unexpected image/command left after verified image')
            else:
                self.assertEqual(tokens[position+1:position+5], ['gcloud', 'beta', 'emulators', 'pubsub'])
        self.assertEqual(seen, set(cache.NAMES))

if __name__ == '__main__':
    unittest.main()
