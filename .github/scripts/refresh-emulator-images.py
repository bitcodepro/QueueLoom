#!/usr/bin/env python3
"""Explicitly refresh the six public CI images, or freeze known green-CI digests.

Uses anonymous Registry V2 access; no Docker daemon, user credentials or tokens
are written to disk. Review the lock diff and run all emulator tests before merge.
"""
import argparse
import hashlib
import json
from pathlib import Path
import re
import urllib.error
import urllib.parse
import urllib.request

SOURCES = {
    'localstack': ('localstack/localstack:4.14', 'registry-1.docker.io', 'localstack/localstack', '4.14'),
    'rabbitmq': ('rabbitmq:4-management', 'registry-1.docker.io', 'library/rabbitmq', '4-management'),
    'kafka': ('apache/kafka:3.9.2', 'registry-1.docker.io', 'apache/kafka', '3.9.2'),
    'pubsub': ('gcr.io/google.com/cloudsdktool/google-cloud-cli:emulators', 'gcr.io', 'google.com/cloudsdktool/google-cloud-cli', 'emulators'),
    'sqlserver': ('mcr.microsoft.com/mssql/server:2022-latest', 'mcr.microsoft.com', 'mssql/server', '2022-latest'),
    'servicebus': ('mcr.microsoft.com/azure-messaging/servicebus-emulator:latest', 'mcr.microsoft.com', 'azure-messaging/servicebus-emulator', 'latest'),
}
ACCEPT = ', '.join(('application/vnd.oci.image.index.v1+json', 'application/vnd.docker.distribution.manifest.list.v2+json', 'application/vnd.oci.image.manifest.v1+json', 'application/vnd.docker.distribution.manifest.v2+json'))

class Registry:
    def __init__(self, host, repository):
        self.host, self.repository = host, repository
        self.token = None

    def get(self, kind, reference):
        url = f'https://{self.host}/v2/{self.repository}/{kind}/{urllib.parse.quote(reference, safe=":")}'
        headers = {'Accept': ACCEPT}
        if self.token:
            headers['Authorization'] = f'Bearer {self.token}'
        try:
            response = urllib.request.urlopen(urllib.request.Request(url, headers=headers), timeout=60)
        except urllib.error.HTTPError as error:
            if error.code != 401 or self.token:
                raise
            challenge = error.headers.get('WWW-Authenticate', '')
            if not challenge.lower().startswith('bearer '):
                raise RuntimeError(f'Unsupported anonymous authentication for {self.host}') from error
            values = dict(re.findall(r'(\w+)="([^"]*)"', challenge))
            realm = values.pop('realm')
            if urllib.parse.urlparse(realm).scheme != 'https':
                raise RuntimeError('Registry authentication must use HTTPS')
            auth_url = realm + '?' + urllib.parse.urlencode(values)
            with urllib.request.urlopen(auth_url, timeout=60) as auth:
                token = json.load(auth)
            self.token = token.get('token') or token.get('access_token')
            if not self.token:
                raise RuntimeError('Anonymous registry token unavailable')
            return self.get(kind, reference)
        with response:
            body = response.read()
            reported_digest = response.headers.get('Docker-Content-Digest')
        digest = 'sha256:' + hashlib.sha256(body).hexdigest()
        if reference.startswith('sha256:') and digest != reference:
            raise RuntimeError(f'Registry content digest mismatch: {self.host}/{self.repository}')
        if reported_digest and reported_digest != digest:
            raise RuntimeError('Registry digest header differs from content')
        return json.loads(body), digest

def resolve(name, reference=None):
    source, host, repository, tag = SOURCES[name]
    registry = Registry(host, repository)
    manifest, source_digest = registry.get('manifests', reference or tag)
    digest = source_digest
    if 'manifests' in manifest:
        candidates = [entry for entry in manifest['manifests'] if entry.get('platform', {}).get('os') == 'linux' and entry.get('platform', {}).get('architecture') == 'amd64' and entry.get('platform', {}).get('variant') in (None, '', 'v1')]
        if len(candidates) != 1:
            raise RuntimeError(f'Expected one linux/amd64 manifest for {source}')
        manifest, digest = registry.get('manifests', candidates[0]['digest'])
    config_digest = manifest['config']['digest']
    config, actual_config_digest = registry.get('blobs', config_digest)
    if actual_config_digest != config_digest:
        raise RuntimeError('Registry config digest mismatch')
    if config.get('os') != 'linux' or config.get('architecture') != 'amd64':
        raise RuntimeError('Registry config must be linux/amd64')
    diffs = config['rootfs']['diff_ids']
    if not isinstance(diffs, list) or not diffs or not all(isinstance(value, str) and re.fullmatch('sha256:[0-9a-f]{64}', value) for value in diffs):
        raise RuntimeError('Registry rootfs identities are invalid')
    repository_reference = source.rsplit(':', 1)[0]
    labels = config.get('config', {}).get('Labels') or {}
    version_labels = {key: value for key, value in labels.items() if key in ('org.opencontainers.image.version', 'org.opencontainers.image.revision', 'com.microsoft.version', 'com.microsoft.product')}
    version_environment = {}
    for value in config.get('config', {}).get('Env') or []:
        key, separator, text = value.partition('=')
        if separator and key in ('LOCALSTACK_VERSION', 'RABBITMQ_VERSION', 'KAFKA_VERSION', 'CLOUD_SDK_VERSION', 'CLOUDSDK_VERSION', 'MSSQL_VERSION', 'MSSQL_MAJOR_VERSION', 'SERVICE_BUS_EMULATOR_VERSION'):
            version_environment[key] = text
    return {'name': name, 'source_tag': source, 'source_digest': source_digest, 'image': repository_reference+'@'+digest, 'config_id': config_digest, 'rootfs_diff_ids': diffs, 'version_labels': version_labels, 'version_environment': version_environment, 'registry_layer_bytes': sum(layer['size'] for layer in manifest['layers'])}

def verify_checked_lock(result, old):
    if result != old:
        raise RuntimeError('Registry metadata differs from the checked-in lock')

if __name__ == '__main__':
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--lock', type=Path, default=Path(__file__).resolve().parents[1] / 'emulator-images.lock.json')
    modes = parser.add_mutually_exclusive_group(required=True)
    modes.add_argument('--refresh', action='store_true', help='Explicitly resolve the existing six source tags again; review every version change')
    modes.add_argument('--baseline', type=Path, help='JSON object mapping the six names to known green-CI sha256 digests')
    modes.add_argument('--check', action='store_true', help='Verify existing immutable manifest/config/layer identity without modifying the lock')
    args = parser.parse_args()
    old = json.loads(args.lock.read_text()) if args.lock.exists() else None
    refs = json.loads(args.baseline.read_text()) if args.baseline else {}
    if args.check:
        if not old:
            parser.error('No existing lock to check')
        refs = {entry['name']: entry['source_digest'] for entry in old['images']}
    if refs and set(refs) != set(SOURCES):
        parser.error('Baseline/check must contain exactly the six CI images')
    images = []
    for name in SOURCES:
        entry = resolve(name, refs.get(name))
        images.append(entry)
        print(f"{name}: {entry['image']} config={entry['config_id']} version_env={entry['version_environment']} labels={entry['version_labels']}", flush=True)
    result = {'format': 1, 'platform': 'linux/amd64', 'images': images}
    if args.check:
        verify_checked_lock(result, old)
        print('All six locked linux/amd64 manifests, configs and rootfs identities verified.')
    else:
        args.lock.write_text(json.dumps(result, indent=2)+'\n', encoding='utf-8')
        if old:
            previous = {entry['name']: entry for entry in old['images']}
            for entry in images:
                if previous.get(entry['name']) != entry:
                    print(f"REVIEW REQUIRED: changed lock entry {entry['name']}")
