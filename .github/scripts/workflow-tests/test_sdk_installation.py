"""Keep SDK installation exact without changing the project's roll-forward policy."""
import json
from pathlib import Path
import unittest

import yaml


ROOT = Path(__file__).resolve().parents[3]


class SdkInstallationTests(unittest.TestCase):
    def test_all_sdk_installations_use_one_exact_compatible_version(self):
        sdk = json.loads((ROOT / 'global.json').read_text())['sdk']
        self.assertEqual(sdk['rollForward'], 'latestPatch')
        self.assertFalse(sdk['allowPrerelease'])
        minimum = tuple(map(int, sdk['version'].split('.')))
        found = set()
        versions = set()
        for path in sorted((ROOT / '.github/workflows').glob('*.yml')):
            workflow = yaml.load(path.read_text(), Loader=yaml.BaseLoader)
            for job_name, job in workflow['jobs'].items():
                for step in job.get('steps', []):
                    if not step.get('uses', '').startswith('actions/setup-dotnet@'):
                        continue
                    with self.subTest(workflow=path.name, job=job_name):
                        inputs = step.get('with', {})
                        version = inputs.get('dotnet-version', '')
                        self.assertRegex(version, r'^\d+\.\d+\.\d+$')
                        parts = tuple(map(int, version.split('.')))
                        self.assertEqual(parts[:2], minimum[:2])
                        self.assertEqual(parts[2] // 100, minimum[2] // 100)
                        self.assertGreaterEqual(parts, minimum)
                        versions.add(version)
                        # v6 expands latestPatch in global.json into a floating channel,
                        # even when an exact dotnet-version is also supplied.
                        self.assertNotIn('global-json-file', inputs)
                        self.assertNotIn('dotnet-channel', inputs)
                        self.assertNotIn('continue-on-error', step)
                    found.add((path.name, job_name))
        self.assertEqual(len(versions), 1)
        self.assertEqual(found, {
            ('ci.yml', 'build-and-test'),
            ('ci.yml', 'emulator-tests'),
            ('package.yml', 'package'),
        })


if __name__ == '__main__':
    unittest.main()
