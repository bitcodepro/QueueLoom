"""Structural regression checks for GitHub's implicit success() + needs gate.

These model dependency results; they do not dispatch or publish a test release.
"""
from pathlib import Path
import unittest
import yaml

ROOT = Path(__file__).resolve().parents[2]


class WorkflowTests(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.workflow = yaml.load((ROOT / 'workflows/ci.yml').read_text(), Loader=yaml.BaseLoader)
        cls.jobs = cls.workflow['jobs']

    def test_no_tag_or_other_untrusted_release_trigger(self):
        triggers = self.workflow['on']
        self.assertEqual(set(triggers), {'push', 'pull_request', 'workflow_dispatch'})
        self.assertEqual(triggers['push']['branches'], ['main'])
        self.assertNotIn('tags', triggers['push'])
        self.assertFalse((ROOT / 'workflows/release.yml').exists())
        self.assertIn('release_version', triggers['workflow_dispatch']['inputs'])

    def test_all_required_jobs_gate_the_only_writer(self):
        required = {'build-and-test', 'emulator-tests', 'release-policy', 'version',
                    'release-packages', 'verify-release-packages'}
        publisher = self.jobs['release']
        self.assertEqual(set(publisher['needs']), required)
        self.assertEqual(publisher['if'], "github.ref == 'refs/heads/main' && (github.event_name == 'push' || github.event_name == 'workflow_dispatch')")
        self.assertNotIn('always(', publisher['if'])
        self.assertNotIn('continue-on-error', publisher)
        writers = {name for name, job in self.jobs.items()
                   if job.get('permissions', {}).get('contents') == 'write'}
        self.assertEqual(writers, {'release'})
        # With no status override, GitHub applies success() to every needs result.
        for prerequisite in required:
            self.assertNotIn('continue-on-error', self.jobs[prerequisite])
            for state in ('missing', 'queued', 'in_progress', 'failure', 'cancelled', 'skipped'):
                results = dict.fromkeys(required, 'success')
                results[prerequisite] = state
                with self.subTest(job=prerequisite, state=state):
                    self.assertFalse(all(result == 'success' for result in results.values()))
        self.assertTrue(all(result == 'success' for result in dict.fromkeys(required, 'success').values()))

    def test_release_package_verification_is_read_only_and_waits_for_packages(self):
        verify = self.jobs['verify-release-packages']
        self.assertEqual(verify['needs'], 'release-packages')
        self.assertEqual(self.workflow['permissions']['contents'], 'read')
        self.assertTrue(any('verify-packages.ps1' in step.get('run', '') for step in verify['steps']))

    def test_every_checkout_is_the_run_sha_without_persisted_credentials(self):
        package = yaml.load((ROOT / 'workflows/package.yml').read_text(), Loader=yaml.BaseLoader)
        for name, job in {**self.jobs, 'reusable-package': package['jobs']['package']}.items():
            for step in job.get('steps', []):
                if step.get('uses', '').startswith('actions/checkout@'):
                    with self.subTest(job=name):
                        self.assertEqual(step['with']['ref'], '${{ github.sha }}')
                        self.assertEqual(step['with']['persist-credentials'], 'false')

    def test_publisher_uses_current_run_artifacts_and_never_executes_packages(self):
        steps = self.jobs['release']['steps']
        downloads = [step for step in steps if step.get('uses', '').startswith('actions/download-artifact@')]
        self.assertEqual(len(downloads), 1)
        self.assertNotIn('run-id', downloads[0]['with'])
        self.assertNotIn('repository', downloads[0]['with'])
        commands = [step['run'] for step in steps if 'run' in step]
        self.assertEqual(commands, ['python3 -B .github/scripts/publish-release.py'])
        self.assertNotIn('workflow-runs', (ROOT / 'scripts/publish-release.py').read_text())


if __name__ == '__main__':
    unittest.main()
