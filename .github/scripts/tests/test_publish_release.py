import importlib.util
from pathlib import Path
import unittest
from unittest.mock import Mock, patch
import urllib.error

SCRIPT = Path(__file__).resolve().parents[1] / 'publish-release.py'
spec = importlib.util.spec_from_file_location('publish_release', SCRIPT)
release = importlib.util.module_from_spec(spec)
spec.loader.exec_module(release)
SHA = 'a' * 40
MAIN = 'b' * 40
ENV = {'GITHUB_REPOSITORY': 'bitcodepro/QueueLoom', 'GITHUB_REF': 'refs/heads/main',
       'GITHUB_EVENT_NAME': 'push', 'GITHUB_SHA': SHA, 'RELEASE_TAG': 'v1.2.3',
       'GITHUB_WORKFLOW_REF': 'bitcodepro/QueueLoom/.github/workflows/ci.yml@refs/heads/main'}


class PublisherTests(unittest.TestCase):
    def api(self, *, status='ahead', merge_base=SHA, tag=None, releases=None, moved=None):
        api = Mock()
        def request(method, path, data=None, **kwargs):
            if path == 'git/ref/heads/main':
                return {'object': {'sha': MAIN}}
            if path == f'compare/{SHA}...{MAIN}':
                return {'status': status, 'merge_base_commit': {'sha': merge_base}}
            if path == 'git/ref/tags/v1.2.3':
                if any(call.args[0] == 'POST' for call in api.request.call_args_list):
                    return {'object': {'type': 'commit', 'sha': moved or SHA}}
                return tag
            if path.startswith('releases?'):
                return releases or []
            if method == 'POST' and path == 'git/refs':
                return {}
            raise AssertionError((method, path))
        api.request.side_effect = request
        return api

    def test_main_push_creates_exact_sha_tag_and_verifies_before_release(self):
        api, run = self.api(), Mock()
        release.publish(api, ENV, ['packages/archive.zip'], run)
        api.request.assert_any_call('POST', 'git/refs', {'ref': 'refs/tags/v1.2.3', 'sha': SHA})
        command = run.call_args.args[0]
        self.assertIn('--verify-tag', command)
        self.assertNotIn('--prerelease', command)
        self.assertEqual(run.call_args.kwargs, {'check': True})

    def test_identical_main_allowed(self):
        release.publish(self.api(status='identical'), ENV, ['archive'], Mock())

    def test_off_main_pr_fork_and_wrong_workflow_fail_before_api(self):
        for key, values in {
            'GITHUB_REF': ['refs/tags/v1.2.3', 'refs/heads/feature'],
            'GITHUB_EVENT_NAME': ['pull_request', 'workflow_run'],
            'GITHUB_REPOSITORY': ['attacker/QueueLoom'],
            'GITHUB_WORKFLOW_REF': ['bitcodepro/QueueLoom/.github/workflows/other.yml@refs/heads/main'],
            'GITHUB_SHA': ['main', 'a' * 39],
            'RELEASE_TAG': ['v1.2.3;echo', 'v01.2.3', 'v1.2.3-01', 'v1.2.3+meta'],
        }.items():
            for value in values:
                with self.subTest(key=key, value=value):
                    api, run = Mock(), Mock()
                    with self.assertRaises(RuntimeError):
                        release.publish(api, {**ENV, key: value}, ['archive'], run)
                    api.request.assert_not_called()
                    run.assert_not_called()

    def test_divergent_behind_or_wrong_merge_base_never_create_tag(self):
        for status, merge_base in [('diverged', MAIN), ('behind', SHA), ('ahead', MAIN)]:
            api, run = self.api(status=status, merge_base=merge_base), Mock()
            with self.assertRaises(RuntimeError):
                release.publish(api, ENV, ['archive'], run)
            self.assertFalse(any(call.args[0] != 'GET' for call in api.request.call_args_list))
            run.assert_not_called()

    def test_existing_tag_even_at_same_sha_and_rerun_fail(self):
        for sha in [SHA, MAIN]:
            api, run = self.api(tag={'object': {'sha': sha}}), Mock()
            with self.assertRaisesRegex(RuntimeError, 'already exists'):
                release.publish(api, ENV, ['archive'], run)
            run.assert_not_called()
            self.assertFalse(any(call.args[0] == 'POST' for call in api.request.call_args_list))

    def test_existing_draft_release_and_pagination_fail(self):
        api, run = self.api(), Mock()
        original = api.request.side_effect
        def request(method, path, *args, **kwargs):
            if path == 'releases?per_page=100&page=1':
                return [{'tag_name': 'other'}] * 100
            if path == 'releases?per_page=100&page=2':
                return [{'tag_name': 'v1.2.3', 'draft': True}]
            return original(method, path, *args, **kwargs)
        api.request.side_effect = request
        with self.assertRaisesRegex(RuntimeError, 'including drafts'):
            release.publish(api, ENV, ['archive'], run)
        run.assert_not_called()

    def test_racing_tag_creation_fails_without_reuse_or_move(self):
        api, run = self.api(), Mock()
        original = api.request.side_effect
        def request(method, path, *args, **kwargs):
            if method == 'POST':
                raise RuntimeError('HTTP 422')
            return original(method, path, *args, **kwargs)
        api.request.side_effect = request
        with self.assertRaisesRegex(RuntimeError, '422'):
            release.publish(api, ENV, ['archive'], run)
        run.assert_not_called()
        self.assertFalse(any(call.args[0] in ('PATCH', 'DELETE') for call in api.request.call_args_list))

    def test_moved_tag_stops_publication(self):
        api, run = self.api(moved=MAIN), Mock()
        with self.assertRaisesRegex(RuntimeError, 'changed'):
            release.publish(api, ENV, ['archive'], run)
        run.assert_not_called()

    def test_dispatch_prerelease(self):
        api, run = Mock(), Mock()
        api.request.side_effect = [{'object': {'sha': MAIN}},
                                   {'status': 'ahead', 'merge_base_commit': {'sha': SHA}},
                                   None, [], {}, {'object': {'type': 'commit', 'sha': SHA}}]
        release.publish(api, {**ENV, 'GITHUB_EVENT_NAME': 'workflow_dispatch',
                              'RELEASE_TAG': 'v1.2.3-rc.1'}, ['archive'], run)
        self.assertIn('--prerelease', run.call_args.args[0])

    def test_no_assets_fail_without_api(self):
        api = Mock()
        with self.assertRaisesRegex(RuntimeError, 'No verified'):
            release.publish(api, ENV, [], Mock())
        api.request.assert_not_called()

    def test_only_404_can_mean_missing(self):
        api = release.GitHub('bitcodepro/QueueLoom', 'fake')
        for code in (403, 404, 429, 500):
            error = urllib.error.HTTPError(api.base, code, 'failure', {}, None)
            with patch.object(release.urllib.request, 'urlopen', side_effect=error):
                if code == 404:
                    self.assertIsNone(api.request('GET', 'git/ref/tags/test', missing_ok=True))
                else:
                    with self.assertRaisesRegex(RuntimeError, str(code)):
                        api.request('GET', 'git/ref/tags/test', missing_ok=True)


if __name__ == '__main__':
    unittest.main()
