import importlib.util
from pathlib import Path
import unittest
from unittest.mock import Mock, patch
import subprocess
import urllib.error

SCRIPT = Path(__file__).resolve().parents[1] / 'publish-release.py'
spec = importlib.util.spec_from_file_location('publish_release', SCRIPT)
release = importlib.util.module_from_spec(spec)
spec.loader.exec_module(release)
SHA = 'a' * 40
MAIN = 'b' * 40
OLDER = 'c' * 40
ENV = {'GITHUB_REPOSITORY': 'bitcodepro/QueueLoom', 'GITHUB_REF': 'refs/heads/main',
       'GITHUB_EVENT_NAME': 'push', 'GITHUB_SHA': SHA, 'RELEASE_TAG': 'v1.2.3',
       'GITHUB_WORKFLOW_REF': 'bitcodepro/QueueLoom/.github/workflows/ci.yml@refs/heads/main'}


class PublisherTests(unittest.TestCase):
    def api(self, *, status='ahead', merge_base=SHA, tag=None, releases=None, moved=None,
            tags=None, ancestry=None, objects=None):
        api = Mock()
        existing = dict(tags or {})
        created = {}
        release_list = []
        for item in releases or []:
            item = dict(item)
            item.setdefault('prerelease', '-' in item['tag_name'])
            release_list.append(item)
        def reference(name, obj):
            return {'ref': 'refs/tags/' + name,
                    'object': {'type': 'commit', 'sha': obj} if isinstance(obj, str) else obj}
        def request(method, path, data=None, **kwargs):
            if path == 'git/ref/heads/main':
                return {'object': {'sha': MAIN}}
            if path == f'compare/{SHA}...{MAIN}':
                return {'status': status, 'merge_base_commit': {'sha': merge_base}}
            if path == 'git/matching-refs/tags/v':
                return [reference(name, obj) for name, obj in {**existing, **created}.items()]
            if path.startswith('git/ref/tags/'):
                name = path.removeprefix('git/ref/tags/')
                if name in created:
                    return {'object': {'type': 'commit', 'sha': moved or SHA}}
                if name in existing:
                    return reference(name, existing[name])
                if name == ENV['RELEASE_TAG'] and tag is not None:
                    return tag
                if kwargs.get('missing_ok'):
                    return None
                raise RuntimeError('HTTP 404: missing tag')
            if path.startswith('git/tags/'):
                return {'object': objects[path.removeprefix('git/tags/')]}
            if path.startswith('compare/'):
                base = path.removeprefix('compare/').split('...')[0]
                state, merge = (ancestry or {}).get(base, ('ahead', base))
                return {'status': state, 'merge_base_commit': {'sha': merge}}
            if path.startswith('releases?'):
                return release_list
            if method == 'POST' and path == 'git/refs':
                created[data['ref'].removeprefix('refs/tags/')] = data['sha']
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
        self.assertIn('--latest', command)
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
                return [{'tag_name': 'other', 'draft': True}] * 100
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
        api, run = self.api(), Mock()
        release.publish(api, {**ENV, 'GITHUB_EVENT_NAME': 'workflow_dispatch',
                              'RELEASE_TAG': 'v1.2.3-rc.1'}, ['archive'], run)
        self.assertIn('--prerelease', run.call_args.args[0])
        self.assertIn('--latest=false', run.call_args.args[0])

    def test_stable_must_exceed_remote_maximum_even_if_version_job_was_earlier(self):
        for requested in ['v1.0.1', 'v1.5.0']:
            api, run = self.api(tags={'v1.5.0': OLDER, 'v1.4.99': OLDER}), Mock()
            with self.assertRaisesRegex(RuntimeError, 'highest stable v1.5.0|Tag v1.5.0 already exists'):
                release.publish(api, {**ENV, 'RELEASE_TAG': requested}, ['archive'], run)
            self.assertFalse(any(call.args[0] == 'POST' for call in api.request.call_args_list))
            run.assert_not_called()

    def test_numeric_order_not_lexicographic_and_reserved_stable_tags_count(self):
        api, run = self.api(tags={'v1.10.0': OLDER, 'v1.9.99': OLDER}), Mock()
        with self.assertRaisesRegex(RuntimeError, 'highest stable v1.10.0'):
            release.publish(api, {**ENV, 'RELEASE_TAG': 'v1.9.100'}, ['archive'], run)
        run.assert_not_called()
        release.publish(self.api(tags={'v1.9.99': OLDER}),
                        {**ENV, 'RELEASE_TAG': 'v1.10.0'}, ['archive'], Mock())

    def test_old_main_run_cannot_relabel_old_code_after_newer_release(self):
        api, run = self.api(tags={'v1.2.5': MAIN},
                            releases=[{'tag_name': 'v1.2.5', 'draft': False}],
                            ancestry={MAIN: ('behind', SHA)}), Mock()
        with self.assertRaisesRegex(RuntimeError, 'older than or diverges from v1.2.5'):
            release.publish(api, {**ENV, 'RELEASE_TAG': 'v1.2.6'}, ['archive'], run)
        run.assert_not_called()
        self.assertFalse(any(call.args[0] == 'POST' for call in api.request.call_args_list))

    def test_every_published_release_counts_including_prerelease_and_lower_version(self):
        # A later-published small version/RC can carry newer code. Latest label,
        # target_commitish and release list order must not hide that commit.
        for previous in ['v1.0.0', 'v2.0.0-rc.1']:
            api, run = self.api(tags={'v1.2.2': OLDER, previous: MAIN},
                                releases=[{'tag_name': 'v1.2.2', 'draft': False},
                                          {'tag_name': previous, 'draft': False,
                                           'target_commitish': 'main'}],
                                ancestry={MAIN: ('behind', SHA)}), Mock()
            with self.assertRaisesRegex(RuntimeError, 'older than or diverges'):
                release.publish(api, ENV, ['archive'], run)
            run.assert_not_called()

    def test_published_code_on_later_release_page_cannot_be_hidden(self):
        api, run = self.api(tags={'v1.2.2': OLDER, 'v2.0.0-rc.1': MAIN},
                            ancestry={MAIN: ('behind', SHA)}), Mock()
        original = api.request.side_effect
        def request(method, path, *args, **kwargs):
            if path == 'releases?per_page=100&page=1':
                return [{'tag_name': 'noise', 'draft': True}] * 100
            if path == 'releases?per_page=100&page=2':
                return [{'tag_name': 'v2.0.0-rc.1', 'draft': False, 'prerelease': True}]
            return original(method, path, *args, **kwargs)
        api.request.side_effect = request
        with self.assertRaisesRegex(RuntimeError, 'older than or diverges'):
            release.publish(api, ENV, ['archive'], run)
        run.assert_not_called()

    def test_unknown_draft_status_never_hides_release_history(self):
        for value in [None, 'false', 0]:
            api, run = self.api(releases=[{'tag_name': 'v2.0.0-rc.1', 'draft': value}]), Mock()
            with self.assertRaisesRegex(RuntimeError, 'Invalid release draft status'):
                release.publish(api, ENV, ['archive'], run)
            run.assert_not_called()

    def test_descendant_code_allowed_and_annotated_tag_peeled(self):
        annotation = 'd' * 40
        api, run = self.api(tags={'v1.2.1': OLDER,
                                'v1.2.2': {'type': 'tag', 'sha': annotation}},
                            releases=[{'tag_name': 'v1.2.1', 'draft': False},
                                      {'tag_name': 'v1.2.2', 'draft': False}],
                            objects={annotation: {'type': 'commit', 'sha': OLDER}}), Mock()
        release.publish(api, ENV, ['archive'], run)
        api.request.assert_any_call('GET', f'git/tags/{annotation}')
        api.request.assert_any_call('GET', f'compare/{OLDER}...{SHA}')
        run.assert_called_once()

    def test_published_stable_sha_cannot_be_reissued_with_a_higher_version(self):
        annotation = 'd' * 40
        for obj in [SHA, {'type': 'tag', 'sha': annotation}]:
            for requested in ['v1.2.3', 'v1.3.0', 'v2.0.0']:
                with self.subTest(obj=obj, requested=requested):
                    api, run = self.api(tags={'v1.2.2': obj},
                                        releases=[{'tag_name': 'v1.2.2', 'draft': False, 'prerelease': False}],
                                        objects={annotation: {'type': 'commit', 'sha': SHA}}), Mock()
                    with self.assertRaisesRegex(release.DuplicateStableRelease, 'already has published stable release v1.2.2') as caught:
                        release.publish(api, {**ENV, 'RELEASE_TAG': requested}, ['archive'], run)
                    self.assertIn('new tested main commit', str(caught.exception))
                    self.assertNotIn('release_version=', str(caught.exception))
                    self.assertFalse(any(call.args[0] != 'GET' for call in api.request.call_args_list))
                    run.assert_not_called()

    def test_published_prerelease_can_be_promoted_to_stable_at_same_sha(self):
        api, run = self.api(tags={'v1.2.2': OLDER, 'v1.2.3-rc.1': SHA},
                            releases=[{'tag_name': 'v1.2.2', 'draft': False, 'prerelease': False},
                                      {'tag_name': 'v1.2.3-rc.1', 'draft': False, 'prerelease': True}]), Mock()
        release.publish(api, ENV, ['archive'], run)
        api.request.assert_any_call('POST', 'git/refs', {'ref': 'refs/tags/v1.2.3', 'sha': SHA})
        self.assertIn('--latest', run.call_args.args[0])
        self.assertNotIn('--prerelease', run.call_args.args[0])
        run.assert_called_once()

    def test_same_sha_reserved_tag_or_draft_does_not_prevent_recovery_with_new_version(self):
        for releases in [[], [{'tag_name': 'v1.2.2', 'draft': True, 'prerelease': False}]]:
            api, run = self.api(tags={'v1.2.2': SHA}, releases=releases), Mock()
            release.publish(api, ENV, ['archive'], run)
            run.assert_called_once()
            self.assertFalse(any(call.args[0] in ('PATCH', 'DELETE') for call in api.request.call_args_list))

    def test_published_channel_uses_release_flags_instead_of_tag_suffix(self):
        api, run = self.api(tags={'v1.2.2-rc.1': SHA},
                            releases=[{'tag_name': 'v1.2.2-rc.1', 'draft': False, 'prerelease': False}]), Mock()
        with self.assertRaises(release.DuplicateStableRelease):
            release.publish(api, ENV, ['archive'], run)
        run.assert_not_called()
        self.assertFalse(any(call.args[0] != 'GET' for call in api.request.call_args_list))

        api, run = self.api(tags={'v1.2.2': SHA},
                            releases=[{'tag_name': 'v1.2.2', 'draft': False, 'prerelease': True}]), Mock()
        release.publish(api, ENV, ['archive'], run)
        run.assert_called_once()
        self.assertIn('--latest', run.call_args.args[0])

    def test_prerelease_channel_is_not_subject_to_stable_sha_duplicate_rule(self):
        api, run = self.api(tags={'v1.2.2': SHA},
                            releases=[{'tag_name': 'v1.2.2', 'draft': False, 'prerelease': False}]), Mock()
        release.publish(api, {**ENV, 'RELEASE_TAG': 'v1.2.3-rc.1'}, ['archive'], run)
        self.assertIn('--prerelease', run.call_args.args[0])
        self.assertIn('--latest=false', run.call_args.args[0])

    def test_same_sha_stable_published_after_reservation_prevents_duplicate(self):
        api, run = self.api(tags={'v1.2.2': SHA}), Mock()
        original = api.request.side_effect
        def request(method, path, *args, **kwargs):
            if path.startswith('releases?') and any(call.args[0] == 'POST' for call in api.request.call_args_list):
                return [{'tag_name': 'v1.2.2', 'draft': False, 'prerelease': False}]
            return original(method, path, *args, **kwargs)
        api.request.side_effect = request
        with self.assertRaisesRegex(release.DuplicateStableRelease, 'Any reserved tag remains'):
            release.publish(api, ENV, ['archive'], run)
        self.assertEqual(sum(call.args[0] == 'POST' for call in api.request.call_args_list), 1)
        self.assertFalse(any(call.args[0] in ('PATCH', 'DELETE') for call in api.request.call_args_list))
        run.assert_not_called()

    def test_same_sha_stable_on_later_release_page_is_not_ignored(self):
        api, run = self.api(tags={'v1.2.2': SHA}), Mock()
        original = api.request.side_effect
        def request(method, path, *args, **kwargs):
            if path == 'releases?per_page=100&page=1':
                return [{'tag_name': 'noise', 'draft': True}] * 100
            if path == 'releases?per_page=100&page=2':
                return [{'tag_name': 'v1.2.2', 'draft': False, 'prerelease': False}]
            return original(method, path, *args, **kwargs)
        api.request.side_effect = request
        with self.assertRaises(release.DuplicateStableRelease):
            release.publish(api, ENV, ['archive'], run)
        self.assertFalse(any(call.args[0] != 'GET' for call in api.request.call_args_list))
        run.assert_not_called()

    def test_unknown_published_prerelease_status_fails_closed(self):
        for value in [None, 'false', 0]:
            api, run = self.api(tags={'v1.2.2': SHA},
                                releases=[{'tag_name': 'v1.2.2', 'draft': False, 'prerelease': value}]), Mock()
            with self.assertRaisesRegex(RuntimeError, 'Invalid release prerelease status'):
                release.publish(api, ENV, ['archive'], run)
            self.assertFalse(any(call.args[0] != 'GET' for call in api.request.call_args_list))
            run.assert_not_called()

    def test_missing_or_unresolvable_published_tag_fails_closed(self):
        for tags in [{}, {'v1.2.2': {'type': 'tree', 'sha': OLDER}}]:
            api, run = self.api(tags=tags, releases=[{'tag_name': 'v1.2.2', 'draft': False}]), Mock()
            with self.assertRaises(RuntimeError):
                release.publish(api, ENV, ['archive'], run)
            run.assert_not_called()

    def test_higher_stable_reserved_after_our_reservation_stops_publication(self):
        api, run = self.api(), Mock()
        original = api.request.side_effect
        def request(method, path, *args, **kwargs):
            value = original(method, path, *args, **kwargs)
            if path == 'git/matching-refs/tags/v' and any(call.args[0] == 'POST' for call in api.request.call_args_list):
                value.append({'ref': 'refs/tags/v1.2.4', 'object': {'type': 'commit', 'sha': MAIN}})
            return value
        api.request.side_effect = request
        with self.assertRaisesRegex(RuntimeError, 'highest stable v1.2.4') as caught:
            release.publish(api, ENV, ['archive'], run)
        self.assertIn('release_version=1.2.5', str(caught.exception))
        run.assert_not_called()
        self.assertFalse(any(call.args[0] in ('PATCH', 'DELETE') for call in api.request.call_args_list))

    def test_new_published_code_after_reservation_stops_even_prerelease(self):
        api, run = self.api(tags={'v1.2.2': OLDER, 'v2.0.0-rc.1': MAIN},
                            ancestry={MAIN: ('behind', SHA)}), Mock()
        original = api.request.side_effect
        def request(method, path, *args, **kwargs):
            if path.startswith('releases?') and any(call.args[0] == 'POST' for call in api.request.call_args_list):
                return [{'tag_name': 'v2.0.0-rc.1', 'draft': False, 'prerelease': True}]
            return original(method, path, *args, **kwargs)
        api.request.side_effect = request
        with self.assertRaisesRegex(RuntimeError, 'older than or diverges'):
            release.publish(api, {**ENV, 'RELEASE_TAG': 'v2.0.0-rc.2'}, ['archive'], run)
        run.assert_not_called()

    def test_failure_after_reservation_reports_fresh_unused_version_without_cleanup(self):
        api, run = self.api(), Mock(side_effect=subprocess.CalledProcessError(1, ['gh', 'release', 'create']))
        with self.assertRaisesRegex(RuntimeError, 'release_version=1.2.4') as caught:
            release.publish(api, ENV, ['archive'], run)
        self.assertIn('fresh CI on main', str(caught.exception))
        self.assertFalse(any(call.args[0] in ('PATCH', 'DELETE') for call in api.request.call_args_list))

    def test_retry_published_stable_inspects_existing_release_without_version_hint(self):
        api, run = self.api(tags={'v1.2.3': SHA},
                            releases=[{'tag_name': 'v1.2.3', 'draft': False, 'prerelease': False}]), Mock()
        with self.assertRaisesRegex(RuntimeError, 'Tag v1.2.3 already exists') as caught:
            release.publish(api, ENV, ['archive'], run)
        self.assertIn('Inspect the existing release', str(caught.exception))
        self.assertIn('new tested main commit', str(caught.exception))
        self.assertNotIn('release_version=', str(caught.exception))
        self.assertFalse(any(call.args[0] != 'GET' for call in api.request.call_args_list))
        run.assert_not_called()

    def test_server_published_stable_but_cli_lost_response_does_not_suggest_same_sha(self):
        api = self.api()
        original = api.request.side_effect
        published = False
        def request(method, path, *args, **kwargs):
            if path.startswith('releases?') and published:
                return [{'tag_name': 'v1.2.3', 'draft': False, 'prerelease': False}]
            return original(method, path, *args, **kwargs)
        def lose_response(*args, **kwargs):
            nonlocal published
            published = True
            raise subprocess.CalledProcessError(1, ['gh', 'release', 'create'])
        api.request.side_effect = request
        run = Mock(side_effect=lose_response)
        with self.assertRaises(RuntimeError) as caught:
            release.publish(api, ENV, ['archive'], run)
        self.assertIn('Inspect the existing release', str(caught.exception))
        self.assertIn('publication may have succeeded despite the error', str(caught.exception))
        self.assertIn('new tested main commit', str(caught.exception))
        self.assertNotIn('release_version=', str(caught.exception))
        self.assertEqual(sum(call.args[0] == 'POST' for call in api.request.call_args_list), 1)
        self.assertFalse(any(call.args[0] in ('PATCH', 'DELETE') for call in api.request.call_args_list))
        run.assert_called_once()

    def test_reserved_or_draft_retry_still_suggests_next_unused_version(self):
        for releases in [[], [{'tag_name': 'v1.2.3', 'draft': True, 'prerelease': False}]]:
            api, run = self.api(tags={'v1.2.3': SHA}, releases=releases), Mock()
            with self.assertRaises(RuntimeError) as caught:
                release.publish(api, ENV, ['archive'], run)
            self.assertIn('release_version=1.2.4', str(caught.exception))
            self.assertNotIn('new tested main commit', str(caught.exception))
            self.assertFalse(any(call.args[0] != 'GET' for call in api.request.call_args_list))
            run.assert_not_called()

    def test_prerelease_same_sha_does_not_consume_stable_recovery(self):
        api, run = self.api(tags={'v1.2.3-rc.1': SHA},
                            releases=[{'tag_name': 'v1.2.3-rc.1', 'draft': False, 'prerelease': True}]), Mock(
                                side_effect=subprocess.CalledProcessError(1, ['gh', 'release', 'create']))
        with self.assertRaises(RuntimeError) as caught:
            release.publish(api, ENV, ['archive'], run)
        self.assertIn('release_version=1.2.4', str(caught.exception))
        self.assertNotIn('new tested main commit', str(caught.exception))
        run.assert_called_once()

    def test_prerelease_recovery_skips_occupied_suggestions(self):
        api = self.api(tags={'v1.2.3-rc.1.1': SHA},
                       releases=[{'tag_name': 'v1.2.3-rc.1.1.1', 'draft': True}])
        self.assertIn('release_version=1.2.3-rc.1.1.1.1', release.recovery_hint(api, 'v1.2.3-rc.1', SHA))

    def test_recovery_api_failure_never_turns_failure_into_publication(self):
        api, run = self.api(), Mock()
        original = api.request.side_effect
        def request(method, path, *args, **kwargs):
            if path == 'git/matching-refs/tags/v':
                raise RuntimeError('HTTP 403')
            return original(method, path, *args, **kwargs)
        api.request.side_effect = request
        with self.assertRaisesRegex(RuntimeError, 'Could not determine the next unused version'):
            release.publish(api, ENV, ['archive'], run)
        run.assert_not_called()

    def test_two_publishers_selecting_one_version_have_one_winner_and_safe_recovery(self):
        api = self.api()
        first, second = Mock(), Mock()
        release.publish(api, ENV, ['archive'], first)
        with self.assertRaisesRegex(RuntimeError, 'release_version=1.2.4'):
            release.publish(api, ENV, ['archive'], second)
        self.assertEqual(sum(call.args[0] == 'POST' for call in api.request.call_args_list), 1)
        first.assert_called_once()
        second.assert_not_called()

    def test_main_divergence_after_reservation_stops_publication(self):
        api, run = self.api(), Mock()
        original = api.request.side_effect
        def request(method, path, *args, **kwargs):
            if path == f'compare/{SHA}...{MAIN}' and any(call.args[0] == 'POST' for call in api.request.call_args_list):
                return {'status': 'diverged', 'merge_base_commit': {'sha': OLDER}}
            return original(method, path, *args, **kwargs)
        api.request.side_effect = request
        with self.assertRaisesRegex(RuntimeError, 'no longer an ancestor'):
            release.publish(api, ENV, ['archive'], run)
        run.assert_not_called()

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
