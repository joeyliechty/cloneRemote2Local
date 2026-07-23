import importlib.util
import json
import os
import shutil
import sys
import tempfile
import unittest
from unittest import mock

MODULE_PATH = os.path.join(os.path.dirname(__file__), 'cloneRemote2Local.py')


def _load_module_under_test():
    """Import cloneRemote2Local.py in isolation.

    Stubs any of its third-party runtime deps (dateutil/tqdm/requests) that
    aren't installed in the current environment — none of their real
    behavior is exercised by these tests, only import-time attribute access
    (`from dateutil import parser`, etc.).
    """
    missing = [
        name for name in ('dateutil', 'tqdm', 'requests')
        if importlib.util.find_spec(name) is None
    ]
    patched = {name: mock.MagicMock() for name in missing}
    for name in missing:
        patched['{}.parser'.format(name)] = patched[name] if name == 'dateutil' else mock.MagicMock()
        patched['{}.exceptions'.format(name)] = patched[name] if name == 'requests' else mock.MagicMock()

    with mock.patch.dict(sys.modules, patched):
        spec = importlib.util.spec_from_file_location('cloneRemote2Local_under_test', MODULE_PATH)
        module = importlib.util.module_from_spec(spec)
        spec.loader.exec_module(module)
    return module


target = _load_module_under_test()


class DiffDirectoriesTests(unittest.TestCase):
    def setUp(self):
        self.left = tempfile.mkdtemp()
        self.right = tempfile.mkdtemp()

    def tearDown(self):
        shutil.rmtree(self.left, ignore_errors=True)
        shutil.rmtree(self.right, ignore_errors=True)

    def _write(self, root, rel_path, content):
        full = os.path.join(root, rel_path)
        os.makedirs(os.path.dirname(full), exist_ok=True)
        with open(full, 'w') as f:
            f.write(content)
        return full

    def test_same_size_and_mtime_but_different_content_is_flagged(self):
        # This is the exact failure mode of filecmp.dircmp's default shallow
        # comparison: it trusts os.stat() (size + mtime) and never reads
        # bytes, so two files with equal size/mtime but different content
        # were previously reported as identical.
        left_path = self._write(self.left, 'a/file.txt', 'AAAA')
        right_path = self._write(self.right, 'a/file.txt', 'BBBB')
        same_mtime = 1_000_000
        os.utime(left_path, (same_mtime, same_mtime))
        os.utime(right_path, (same_mtime, same_mtime))

        _, _, diff_files = target._diff_directories(self.left, self.right)

        self.assertEqual(diff_files, [os.path.join('a', 'file.txt')])

    def test_identical_content_is_not_flagged_despite_differing_mtime(self):
        # Fresh tar extractions on each side almost never share mtimes even
        # when content is byte-identical — comparison must not be fooled by
        # that in either direction.
        self._write(self.left, 'a/file.txt', 'same content')
        right_path = self._write(self.right, 'a/file.txt', 'same content')
        os.utime(right_path, (1_000_000, 1_000_000))

        left_only, right_only, diff_files = target._diff_directories(self.left, self.right)

        self.assertEqual((left_only, right_only, diff_files), ([], [], []))

    def test_reports_files_only_present_on_one_side(self):
        self._write(self.left, 'only_left.txt', 'x')
        self._write(self.right, 'only_right.txt', 'y')

        left_only, right_only, diff_files = target._diff_directories(self.left, self.right)

        self.assertEqual(left_only, ['only_left.txt'])
        self.assertEqual(right_only, ['only_right.txt'])
        self.assertEqual(diff_files, [])

    def test_nested_subdirectories_are_compared(self):
        left_path = self._write(self.left, 'nested/deep/file.txt', 'AAAA')
        right_path = self._write(self.right, 'nested/deep/file.txt', 'ZZZZ')
        os.utime(left_path, (1, 1))
        os.utime(right_path, (1, 1))

        _, _, diff_files = target._diff_directories(self.left, self.right)

        self.assertEqual(diff_files, [os.path.join('nested', 'deep', 'file.txt')])


class _FakeClock:
    """A settable stand-in for time.monotonic(), passed as AccessToken's `now`."""

    def __init__(self, start=0.0):
        self.now = start

    def __call__(self):
        return self.now


class AccessTokenTests(unittest.TestCase):
    def test_value_returns_current_token_without_refreshing_before_expiry(self):
        clock = _FakeClock()
        token = target.AccessToken('initial-token', 'refresh-token', now=clock)
        clock.now = target.AccessToken.LIFETIME_SECONDS - target.AccessToken.REFRESH_MARGIN_SECONDS - 1

        with mock.patch.object(target, 'refreshAccessToken') as mocked_refresh:
            value = token.value

        self.assertEqual(value, 'initial-token')
        mocked_refresh.assert_not_called()

    def test_value_refreshes_once_the_expiry_margin_is_reached(self):
        # This is exactly the scenario that broke the real run: a step that
        # started well after authentication (e.g. requesting a distribution
        # download token after a multi-minute backup download) must not use
        # a token that's about to lapse.
        clock = _FakeClock()
        token = target.AccessToken('initial-token', 'refresh-token', now=clock)
        clock.now = target.AccessToken.LIFETIME_SECONDS - target.AccessToken.REFRESH_MARGIN_SECONDS

        with mock.patch.object(target, 'refreshAccessToken', return_value='refreshed-token') as mocked_refresh:
            value = token.value

        self.assertEqual(value, 'refreshed-token')
        mocked_refresh.assert_called_once_with('refresh-token')

    def test_value_does_not_refresh_again_immediately_after_refreshing(self):
        clock = _FakeClock()
        token = target.AccessToken('initial-token', 'refresh-token', now=clock)
        clock.now = target.AccessToken.LIFETIME_SECONDS

        with mock.patch.object(target, 'refreshAccessToken', return_value='refreshed-token') as mocked_refresh:
            token.value
            token.value

        mocked_refresh.assert_called_once()


class RefreshAccessTokenTests(unittest.TestCase):
    def test_posts_grant_type_refresh_token_and_returns_new_access_token(self):
        response = mock.Mock(status_code=200)
        response.json.return_value = {'token_type': 'bearer', 'access_token': 'new-access-token'}
        response.raise_for_status.return_value = None

        with mock.patch.object(target, 'requests') as mocked_requests:
            mocked_requests.post.return_value = response
            result = target.refreshAccessToken('refresh-456')

        self.assertEqual(result, 'new-access-token')
        _, kwargs = mocked_requests.post.call_args
        self.assertEqual(json.loads(kwargs['data']),
                          {'grant_type': 'refresh_token', 'refresh_token': 'refresh-456'})


class AuthenticateCloudAPITests(unittest.TestCase):
    def test_returns_an_access_token_wrapping_both_tokens_from_the_response(self):
        response = mock.Mock(status_code=200)
        response.json.return_value = {
            'token_type': 'bearer',
            'access_token': 'access-123',
            'refresh_token': 'refresh-456',
        }
        response.raise_for_status.return_value = None

        with mock.patch.object(target, 'requests') as mocked_requests:
            mocked_requests.post.return_value = response
            result = target.authenticateCloudAPI('user', 'pass')

        self.assertIsInstance(result, target.AccessToken)
        self.assertEqual(result.value, 'access-123')


class ListEnvironmentsTests(unittest.TestCase):
    def test_logs_a_warning_when_the_api_reports_more_results(self):
        # The docs document 'more'/'total'/'count' fields but never show how
        # to request a further page — rather than silently returning a
        # truncated list, a truncation must be visible in the logs.
        response = mock.Mock(status_code=200)
        response.json.return_value = {'items': [{'name': 'dev'}], 'more': True, 'total': 50, 'count': 25}
        response.raise_for_status.return_value = None
        fake_token = mock.Mock(value='access-123')

        with mock.patch.object(target, 'requests') as mocked_requests, \
                mock.patch.object(target, 'log') as mocked_log:
            mocked_requests.get.return_value = response
            items = target.listEnvironments(fake_token)

        self.assertEqual(items, [{'name': 'dev'}])
        mocked_log.warning.assert_called_once()

    def test_does_not_warn_when_more_is_false(self):
        response = mock.Mock(status_code=200)
        response.json.return_value = {'items': [{'name': 'dev'}], 'more': False, 'total': 1, 'count': 1}
        response.raise_for_status.return_value = None
        fake_token = mock.Mock(value='access-123')

        with mock.patch.object(target, 'requests') as mocked_requests, \
                mock.patch.object(target, 'log') as mocked_log:
            mocked_requests.get.return_value = response
            target.listEnvironments(fake_token)

        mocked_log.warning.assert_not_called()


class GetBackupDownloadLinkTests(unittest.TestCase):
    def test_202_response_is_treated_as_success_not_a_retry_signal(self):
        # Per https://api.cavco.bloomreach.cloud/v3/docs, 202 IS the success
        # status for GET /v3/backups/{id}/repositorydownloadlink — it returns
        # the download URL directly in the body. Treating 202 as "not ready
        # yet" discarded every real response and eventually raised
        # TimeoutError instead of ever returning the URL.
        response = mock.Mock(status_code=202)
        response.json.return_value = {'url': 'https://backups.example.s3.amazonaws.com/backup.gz'}
        response.raise_for_status.return_value = None

        with mock.patch.object(target, 'requests') as mocked_requests, \
                mock.patch.object(target, 'time') as mocked_time:
            mocked_requests.get.return_value = response

            url = target.getBackupDownloadLink(mock.Mock(value='token'), 'backup-id')

        self.assertEqual(url, 'https://backups.example.s3.amazonaws.com/backup.gz')
        mocked_requests.get.assert_called_once()
        mocked_time.sleep.assert_not_called()


class ImportSafetyTests(unittest.TestCase):
    def test_importing_the_script_does_not_parse_argv_or_prompt_for_a_password(self):
        # Regression: CLI parsing + the password prompt used to run at
        # module import time, so merely importing this file (as this test
        # does, under unittest's own argv) used to crash on argparse's
        # required-argument check. All of that must now live behind main().
        module = _load_module_under_test()
        self.assertTrue(callable(module.main))


if __name__ == '__main__':
    unittest.main()
