import argparse
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


class GetRequiredJavaVersionTests(unittest.TestCase):
    def setUp(self):
        self.project = tempfile.mkdtemp()

    def tearDown(self):
        shutil.rmtree(self.project, ignore_errors=True)

    def _write_pom(self, properties_xml):
        pom_path = os.path.join(self.project, 'pom.xml')
        with open(pom_path, 'w') as f:
            f.write(
                '<project xmlns="http://maven.apache.org/POM/4.0.0">'
                '<properties>{}</properties>'
                '</project>'.format(properties_xml))
        return pom_path

    def test_returns_none_when_no_pom_exists(self):
        self.assertIsNone(target.getRequiredJavaVersion(self.project))

    def test_returns_none_when_pom_has_no_known_java_property(self):
        self._write_pom('<some.other.property>x</some.other.property>')
        self.assertIsNone(target.getRequiredJavaVersion(self.project))

    def test_reads_java_version_property_verbatim(self):
        self._write_pom('<java.version>17</java.version>')
        self.assertEqual(target.getRequiredJavaVersion(self.project), '17')

    def test_normalizes_legacy_1_dot_x_version_strings(self):
        self._write_pom('<maven.compiler.source>1.8</maven.compiler.source>')
        self.assertEqual(target.getRequiredJavaVersion(self.project), '8')

    def test_prefers_java_version_over_compiler_properties(self):
        self._write_pom(
            '<java.version>17</java.version>'
            '<maven.compiler.source>1.8</maven.compiler.source>')
        self.assertEqual(target.getRequiredJavaVersion(self.project), '17')


class FindSdkmanJavaHomeTests(unittest.TestCase):
    def setUp(self):
        self.candidates_dir = tempfile.mkdtemp()
        self._patcher = mock.patch.object(target, 'SDKMAN_JAVA_CANDIDATES_DIR', self.candidates_dir)
        self._patcher.start()

    def tearDown(self):
        self._patcher.stop()
        shutil.rmtree(self.candidates_dir, ignore_errors=True)

    def _add_candidate(self, name):
        os.makedirs(os.path.join(self.candidates_dir, name), exist_ok=True)

    def test_returns_none_when_sdkman_is_not_installed(self):
        with mock.patch.object(target, 'SDKMAN_JAVA_CANDIDATES_DIR', '/nonexistent-sdkman-dir'):
            self.assertIsNone(target.findSdkmanJavaHome('17'))

    def test_returns_none_when_no_candidate_matches(self):
        self._add_candidate('11.0.21-amzn')
        self.assertIsNone(target.findSdkmanJavaHome('17'))

    def test_finds_a_matching_candidate(self):
        self._add_candidate('17.0.9-tem')
        self._add_candidate('11.0.21-amzn')

        result = target.findSdkmanJavaHome('17')

        self.assertEqual(result, os.path.join(self.candidates_dir, '17.0.9-tem'))

    def test_ignores_the_current_symlink_entry(self):
        self._add_candidate('current')
        self.assertIsNone(target.findSdkmanJavaHome('current'))


class MvnEnvironmentForProjectTests(unittest.TestCase):
    def test_leaves_environment_unchanged_when_no_version_is_declared(self):
        with mock.patch.object(target, 'getRequiredJavaVersion', return_value=None), \
                mock.patch.object(target, 'findSdkmanJavaHome') as mocked_find:
            env = target._mvnEnvironmentForProject('/some/project')

        mocked_find.assert_not_called()
        self.assertEqual(env.get('JAVA_HOME'), os.environ.get('JAVA_HOME'))

    def test_leaves_environment_unchanged_when_no_candidate_matches(self):
        with mock.patch.object(target, 'getRequiredJavaVersion', return_value='17'), \
                mock.patch.object(target, 'findSdkmanJavaHome', return_value=None), \
                mock.patch.object(target, 'log') as mocked_log:
            env = target._mvnEnvironmentForProject('/some/project')

        self.assertEqual(env.get('JAVA_HOME'), os.environ.get('JAVA_HOME'))
        mocked_log.warning.assert_called_once()

    def test_sets_java_home_and_prepends_path_when_a_candidate_matches(self):
        candidate = '/home/user/.sdkman/candidates/java/17.0.9-tem'
        with mock.patch.object(target, 'getRequiredJavaVersion', return_value='17'), \
                mock.patch.object(target, 'findSdkmanJavaHome', return_value=candidate):
            env = target._mvnEnvironmentForProject('/some/project')

        self.assertEqual(env['JAVA_HOME'], candidate)
        self.assertTrue(env['PATH'].startswith(os.path.join(candidate, 'bin') + os.pathsep))


class RunMavenBuildTests(unittest.TestCase):
    def test_runs_both_mvn_phases_with_the_resolved_environment(self):
        fake_env = {'JAVA_HOME': '/fake/jdk'}
        with mock.patch.object(target, '_mvnEnvironmentForProject', return_value=fake_env), \
                mock.patch.object(target.subprocess, 'check_call') as mocked_check_call:
            target._runMavenBuild('/some/project')

        mocked_check_call.assert_any_call(['mvn', 'clean', 'install'], cwd='/some/project', env=fake_env)
        mocked_check_call.assert_any_call(['mvn', '-Pdist'], cwd='/some/project', env=fake_env)


class BrewMysqlServiceNameTests(unittest.TestCase):
    def test_returns_none_when_brew_is_not_installed(self):
        with mock.patch.object(target, 'which', return_value=None):
            self.assertIsNone(target._brewMysqlServiceName())

    def test_returns_none_when_no_mysql_like_service_is_listed(self):
        listing = mock.Mock(stdout='Name    Status  User Plist\npostgresql  none\n')
        with mock.patch.object(target, 'which', return_value='/opt/homebrew/bin/brew'), \
                mock.patch.object(target.subprocess, 'run', return_value=listing):
            self.assertIsNone(target._brewMysqlServiceName())

    def test_finds_a_mysql_named_service_case_insensitively(self):
        listing = mock.Mock(stdout='Name    Status  User Plist\nMySQL   none\n')
        with mock.patch.object(target, 'which', return_value='/opt/homebrew/bin/brew'), \
                mock.patch.object(target.subprocess, 'run', return_value=listing):
            self.assertEqual(target._brewMysqlServiceName(), 'MySQL')

    def test_finds_a_versioned_mysql_service(self):
        listing = mock.Mock(stdout='Name       Status  User Plist\nmysql@8.0  none\n')
        with mock.patch.object(target, 'which', return_value='/opt/homebrew/bin/brew'), \
                mock.patch.object(target.subprocess, 'run', return_value=listing):
            self.assertEqual(target._brewMysqlServiceName(), 'mysql@8.0')

    def test_returns_none_when_brew_services_list_fails(self):
        with mock.patch.object(target, 'which', return_value='/opt/homebrew/bin/brew'), \
                mock.patch.object(target.subprocess, 'run',
                                   side_effect=OSError('brew not runnable')):
            self.assertIsNone(target._brewMysqlServiceName())


class WaitForMysqlTests(unittest.TestCase):
    def test_returns_true_immediately_when_already_running(self):
        with mock.patch.object(target, 'assertMysqlRunning', return_value=True), \
                mock.patch.object(target.time, 'sleep') as mocked_sleep:
            self.assertTrue(target._waitForMysql(attempts=5, delay=1))

        mocked_sleep.assert_not_called()

    def test_polls_until_mysql_comes_up(self):
        with mock.patch.object(target, 'assertMysqlRunning', side_effect=[False, False, True]), \
                mock.patch.object(target.time, 'sleep') as mocked_sleep:
            self.assertTrue(target._waitForMysql(attempts=5, delay=1))

        self.assertEqual(mocked_sleep.call_count, 2)

    def test_gives_up_after_exhausting_attempts(self):
        with mock.patch.object(target, 'assertMysqlRunning', return_value=False), \
                mock.patch.object(target.time, 'sleep'):
            self.assertFalse(target._waitForMysql(attempts=3, delay=1))


class StartLocalMysqlTests(unittest.TestCase):
    def test_starts_via_brew_services_when_a_brew_service_is_found(self):
        with mock.patch.object(target, '_brewMysqlServiceName', return_value='mysql'), \
                mock.patch.object(target, 'which', return_value=None), \
                mock.patch.object(target.subprocess, 'run') as mocked_run, \
                mock.patch.object(target, '_waitForMysql', return_value=True) as mocked_wait:
            result = target.startLocalMysql()

        mocked_run.assert_called_once()
        self.assertEqual(mocked_run.call_args[0][0], ['brew', 'services', 'start', 'mysql'])
        mocked_wait.assert_called_once()
        self.assertTrue(result)

    def test_falls_back_to_mysql_server_when_no_brew_service_is_found(self):
        with mock.patch.object(target, '_brewMysqlServiceName', return_value=None), \
                mock.patch.object(target, 'which', return_value='/usr/local/bin/mysql.server'), \
                mock.patch.object(target.subprocess, 'run') as mocked_run, \
                mock.patch.object(target, '_waitForMysql', return_value=True):
            result = target.startLocalMysql()

        mocked_run.assert_called_once()
        self.assertEqual(mocked_run.call_args[0][0], ['mysql.server', 'start'])
        self.assertTrue(result)

    def test_returns_false_when_no_known_start_method_is_available(self):
        with mock.patch.object(target, '_brewMysqlServiceName', return_value=None), \
                mock.patch.object(target, 'which', return_value=None), \
                mock.patch.object(target.subprocess, 'run') as mocked_run, \
                mock.patch.object(target, '_waitForMysql') as mocked_wait:
            result = target.startLocalMysql()

        mocked_run.assert_not_called()
        mocked_wait.assert_not_called()
        self.assertFalse(result)

    def test_returns_false_when_the_start_command_itself_fails(self):
        with mock.patch.object(target, '_brewMysqlServiceName', return_value='mysql'), \
                mock.patch.object(target.subprocess, 'run', side_effect=OSError('boom')), \
                mock.patch.object(target, '_waitForMysql') as mocked_wait:
            result = target.startLocalMysql()

        mocked_wait.assert_not_called()
        self.assertFalse(result)

    def test_returns_false_when_start_command_succeeds_but_mysql_never_comes_up(self):
        with mock.patch.object(target, '_brewMysqlServiceName', return_value='mysql'), \
                mock.patch.object(target.subprocess, 'run'), \
                mock.patch.object(target, '_waitForMysql', return_value=False):
            result = target.startLocalMysql()

        self.assertFalse(result)


class EnsureMysqlRunningTests(unittest.TestCase):
    def test_returns_true_without_attempting_to_start_when_already_running(self):
        with mock.patch.object(target, 'assertMysqlRunning', return_value=True), \
                mock.patch.object(target, 'startLocalMysql') as mocked_start:
            self.assertTrue(target.ensureMysqlRunning())

        mocked_start.assert_not_called()

    def test_attempts_to_start_and_returns_its_result_when_not_running(self):
        with mock.patch.object(target, 'assertMysqlRunning', return_value=False), \
                mock.patch.object(target, 'startLocalMysql', return_value=True) as mocked_start:
            self.assertTrue(target.ensureMysqlRunning())

        mocked_start.assert_called_once()

    def test_returns_false_when_start_attempt_fails(self):
        with mock.patch.object(target, 'assertMysqlRunning', return_value=False), \
                mock.patch.object(target, 'startLocalMysql', return_value=False):
            self.assertFalse(target.ensureMysqlRunning())


class EnsureDatabaseExistsTests(unittest.TestCase):
    def test_runs_create_database_if_not_exists(self):
        env = {'MYSQL_PWD': 'secret'}
        completed = mock.Mock(returncode=0, stderr='')
        with mock.patch.object(target.subprocess, 'run', return_value=completed) as mocked_run:
            target._ensureDatabaseExists('root', 'cavco', env)

        args, kwargs = mocked_run.call_args
        self.assertEqual(args[0], ['mysql', '-u', 'root', '-h', 'localhost', '-e',
                                    'CREATE DATABASE IF NOT EXISTS `cavco`'])
        self.assertEqual(kwargs['env'], env)

    def test_escapes_backticks_in_database_name(self):
        completed = mock.Mock(returncode=0, stderr='')
        with mock.patch.object(target.subprocess, 'run', return_value=completed) as mocked_run:
            target._ensureDatabaseExists('root', 'weird`name', {})

        args, _ = mocked_run.call_args
        self.assertEqual(args[0][-1], 'CREATE DATABASE IF NOT EXISTS `weird``name`')

    def test_raises_when_creation_fails(self):
        completed = mock.Mock(returncode=1, stderr='Access denied\n')
        with mock.patch.object(target.subprocess, 'run', return_value=completed):
            with self.assertRaises(RuntimeError) as ctx:
                target._ensureDatabaseExists('root', 'cavco', {})

        self.assertIn('cavco', str(ctx.exception))
        self.assertIn('Access denied', str(ctx.exception))


class LoadBackupLocalMySQLTests(unittest.TestCase):
    def setUp(self):
        self.backup_path = tempfile.mktemp()
        with open(self.backup_path, 'wb') as f:
            f.write(b'-- dump contents')
        self.addCleanup(os.remove, self.backup_path)

    def _run(self, mysql_returncode=0, gunzip_returncode=0):
        completed = mock.Mock(returncode=mysql_returncode)
        gunzip_proc = mock.Mock(returncode=gunzip_returncode)
        gunzip_proc.stdout = mock.Mock()
        with mock.patch('builtins.input', side_effect=['root', 'cavco']), \
                mock.patch.object(target.getpass, 'getpass', return_value='pw'), \
                mock.patch.object(target, '_ensureDatabaseExists') as mocked_ensure, \
                mock.patch.object(target.subprocess, 'Popen', return_value=gunzip_proc) as mocked_popen, \
                mock.patch.object(target.subprocess, 'run', return_value=completed) as mocked_run:
            result = {}
            try:
                result['return_value'] = target.loadBackupLocalMySQL(self.backup_path)
            except RuntimeError as e:
                result['error'] = e
            return mocked_ensure, mocked_popen, mocked_run, gunzip_proc, result

    def test_ensures_the_destination_database_exists_before_importing(self):
        mocked_ensure, _, mocked_run, _, result = self._run()

        mocked_ensure.assert_called_once()
        args, _ = mocked_ensure.call_args
        self.assertEqual(args[0], 'root')
        self.assertEqual(args[1], 'cavco')
        mocked_run.assert_called_once()
        self.assertNotIn('error', result)

    def test_raises_when_the_import_itself_fails(self):
        _, _, _, _, result = self._run(mysql_returncode=1)

        self.assertIn('error', result)
        self.assertIsInstance(result['error'], RuntimeError)

    def test_imports_in_binary_mode(self):
        # Dumps of Jackrabbit's DEFAULT_BUNDLE/DEFAULT_BINVAL tables contain
        # raw longblob data with embedded NUL bytes. Without --binary-mode,
        # mysql 8.0+/9.x clients reject those literals with "ASCII '\0'
        # appeared in the statement... unless --binary-mode is enabled".
        _, _, mocked_run, _, result = self._run()

        args, _ = mocked_run.call_args
        self.assertIn('--binary-mode', args[0])
        self.assertNotIn('error', result)

    def test_decompresses_the_gzipped_backup_before_piping_it_into_mysql(self):
        # backupPath is the .gz file downloaded from Mission Control —
        # piping it into mysql directly (as this used to do) feeds mysql
        # raw compressed bytes instead of SQL text, which mysql either
        # rejects outright or misparses as garbage statements.
        _, mocked_popen, mocked_run, gunzip_proc, result = self._run()

        popen_args, popen_kwargs = mocked_popen.call_args
        self.assertEqual(popen_args[0], ['gunzip', '-c', self.backup_path])
        self.assertEqual(popen_kwargs['stdout'], target.subprocess.PIPE)

        run_args, run_kwargs = mocked_run.call_args
        self.assertEqual(run_kwargs['stdin'], gunzip_proc.stdout)
        self.assertNotIn('error', result)

    def test_raises_when_gunzip_fails(self):
        _, _, _, _, result = self._run(gunzip_returncode=1)

        self.assertIn('error', result)
        self.assertIsInstance(result['error'], RuntimeError)
        self.assertIn('gunzip', str(result['error']))

    def test_returns_the_entered_credentials(self):
        # main() needs these to also wire the local project's config to the
        # database that was just populated.
        _, _, _, _, result = self._run()

        self.assertEqual(result['return_value'], ('root', 'pw', 'cavco'))


class ConfigureLocalProjectForMysqlTests(unittest.TestCase):
    """configureLocalProjectForMysql wires a local checkout's cargo.run to
    read from a locally-imported MySQL backup, if it isn't already."""

    CONTEXT_XML = '''<?xml version='1.0' encoding='utf-8'?>
<Context>
    <Manager pathname="" />
    <Resource name="jdbc/targetingDS" auth="Container" type="javax.sql.DataSource" username="sa" password="" driverClassName="org.h2.Driver" url="jdbc:h2:${repo.path}/targeting/targeting"/>
    <!-- Enable this to let wicket output a wicketpath attribute -->
</Context>
'''

    REPOSITORY_MYSQL_XML = '''<Repository>
  <param name="url" value="java:comp/env/jdbc/@mysql.repo.db@"/>
  <Cluster id="@cluster.node.id@">
  <param name="bundleCacheSize" value="@repo.workspace.bundle.cache@"/>
  <param name="bundleCacheSize" value="@repo.versioning.bundle.cache@"/>
</Repository>
'''

    CONTEXT_MYSQL_XML = '''<Context>
    <Resource name="jdbc/@mysql.repo.db@" username="@mysql.username@" password="@mysql.password@" driverClassName="@mysql.driver@"/>
</Context>
'''

    POM_XML = '''<project>
  <dependencies>
    <dependency>
      <groupId>com.onehippo.cms7</groupId>
      <artifactId>hippo-addon-targeting-shared-api</artifactId>
      <scope>provided</scope>
    </dependency>
    <dependency>
      <groupId>com.onehippo.cms7</groupId>
      <artifactId>hippo-enterprise-services</artifactId>
      <scope>provided</scope>
    </dependency>
  </dependencies>
    <profile>
      <id>cargo.run</id>
              <systemProperties>
                  <log4j.configurationFile>${project.basedir}/conf/log4j2-dev.xml</log4j.configurationFile>
                  <project.basedir>${project.basedir}</project.basedir>
                </systemProperties>
                <dependencies>
                  <dependency>
                    <groupId>com.onehippo.cms7</groupId>
                    <artifactId>hippo-addon-targeting-shared-api</artifactId>
                    <classpath>shared</classpath>
                  </dependency>
                  <dependency>
                    <groupId>com.onehippo.cms7</groupId>
                    <artifactId>hippo-enterprise-services</artifactId>
                    <classpath>shared</classpath>
                  </dependency>
                </dependencies>
                <files>
                  <file>
                    <file>${project.basedir}/repository-data/development/target/cavco-repository-data-development-${project.version}.jar</file>
                    <todir>${development-module-deploy-dir}</todir>
                  </file>
                  <file>
                    <file>${project.basedir}/repository-data/site-development/target/cavco-repository-data-site-development-${project.version}.jar</file>
                    <todir>${development-module-deploy-dir}</todir>
                  </file>
                </files>
    </profile>
</project>
'''

    CMS_POM_XML = '''<project>
  <dependencies>
    <dependency>
      <groupId>junit</groupId>
      <artifactId>junit</artifactId>
    </dependency>
  </dependencies>
</project>
'''

    DOCKER_DB_LIBS_XML = '''<component>
  <dependencySets>
    <dependencySet>
      <includes>
        <include>mysql:mysql-connector-java</include>
      </includes>
    </dependencySet>
  </dependencySets>
</component>
'''

    def setUp(self):
        self.project = tempfile.mkdtemp()
        self.addCleanup(shutil.rmtree, self.project, ignore_errors=True)
        os.makedirs(os.path.join(self.project, 'conf'))
        os.makedirs(os.path.join(self.project, 'cms'))
        os.makedirs(os.path.join(self.project, 'src', 'main', 'docker', 'assembly'))
        self._write('conf/context.xml', self.CONTEXT_XML)
        self._write('conf/repository-mysql.xml', self.REPOSITORY_MYSQL_XML)
        self._write('conf/context-mysql.xml', self.CONTEXT_MYSQL_XML)
        self._write('pom.xml', self.POM_XML)
        self._write('cms/pom.xml', self.CMS_POM_XML)
        self._write('src/main/docker/assembly/docker-db-libs.xml', self.DOCKER_DB_LIBS_XML)

    def _write(self, relPath, content):
        fullPath = os.path.join(self.project, relPath)
        with open(fullPath, 'w') as f:
            f.write(content)
        return fullPath

    def _read(self, relPath):
        with open(os.path.join(self.project, relPath)) as f:
            return f.read()

    def test_writes_repository_xml_with_placeholders_resolved(self):
        target.configureLocalProjectForMysql(self.project, 'root', 'pw', 'cavco')

        content = self._read('conf/repository.xml')
        self.assertIn('jdbc/cavco', content)
        self.assertIn('id="local"', content)
        self.assertIn('value="256"', content)
        self.assertIn('value="64"', content)
        self.assertNotIn('@', content)

    def test_adds_mysql_resource_to_context_xml_without_disturbing_existing_content(self):
        target.configureLocalProjectForMysql(self.project, 'root', 'pw', 'cavco')

        content = self._read('conf/context.xml')
        self.assertIn('jdbc/cavco', content)
        self.assertIn('jdbc/targetingDS', content)
        self.assertIn('wicketpath', content)

    def test_wires_repo_config_into_pom_without_disturbing_existing_content(self):
        target.configureLocalProjectForMysql(self.project, 'root', 'pw', 'cavco')

        content = self._read('pom.xml')
        # LocalHippoRepository.getRepositoryConfigAsStream only opens this
        # as a filesystem File if the value starts with the literal "file:"
        # prefix — otherwise it's treated as a classpath resource lookup,
        # which silently resolves to null and NPEs on openStream().
        self.assertIn('<repo.config>file:${project.basedir}/conf/repository.xml</repo.config>', content)
        self.assertIn('log4j2-dev.xml', content)

    def test_adds_mysql_connector_dependency_to_cms_pom_without_disturbing_existing_content(self):
        target.configureLocalProjectForMysql(self.project, 'root', 'pw', 'cavco')

        content = self._read('cms/pom.xml')
        self.assertIn('mysql-connector-j</artifactId>', content)
        self.assertIn('junit', content)

    def test_fixes_stale_connector_coordinates_in_docker_assembly(self):
        target.configureLocalProjectForMysql(self.project, 'root', 'pw', 'cavco')

        content = self._read('src/main/docker/assembly/docker-db-libs.xml')
        self.assertIn('com.mysql:mysql-connector-j', content)
        self.assertNotIn('mysql:mysql-connector-java', content)

    def test_wires_mysql_connector_into_cargo_container_classpath_without_disturbing_existing_content(self):
        # provided scope in cms/pom.xml alone isn't enough for cargo.run:
        # cargo's embedded Tomcat needs the driver on its OWN classpath so
        # DBCP can load it when registering the JNDI Resource — hence the
        # classpath=shared entry, mirroring the existing shared deps.
        target.configureLocalProjectForMysql(self.project, 'root', 'pw', 'cavco')

        content = self._read('pom.xml')
        self.assertIn('hippo-addon-targeting-shared-api', content)
        self.assertIn('hippo-enterprise-services', content)
        self.assertIn(
            '<artifactId>mysql-connector-j</artifactId>\n                    <classpath>shared</classpath>',
            content)

    def test_also_declares_mysql_connector_as_a_root_project_dependency(self):
        # cargo rejects a classpath=shared entry unless the artifact is
        # ALSO a real dependency of the project running cargo:start (the
        # root module here) — exactly how hippo-addon-targeting-shared-api /
        # hippo-enterprise-services are declared in both places already.
        # Confirmed by the actual cargo error: "Artifact
        # [com.mysql:mysql-connector-j:jar] is not a dependency of the
        # project."
        target.configureLocalProjectForMysql(self.project, 'root', 'pw', 'cavco')

        content = self._read('pom.xml')
        self.assertIn(
            '<artifactId>mysql-connector-j</artifactId>\n      <scope>provided</scope>',
            content)
        self.assertEqual(content.count('<artifactId>mysql-connector-j</artifactId>'), 2)

    def test_copies_the_connector_jar_into_common_lib_without_disturbing_existing_files(self):
        # cargo-maven2-plugin's <classpath>shared</classpath> dependency
        # mechanism only understands Tomcat 6/7's shared/lib classloader
        # tier, which modern Tomcat 8.5+/9/10 no longer builds (confirmed:
        # the jar landed in shared/lib but ClassNotFoundException
        # persisted). common.loader still reads common/lib, so copy the
        # resolved jar straight there instead.
        target.configureLocalProjectForMysql(self.project, 'root', 'pw', 'cavco')

        content = self._read('pom.xml')
        self.assertIn(
            '<file>${settings.localRepository}/com/mysql/mysql-connector-j/'
            '${mysql.version}/mysql-connector-j-${mysql.version}.jar</file>',
            content)
        self.assertIn('<todir>common/lib</todir>', content)
        self.assertIn('cavco-repository-data-development', content)
        self.assertIn('cavco-repository-data-site-development', content)

    def test_does_not_overwrite_a_preexisting_repository_xml_but_still_wires_the_rest(self):
        # conf/repository.xml already existing (e.g. from a previous run, or
        # hand-written) means "don't touch this file's content" — it does
        # NOT mean "this project is fully configured": each other wiring
        # step is independently idempotent and still fills in what's missing.
        self._write('conf/repository.xml', '<Repository><!-- custom, hand-written --></Repository>')

        target.configureLocalProjectForMysql(self.project, 'root', 'pw', 'cavco')

        self.assertIn('custom, hand-written', self._read('conf/repository.xml'))
        self.assertIn('jdbc/cavco', self._read('conf/context.xml'))
        self.assertIn('<repo.config>', self._read('pom.xml'))
        self.assertIn('<artifactId>mysql-connector-j</artifactId>', self._read('pom.xml'))
        self.assertIn('mysql-connector-j</artifactId>', self._read('cms/pom.xml'))

    def test_noop_when_templates_are_missing(self):
        os.remove(os.path.join(self.project, 'conf', 'repository-mysql.xml'))

        target.configureLocalProjectForMysql(self.project, 'root', 'pw', 'cavco')

        self.assertFalse(os.path.exists(os.path.join(self.project, 'conf', 'repository.xml')))

    def test_rerun_is_idempotent(self):
        target.configureLocalProjectForMysql(self.project, 'root', 'pw', 'cavco')
        first_context = self._read('conf/context.xml')
        first_pom = self._read('pom.xml')
        first_cms_pom = self._read('cms/pom.xml')

        # Second call re-runs every step, but each one's own existence
        # check finds its change already present and no-ops.
        target.configureLocalProjectForMysql(self.project, 'root', 'pw', 'cavco')

        self.assertEqual(self._read('conf/context.xml'), first_context)
        self.assertEqual(self._read('pom.xml'), first_pom)
        self.assertEqual(self._read('cms/pom.xml'), first_cms_pom)

    def test_only_adds_whats_missing_when_partially_configured(self):
        # repo.config already wired (e.g. by a previous manual edit), but
        # nothing else — conf/repository.xml still doesn't exist, so this
        # isn't the "already fully configured" short-circuit; each step's
        # own idempotency check should still skip just that one step.
        pom_with_repo_config = self.POM_XML.replace(
            '<log4j.configurationFile>${project.basedir}/conf/log4j2-dev.xml</log4j.configurationFile>',
            '<log4j.configurationFile>${project.basedir}/conf/log4j2-dev.xml</log4j.configurationFile>\n'
            '                  <repo.config>${project.basedir}/conf/repository.xml</repo.config>')
        self._write('pom.xml', pom_with_repo_config)

        target.configureLocalProjectForMysql(self.project, 'root', 'pw', 'cavco')

        content = self._read('pom.xml')
        self.assertEqual(content.count('<repo.config>'), 1)
        self.assertIn('<artifactId>mysql-connector-j</artifactId>', content)
        self.assertIn('mysql-connector-j</artifactId>', self._read('cms/pom.xml'))


class ResolveMissionControlPasswordTests(unittest.TestCase):
    def test_uses_env_var_when_set_without_prompting(self):
        with mock.patch.dict(os.environ, {target.MISSION_CONTROL_PASSWORD_ENV_VAR: 'secret'}), \
                mock.patch.object(target.getpass, 'getpass') as mocked_getpass:
            result = target._resolveMissionControlPassword('joey')

        self.assertEqual(result, 'secret')
        mocked_getpass.assert_not_called()

    def test_falls_back_to_prompt_when_env_var_is_unset(self):
        env_without_var = {k: v for k, v in os.environ.items() if k != target.MISSION_CONTROL_PASSWORD_ENV_VAR}
        with mock.patch.dict(os.environ, env_without_var, clear=True), \
                mock.patch.object(target.getpass, 'getpass', return_value='typed-pw') as mocked_getpass:
            result = target._resolveMissionControlPassword('joey')

        self.assertEqual(result, 'typed-pw')
        mocked_getpass.assert_called_once()

    def test_falls_back_to_prompt_when_env_var_is_empty(self):
        with mock.patch.dict(os.environ, {target.MISSION_CONTROL_PASSWORD_ENV_VAR: ''}), \
                mock.patch.object(target.getpass, 'getpass', return_value='typed-pw') as mocked_getpass:
            result = target._resolveMissionControlPassword('joey')

        self.assertEqual(result, 'typed-pw')
        mocked_getpass.assert_called_once()


class MainCleanupTests(unittest.TestCase):
    """Covers what main() does with the downloaded backup/distribution on failure.

    Only the parity-check-fails path should discard them (they don't
    correspond to this local checkout); every other failure (build errors,
    mysql import errors, network errors, ...) must leave them in place so a
    retry can reuse them via --backup/--dist instead of re-downloading.
    """

    def setUp(self):
        self.project_dir = tempfile.mkdtemp()
        self.backup_path = os.path.join(self.project_dir, 'backup.gz')
        self.dist_path = os.path.join(self.project_dir, 'dist.tar.gz')
        for path in (self.backup_path, self.dist_path):
            with open(path, 'wb') as f:
                f.write(b'fake-bytes')

        fake_args = argparse.Namespace(
            username='user', clientAccount='acme', remoteEnv='dev',
            local_project_path=self.project_dir, dry_run=False,
            backup=None, dist=None, skip_dist_check=False)

        self._patchers = [
            mock.patch.object(target.argparser, 'parse_args', return_value=fake_args),
            mock.patch.object(target, '_resolveMissionControlPassword', return_value='pw'),
            mock.patch.object(target, 'authenticateCloudAPI', return_value=mock.Mock(value='tok')),
            mock.patch.object(target, 'listEnvironments',
                               return_value=[{'name': 'dev', 'id': 'env-1', 'distributionId': 'dist-1'}]),
            mock.patch.object(target, 'verifyBareSystemMinimum', return_value=True),
            mock.patch.object(target, 'listBackups', return_value=[]),
            mock.patch.object(target, 'getMostRecentBackupId', return_value='backup-1'),
            mock.patch.object(target, 'getBackupDownloadLink', return_value='https://backup-link'),
            mock.patch.object(target, 'downloadBackup', return_value=self.backup_path),
            mock.patch.object(target, 'getDistributionDownloadToken', return_value='dist-token'),
            mock.patch.object(target, 'downloadDistribution', return_value=self.dist_path),
            mock.patch.object(target, 'extractDistribution',
                               return_value=os.path.join(self.project_dir, 'latestDist')),
        ]
        for patcher in self._patchers:
            patcher.start()
            self.addCleanup(patcher.stop)

    def tearDown(self):
        shutil.rmtree(self.project_dir, ignore_errors=True)

    def test_parity_failure_deletes_the_downloaded_backup_and_distribution(self):
        with mock.patch.object(target, 'buildDistributionAndCompare', return_value=False):
            with self.assertRaises(SystemExit):
                target.main()

        self.assertFalse(os.path.exists(self.backup_path))
        self.assertFalse(os.path.exists(self.dist_path))

    def test_other_failures_do_not_delete_the_downloaded_backup_or_distribution(self):
        with mock.patch.object(target, 'buildDistributionAndCompare', side_effect=RuntimeError('mvn broke')):
            with self.assertRaises(SystemExit):
                target.main()

        self.assertTrue(os.path.exists(self.backup_path))
        self.assertTrue(os.path.exists(self.dist_path))


class MainSkipsAuthenticationTests(unittest.TestCase):
    """When both --backup and --dist point at local files, nothing from
    Mission Control (a token, the environment/distribution ids) is ever
    read, so there is no reason to prompt for a password or hit the API —
    except for --dry-run, whose entire purpose is validating that auth and
    environment resolution succeed."""

    def setUp(self):
        self.project_dir = tempfile.mkdtemp()
        self.backup_path = os.path.join(self.project_dir, 'backup.gz')
        self.dist_path = os.path.join(self.project_dir, 'dist.tar.gz')
        for path in (self.backup_path, self.dist_path):
            with open(path, 'wb') as f:
                f.write(b'fake-bytes')
        self.addCleanup(shutil.rmtree, self.project_dir, ignore_errors=True)

    def _run_main(self, backup, dist, dry_run=False):
        fake_args = argparse.Namespace(
            username='user', clientAccount='acme', remoteEnv='dev',
            local_project_path=self.project_dir, dry_run=dry_run,
            backup=backup, dist=dist, skip_dist_check=False)

        patchers = {
            'parse_args': mock.patch.object(target.argparser, 'parse_args', return_value=fake_args),
            'resolvePassword': mock.patch.object(target, '_resolveMissionControlPassword', return_value='pw'),
            'authenticateCloudAPI': mock.patch.object(
                target, 'authenticateCloudAPI', return_value=mock.Mock(value='tok')),
            'listEnvironments': mock.patch.object(
                target, 'listEnvironments',
                return_value=[{'name': 'dev', 'id': 'env-1', 'distributionId': 'dist-1'}]),
            'verifyBareSystemMinimum': mock.patch.object(target, 'verifyBareSystemMinimum', return_value=True),
            'listBackups': mock.patch.object(target, 'listBackups', return_value=[]),
            'getMostRecentBackupId': mock.patch.object(target, 'getMostRecentBackupId', return_value='backup-1'),
            'getBackupDownloadLink': mock.patch.object(
                target, 'getBackupDownloadLink', return_value='https://backup-link'),
            'downloadBackup': mock.patch.object(target, 'downloadBackup', return_value=self.backup_path),
            'getDistributionDownloadToken': mock.patch.object(
                target, 'getDistributionDownloadToken', return_value='dist-token'),
            'downloadDistribution': mock.patch.object(target, 'downloadDistribution', return_value=self.dist_path),
            'extractDistribution': mock.patch.object(
                target, 'extractDistribution', return_value=os.path.join(self.project_dir, 'latestDist')),
            'buildDistributionAndCompare': mock.patch.object(
                target, 'buildDistributionAndCompare', return_value=True),
            'assertMysqlRunning': mock.patch.object(target, 'assertMysqlRunning', return_value=True),
            'loadBackupLocalMySQL': mock.patch.object(
                target, 'loadBackupLocalMySQL', return_value=('root', 'pw', 'cavco')),
            'configureLocalProjectForMysql': mock.patch.object(target, 'configureLocalProjectForMysql'),
        }
        mocks = {name: patcher.start() for name, patcher in patchers.items()}
        for patcher in patchers.values():
            self.addCleanup(patcher.stop)
        return mocks

    def test_skips_authentication_when_both_backup_and_dist_are_provided(self):
        mocks = self._run_main(backup=self.backup_path, dist=self.dist_path)

        target.main()

        mocks['authenticateCloudAPI'].assert_not_called()
        mocks['listEnvironments'].assert_not_called()
        mocks['resolvePassword'].assert_not_called()

    def test_configures_local_project_for_mysql_with_the_entered_credentials(self):
        mocks = self._run_main(backup=self.backup_path, dist=self.dist_path)

        target.main()

        mocks['configureLocalProjectForMysql'].assert_called_once_with(
            self.project_dir, 'root', 'pw', 'cavco')

    def test_authenticates_when_backup_is_missing(self):
        mocks = self._run_main(backup=None, dist=self.dist_path)

        target.main()

        mocks['authenticateCloudAPI'].assert_called_once()

    def test_authenticates_when_dist_is_missing(self):
        mocks = self._run_main(backup=self.backup_path, dist=None)

        target.main()

        mocks['authenticateCloudAPI'].assert_called_once()

    def test_still_authenticates_in_dry_run_even_with_both_provided(self):
        mocks = self._run_main(backup=self.backup_path, dist=self.dist_path, dry_run=True)

        target.main()

        mocks['authenticateCloudAPI'].assert_called_once()
        mocks['buildDistributionAndCompare'].assert_not_called()


class MainSkipDistCheckTests(unittest.TestCase):
    """--skip-dist-check loads the backup into the local checkout as-is,
    without ever downloading/building/comparing the distribution — the
    point being to see how a *local checkout* behaves against a remote DB
    backup, independent of whatever is actually deployed remotely."""

    def setUp(self):
        self.project_dir = tempfile.mkdtemp()
        self.backup_path = os.path.join(self.project_dir, 'backup.gz')
        with open(self.backup_path, 'wb') as f:
            f.write(b'fake-bytes')
        self.addCleanup(shutil.rmtree, self.project_dir, ignore_errors=True)

    def _run_main(self, backup, dry_run=False):
        fake_args = argparse.Namespace(
            username='user', clientAccount='acme', remoteEnv='dev',
            local_project_path=self.project_dir, dry_run=dry_run,
            backup=backup, dist=None, skip_dist_check=True)

        patchers = {
            'parse_args': mock.patch.object(target.argparser, 'parse_args', return_value=fake_args),
            'resolvePassword': mock.patch.object(target, '_resolveMissionControlPassword', return_value='pw'),
            'authenticateCloudAPI': mock.patch.object(
                target, 'authenticateCloudAPI', return_value=mock.Mock(value='tok')),
            'listEnvironments': mock.patch.object(
                target, 'listEnvironments',
                return_value=[{'name': 'dev', 'id': 'env-1', 'distributionId': 'dist-1'}]),
            'verifyBareSystemMinimum': mock.patch.object(target, 'verifyBareSystemMinimum', return_value=True),
            'listBackups': mock.patch.object(target, 'listBackups', return_value=[]),
            'getMostRecentBackupId': mock.patch.object(target, 'getMostRecentBackupId', return_value='backup-1'),
            'getBackupDownloadLink': mock.patch.object(
                target, 'getBackupDownloadLink', return_value='https://backup-link'),
            'downloadBackup': mock.patch.object(target, 'downloadBackup', return_value=self.backup_path),
            'getDistributionDownloadToken': mock.patch.object(target, 'getDistributionDownloadToken'),
            'downloadDistribution': mock.patch.object(target, 'downloadDistribution'),
            'extractDistribution': mock.patch.object(target, 'extractDistribution'),
            'buildDistributionAndCompare': mock.patch.object(target, 'buildDistributionAndCompare'),
            'assertMysqlRunning': mock.patch.object(target, 'assertMysqlRunning', return_value=True),
            'loadBackupLocalMySQL': mock.patch.object(
                target, 'loadBackupLocalMySQL', return_value=('root', 'pw', 'cavco')),
            'configureLocalProjectForMysql': mock.patch.object(target, 'configureLocalProjectForMysql'),
        }
        mocks = {name: patcher.start() for name, patcher in patchers.items()}
        for patcher in patchers.values():
            self.addCleanup(patcher.stop)
        return mocks

    def test_loads_backup_without_touching_the_distribution_at_all(self):
        mocks = self._run_main(backup=self.backup_path)

        target.main()

        mocks['getDistributionDownloadToken'].assert_not_called()
        mocks['downloadDistribution'].assert_not_called()
        mocks['extractDistribution'].assert_not_called()
        mocks['buildDistributionAndCompare'].assert_not_called()
        mocks['loadBackupLocalMySQL'].assert_called_once_with(self.backup_path)
        mocks['configureLocalProjectForMysql'].assert_called_once_with(
            self.project_dir, 'root', 'pw', 'cavco')

    def test_skips_authentication_when_backup_is_already_present(self):
        # A distribution is never needed with --skip-dist-check, so unlike
        # the normal flow, --dist doesn't have to be provided to avoid
        # hitting Mission Control — only --backup does.
        mocks = self._run_main(backup=self.backup_path)

        target.main()

        mocks['authenticateCloudAPI'].assert_not_called()
        mocks['resolvePassword'].assert_not_called()

    def test_authenticates_when_backup_is_missing(self):
        mocks = self._run_main(backup=None)

        target.main()

        mocks['authenticateCloudAPI'].assert_called_once()

    def test_still_authenticates_in_dry_run_even_with_backup_provided(self):
        mocks = self._run_main(backup=self.backup_path, dry_run=True)

        target.main()

        mocks['authenticateCloudAPI'].assert_called_once()
        mocks['loadBackupLocalMySQL'].assert_not_called()

    def test_starts_mysql_automatically_when_not_already_running(self):
        mocks = self._run_main(backup=self.backup_path)
        mocks['assertMysqlRunning'].return_value = False
        start_patcher = mock.patch.object(target, 'startLocalMysql', return_value=True)
        mocked_start = start_patcher.start()
        self.addCleanup(start_patcher.stop)

        target.main()

        mocked_start.assert_called_once()
        mocks['loadBackupLocalMySQL'].assert_called_once_with(self.backup_path)

    def test_exits_when_mysql_is_down_and_cannot_be_started_automatically(self):
        mocks = self._run_main(backup=self.backup_path)
        mocks['assertMysqlRunning'].return_value = False
        start_patcher = mock.patch.object(target, 'startLocalMysql', return_value=False)
        start_patcher.start()
        self.addCleanup(start_patcher.stop)

        with self.assertRaises(SystemExit):
            target.main()

        mocks['loadBackupLocalMySQL'].assert_not_called()


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
