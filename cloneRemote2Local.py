#!/Users/josephliechty/Desktop/XM/cloneRemote2Local/.venv/bin/python3
import json
import argparse
import requests
import os
import glob
import time
import logging
from dateutil import parser
from tqdm import tqdm
from shutil import which
import subprocess
import sys
import tarfile
import filecmp
import getpass
import shutil

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s %(levelname)s %(message)s',
    handlers=[
        logging.StreamHandler(sys.stdout),
        logging.FileHandler('clone_remote.log'),
    ]
)
log = logging.getLogger(__name__)

'''
 This script is meant as a jumpstart to loading a remote OD2 instance into your localhost.
 It needs a few prerequisites:
 1. python 3
 2. requests, python-dateutil, tqdm installed via pip

 Required arguments:
 1. --remoteEnv    : remote environment name
 2. --clientAccount: client account name
 3. --username     : admin mission control user

 Optional arguments:
 --local-project-path   : path to local XM project (default: cwd)
 --dry-run              : resolve environment/backups without downloading anything
 --existing-backup      : path to a previously downloaded backup to skip re-download
 --existing-distribution: path to a previously downloaded distribution to skip re-download

 Steps performed:
 1.  Verify bare minimum system prerequisites: java, mysql, maven
 2.  Prompt for mission control password
 3.  Authenticate the BRcloud API and receive token
 4.  Identify and store the remote environment details
 5.  Locate the latest DB dump of the specified environment
 6.  Download the latest DB dump of the specified environment
 7.  Locate the latest distribution file of the specified environment
 8.  Download the latest distribution file of the specified environment
 9.  Compare local environment to the remote distribution to ensure parity
 10. Load the remote DB dump into a local MySQL DB
 11. On successful parity check, prompt user to start cargo to 'clone' remote env.
'''

argparser = argparse.ArgumentParser(description='authenticate, download latest DB, run in local')
argparser.add_argument('--remoteEnv', action='store', required=True,
                       help='name of remote environment you wish to clone locally')
argparser.add_argument('--clientAccount', action='store', required=True,
                       help='name of client account')
argparser.add_argument('--username', action='store', required=True,
                       help='username to authenticate mission control cloud api')
argparser.add_argument('--local-project-path', action='store', default=os.getcwd(),
                       help='path to local XM project root (default: current directory)')
argparser.add_argument('--dry-run', action='store_true', default=False,
                       help='resolve environment and backups without downloading anything')
argparser.add_argument('--existing-backup', action='store', default=None,
                       help='path to an existing backup file — skips backup download')
argparser.add_argument('--existing-distribution', action='store', default=None,
                       help='path to an existing distribution file — skips distribution download')
argparser.set_defaults(feature=True)

# Populated by main() — declared here so functions below resolve them as
# module globals without each needing them threaded through as a parameter.
USER = PASS = CLIENT = ENV = LOCAL_PROJECT_PATH = API = None

LOGIN = '/v3/authn/access_token'
REFRESH = '/v3/authn/refresh_token'
ENVS = '/v3/environments'
BACKUPS = '/v3/backups'

REQUEST_TIMEOUT = 30       # seconds per HTTP request
DOWNLOAD_MAX_RETRIES = 3   # attempts per file download before giving up
DOWNLOAD_RETRY_BACKOFF = 5  # seconds between download retries


def verifyBareSystemMinimum():
    missing = [tool for tool in ('mysql', 'java', 'mvn') if which(tool) is None]
    if missing:
        log.error('Missing required tools: %s', ', '.join(missing))
        return False
    return True


class AccessToken:
    """Wraps a BRcloud bearer token, transparently refreshing it before it expires.

    Per the API docs, access tokens are valid for only 10 minutes while
    refresh tokens last 24 hours. A run of this script routinely spans
    longer than 10 minutes (backup/distribution downloads, a Maven build),
    so any call site that held onto the raw token string risked a 401
    partway through. Call sites read the current token via `.value`
    instead of caching the string themselves.
    """
    LIFETIME_SECONDS = 10 * 60
    REFRESH_MARGIN_SECONDS = 30

    def __init__(self, access_token, refresh_token, now=time.monotonic):
        self._access_token = access_token
        self._refresh_token = refresh_token
        self._now = now
        self._expires_at = self._now() + self.LIFETIME_SECONDS

    @property
    def value(self):
        if self._now() >= self._expires_at - self.REFRESH_MARGIN_SECONDS:
            log.info('Access token nearing expiry, refreshing...')
            self._access_token = refreshAccessToken(self._refresh_token)
            self._expires_at = self._now() + self.LIFETIME_SECONDS
        return self._access_token


def authenticateCloudAPI(username, password):
    URL = '{}{}'.format(API, LOGIN)
    payload = json.dumps({'username': username, 'password': password})
    r = requests.post(URL, data=payload, timeout=REQUEST_TIMEOUT)
    r.raise_for_status()
    body = r.json()
    log.info('Authentication successful.')
    return AccessToken(body['access_token'], body['refresh_token'])


def refreshAccessToken(refresh_token):
    URL = '{}{}'.format(API, REFRESH)
    payload = json.dumps({'grant_type': 'refresh_token', 'refresh_token': refresh_token})
    r = requests.post(URL, data=payload, timeout=REQUEST_TIMEOUT)
    r.raise_for_status()
    return r.json()['access_token']


def listEnvironments(token):
    URL = '{}{}'.format(API, ENVS)
    headers = {'Authorization': 'Bearer {}'.format(token.value)}
    r = requests.get(URL, headers=headers, timeout=REQUEST_TIMEOUT)
    r.raise_for_status()
    body = r.json()
    if body.get('more'):
        # The docs document 'more'/'total'/'count' on this response but
        # never show how to request a further page — surface the
        # truncation instead of silently returning an incomplete list.
        log.warning("GET %s reports more results exist (total=%s, returned=%s) "
                    'but this client does not know how to page further; '
                    'some environments may be missing below.',
                    ENVS, body.get('total'), body.get('count'))
    return body['items']


def getEnvironmentDistributionId(environments, env):
    for e in environments:
        if e['name'] == env:
            return e['id'], e['distributionId']
    raise ValueError("Environment '{}' not found. Available: {}".format(
        env, [e['name'] for e in environments]))


def listBackups(token):
    URL = '{}{}'.format(API, BACKUPS)
    headers = {'Authorization': 'Bearer {}'.format(token.value)}
    r = requests.get(URL, headers=headers, timeout=REQUEST_TIMEOUT)
    r.raise_for_status()
    return r.json()


def getMostRecentBackupId(backups, environmentId):
    env_backups = [b for b in backups if b.get('environmentId') == environmentId and b.get('id')]
    if not env_backups:
        raise ValueError("No backups found for environment ID '{}'.".format(environmentId))
    most_recent = max(env_backups, key=lambda b: parser.parse(b['createdAt']))
    log.info('Most recent backup: %s (created %s)', most_recent['id'], most_recent['createdAt'])
    return most_recent['id']


def getBackupDownloadLink(token, backupId):
    """Fetch the signed S3 download URL for an already-created backup.

    Per the BRcloud API docs, 202 is this endpoint's success status — the
    body already contains the URL, valid for 15 minutes. There is no
    documented "still preparing" state to poll for here (that only applies
    to shared-datastore backup *creation*, a separate endpoint).
    """
    URL = '{}/v3/backups/{}/repositorydownloadlink'.format(API, backupId)
    headers = {'Authorization': 'Bearer {}'.format(token.value)}
    r = requests.get(URL, headers=headers, timeout=REQUEST_TIMEOUT)
    r.raise_for_status()
    return r.json()['url']


def _stream_download(url, file_name, dest_dir=None):
    """Stream-download url into file_name with a progress bar. Returns the absolute path.

    Retries on transient network failures, restarting the file from scratch
    each attempt (no partial-resume — not worth the complexity for files
    this size relative to the failure modes seen so far).
    """
    dest_path = os.path.join(dest_dir or os.getcwd(), file_name)
    for attempt in range(1, DOWNLOAD_MAX_RETRIES + 1):
        try:
            r = requests.get(url, stream=True, timeout=REQUEST_TIMEOUT)
            r.raise_for_status()
            total = int(r.headers.get('content-length', 0))
            with open(dest_path, 'wb') as f, tqdm(
                    desc=file_name,
                    total=total,
                    unit='iB',
                    unit_scale=True,
                    unit_divisor=1024,
            ) as bar:
                for chunk in r.iter_content(chunk_size=1024):
                    if chunk:
                        bar.update(f.write(chunk))
            return dest_path
        except requests.exceptions.RequestException as e:
            if attempt == DOWNLOAD_MAX_RETRIES:
                raise
            log.warning("Download of '%s' failed (attempt %d/%d): %s. Retrying in %ds...",
                        file_name, attempt, DOWNLOAD_MAX_RETRIES, e, DOWNLOAD_RETRY_BACKOFF)
            time.sleep(DOWNLOAD_RETRY_BACKOFF)


def downloadBackup(backupDownloadLink, file_name, dest_dir=None):
    log.info("Downloading backup to '%s'...", file_name)
    return _stream_download(backupDownloadLink, file_name, dest_dir=dest_dir)


def assertMysqlRunning():
    """Returns True if MySQL is running and reachable, False otherwise."""
    try:
        subprocess.check_call(
            ['mysql', '-u', 'root', '-e', ''],
            stdout=subprocess.DEVNULL,
            stderr=subprocess.DEVNULL,
        )
        return True
    except subprocess.CalledProcessError:
        return False


def loadBackupLocalMySQL(backupPath):
    print('Enter destination database user:')
    dest_user = input().strip()
    print('Enter destination database password (will not be visible):')
    dest_password = getpass.getpass()
    print('Enter destination database name:')
    dest_database = input().strip()

    cmd = ['mysql', '-u', dest_user, '-h', 'localhost',
           '--default-character-set=utf8', dest_database]
    env = os.environ.copy()
    env['MYSQL_PWD'] = dest_password  # avoids password in process list / shell history

    with open(backupPath, 'rb') as backup_file:
        result = subprocess.run(cmd, stdin=backup_file, env=env)

    if result.returncode != 0:
        raise RuntimeError('mysql import failed with exit code {}.'.format(result.returncode))


def getDistributionDownloadToken(distributionId, token):
    URL = '{}/v3/distributions/{}/download-token'.format(API, distributionId)
    headers = {'Authorization': 'Bearer {}'.format(token.value)}
    r = requests.post(URL, headers=headers, timeout=REQUEST_TIMEOUT)
    r.raise_for_status()
    return r.json()['token']


def downloadDistribution(distributionDownloadToken, file_name, dest_dir=None):
    URL = '{}/v3/distributions/download/{}'.format(API, distributionDownloadToken)
    log.info("Downloading distribution to '%s'...", file_name)
    return _stream_download(URL, file_name, dest_dir=dest_dir)


def _safe_extract(tar, dest):
    """Extract tar members, rejecting path-traversal entries (zip-slip protection)."""
    abs_dest = os.path.realpath(dest)
    for member in tar.getmembers():
        member_path = os.path.realpath(os.path.join(abs_dest, member.name))
        if not member_path.startswith(abs_dest + os.sep):
            raise ValueError("Unsafe tar entry rejected (zip-slip): '{}'".format(member.name))
    tar.extractall(dest)


def extractDistribution(distributionPath, dest=None):
    if dest is None:
        dest = os.getcwd()
    extract_location = os.path.join(dest, 'latestDist')
    os.makedirs(extract_location, exist_ok=True)
    with tarfile.open(distributionPath) as tar:
        _safe_extract(tar, extract_location)
    log.info("Distribution extracted to '%s'.", extract_location)
    return extract_location


def _collect_relative_files(root):
    """Paths of every file under root, relative to root."""
    relative_paths = set()
    for dirpath, _, filenames in os.walk(root):
        for name in filenames:
            absolute_path = os.path.join(dirpath, name)
            relative_paths.add(os.path.relpath(absolute_path, root))
    return relative_paths


def _diff_directories(left, right):
    """Deep, content-based diff of two directory trees.

    filecmp.dircmp defaults to a shallow comparison (os.stat size + mtime),
    which reports two files as identical whenever their size and mtime
    happen to match — even if their content differs. Since freshly extracted
    tarballs on each side are unlikely to share mtimes even when content IS
    identical (and could coincidentally share them when it isn't), this
    compares actual bytes instead.
    """
    left_files = _collect_relative_files(left)
    right_files = _collect_relative_files(right)
    common = left_files & right_files
    diff_files = sorted(
        rel for rel in common
        if not filecmp.cmp(os.path.join(left, rel), os.path.join(right, rel), shallow=False)
    )
    return sorted(left_files - right_files), sorted(right_files - left_files), diff_files


def buildDistributionAndCompare(projectPath, remoteExtractedPath):
    """Build local distribution and compare extracted contents with the remote."""
    subprocess.check_call(['mvn', 'clean', 'install'], cwd=projectPath)
    subprocess.check_call(['mvn', '-Pdist'], cwd=projectPath)

    local_tarballs = glob.glob(os.path.join(projectPath, 'target', '*.tar.gz'))
    if not local_tarballs:
        raise FileNotFoundError(
            "No local distribution tar.gz found under '{}/target/'.".format(projectPath))

    local_extract = os.path.join(projectPath, 'target', 'localDist')
    os.makedirs(local_extract, exist_ok=True)
    with tarfile.open(local_tarballs[0]) as tar:
        _safe_extract(tar, local_extract)

    left_only, right_only, diff_files = _diff_directories(remoteExtractedPath, local_extract)
    has_diff = bool(left_only or right_only or diff_files)
    if has_diff:
        log.warning('Differences — left_only: %s  right_only: %s  diff_files: %s',
                    left_only, right_only, diff_files)
    return not has_diff


def main():
    global USER, PASS, CLIENT, ENV, LOCAL_PROJECT_PATH, API

    args = argparser.parse_args()
    USER = args.username
    PASS = getpass.getpass("Enter Mission Control password for '{}': ".format(USER))
    CLIENT = args.clientAccount
    ENV = args.remoteEnv
    LOCAL_PROJECT_PATH = args.local_project_path
    API = 'https://api.{}.bloomreach.cloud'.format(CLIENT)

    downloaded_files = []
    extracted_dirs = []
    try:
        token = authenticateCloudAPI(USER, PASS)
        environments = listEnvironments(token)
        environmentId, distributionId = getEnvironmentDistributionId(environments, ENV)
        log.info("Resolved environment '%s' (id=%s, distributionId=%s)",
                 ENV, environmentId, distributionId)

        if args.dry_run:
            log.info('Dry-run mode: skipping downloads. Exiting.')
            return

        if not verifyBareSystemMinimum():
            sys.exit(1)

        # --- Backup ---
        if args.existing_backup:
            backupPath = args.existing_backup
            log.info("Using existing backup: %s", backupPath)
        else:
            backups = listBackups(token)
            backupId = getMostRecentBackupId(backups, environmentId)
            backupDownloadLink = getBackupDownloadLink(token, backupId)
            backupPath = downloadBackup(backupDownloadLink,
                                        '{}-{}-LATESTBACKUP.gz'.format(CLIENT, ENV),
                                        dest_dir=LOCAL_PROJECT_PATH)
            downloaded_files.append(backupPath)

        # --- Distribution ---
        if args.existing_distribution:
            distributionPath = args.existing_distribution
            log.info("Using existing distribution: %s", distributionPath)
        else:
            distributionDownloadToken = getDistributionDownloadToken(distributionId, token)
            distributionPath = downloadDistribution(
                distributionDownloadToken,
                '{}-{}-LATESTDISTRIBUTION.tar.gz'.format(CLIENT, ENV),
                dest_dir=LOCAL_PROJECT_PATH)
            downloaded_files.append(distributionPath)

        extractLocation = extractDistribution(distributionPath, dest=LOCAL_PROJECT_PATH)
        extracted_dirs.append(extractLocation)

        if buildDistributionAndCompare(LOCAL_PROJECT_PATH, extractLocation):
            log.info('Local distribution has parity with remote distribution.')
            if not assertMysqlRunning():
                log.error("MySQL is not running. Start MySQL and re-run with "
                          "--existing-backup '%s' --existing-distribution '%s'.",
                          backupPath, distributionPath)
                sys.exit(1)
            loadBackupLocalMySQL(backupPath)
            log.info("Loaded '%s' into local MySQL.", backupPath)
            log.info("To start the cloned '%s' environment, run: mvn clean install && mvn -Pcargo.run", ENV)
        else:
            log.error("Local distribution does not match remote. Inspect '%s' for differences.",
                      extractLocation)
            sys.exit(1)

    except Exception:
        log.exception('Fatal error')
        for f in downloaded_files:
            if os.path.exists(f):
                log.info("Cleaning up partial download: %s", f)
                os.remove(f)
        for d in extracted_dirs:
            if os.path.isdir(d):
                log.info("Cleaning up extracted directory: %s", d)
                shutil.rmtree(d, ignore_errors=True)
        sys.exit(1)


if __name__ == '__main__':
    main()
