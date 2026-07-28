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
import xml.etree.ElementTree as ET

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

 Set $MISSION_CONTROL_PASSWORD to skip the interactive password prompt.

 Optional arguments:
 --local-project-path   : path to local XM project (default: cwd)
 --dry-run              : resolve environment/backups without downloading anything
 --backup               : path to a previously downloaded backup to skip re-download
 --dist                 : path to a previously downloaded distribution to skip re-download

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
argparser.add_argument('--backup', action='store', default=None,
                       help='path to an existing backup file — skips backup download')
argparser.add_argument('--dist', action='store', default=None,
                       help='path to an existing distribution file — skips distribution download')
argparser.add_argument('--skip-dist-check', action='store_true', default=False,
                       help='load the backup into the local checkout as-is, without downloading, '
                            'building, or comparing the distribution — use this to see how a local '
                            'checkout behaves against a remote DB backup, regardless of parity '
                            'with what is actually deployed')
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

MYSQL_SERVICE_START_TIMEOUT = 30   # seconds to wait for the start command itself
MYSQL_START_WAIT_ATTEMPTS = 10     # polls for MySQL to become reachable after starting it
MYSQL_START_WAIT_DELAY_SECONDS = 2  # seconds between those polls

MISSION_CONTROL_PASSWORD_ENV_VAR = 'MISSION_CONTROL_PASSWORD'

# Values matching this project's own working Docker MySQL path
# (src/main/docker/Dockerfile's REPO_WORKSPACE_BUNDLE_CACHE / REPO_VERSIONING_BUNDLE_CACHE
# and MYSQL_DB_DRIVER), reused here so a local cargo.run matches it.
MYSQL_LOCAL_CLUSTER_NODE_ID = 'local'
MYSQL_LOCAL_REPO_WORKSPACE_BUNDLE_CACHE = '256'
MYSQL_LOCAL_REPO_VERSIONING_BUNDLE_CACHE = '64'
MYSQL_JDBC_DRIVER_CLASS = 'com.mysql.cj.jdbc.Driver'
MYSQL_CONNECTOR_ARTIFACT_ID = 'mysql-connector-j'

JAVA_VERSION_PROPERTY_NAMES = (
    'java.version', 'maven.compiler.release', 'maven.compiler.target', 'maven.compiler.source')
SDKMAN_JAVA_CANDIDATES_DIR = os.path.expanduser('~/.sdkman/candidates/java')


def verifyBareSystemMinimum():
    missing = [tool for tool in ('mysql', 'java', 'mvn') if which(tool) is None]
    if missing:
        log.error('Missing required tools: %s', ', '.join(missing))
        return False
    return True


def _resolveMissionControlPassword(username):
    """Mission Control password from $MISSION_CONTROL_PASSWORD if set (avoids
    retyping it on every run), otherwise an interactive prompt.

    The env var is visible to anything that can read this process's
    environment (e.g. `ps eww`, child processes) — only set it in a private
    shell profile, not inline on a shared machine.
    """
    envPassword = os.environ.get(MISSION_CONTROL_PASSWORD_ENV_VAR)
    if envPassword:
        log.info('Using Mission Control password from $%s.', MISSION_CONTROL_PASSWORD_ENV_VAR)
        return envPassword
    return getpass.getpass("Enter Mission Control password for '{}': ".format(username))


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


def _brewMysqlServiceName():
    """Name of the Homebrew service running a local MySQL/MariaDB, or None
    if Homebrew isn't installed or lists nothing matching."""
    if which('brew') is None:
        return None
    try:
        result = subprocess.run(['brew', 'services', 'list'],
                                 capture_output=True, text=True, timeout=MYSQL_SERVICE_START_TIMEOUT)
    except (subprocess.SubprocessError, OSError) as e:
        log.warning("'brew services list' failed: %s", e)
        return None
    for line in result.stdout.splitlines()[1:]:  # skip header row
        columns = line.split()
        if not columns:
            continue
        name = columns[0]
        if 'mysql' in name.lower() or 'mariadb' in name.lower():
            return name
    return None


def _waitForMysql(attempts=MYSQL_START_WAIT_ATTEMPTS, delay=MYSQL_START_WAIT_DELAY_SECONDS):
    """Polls assertMysqlRunning() until it succeeds or attempts run out."""
    for attempt in range(1, attempts + 1):
        if assertMysqlRunning():
            return True
        if attempt < attempts:
            time.sleep(delay)
    return False


def startLocalMysql():
    """Best-effort attempt to bring up a local MySQL, trying whichever
    service manager this machine's install uses. Returns True once MySQL
    is reachable, False if no known start method worked.
    """
    serviceName = _brewMysqlServiceName()
    if serviceName:
        startCmd = ['brew', 'services', 'start', serviceName]
    elif which('mysql.server') is not None:
        startCmd = ['mysql.server', 'start']
    else:
        log.warning('No known way to start MySQL automatically on this machine '
                    '(neither a brew mysql/mariadb service nor mysql.server was found).')
        return False

    log.info("Starting MySQL via '%s'...", ' '.join(startCmd))
    try:
        subprocess.run(startCmd, stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL,
                        timeout=MYSQL_SERVICE_START_TIMEOUT)
    except (subprocess.SubprocessError, OSError) as e:
        log.warning("'%s' failed: %s", ' '.join(startCmd), e)
        return False

    return _waitForMysql()


def ensureMysqlRunning():
    """Returns True if MySQL is already reachable, or becomes reachable
    after a best-effort automatic start attempt."""
    if assertMysqlRunning():
        return True
    log.info('MySQL is not running locally; attempting to start it...')
    return startLocalMysql()


def _ensureDatabaseExists(user, database, env):
    """Creates `database` via CREATE DATABASE IF NOT EXISTS.

    The dumps produced by this tooling are plain table dumps with no
    CREATE DATABASE/USE statements of their own, and `mysql db < dump.sql`
    requires db to already exist — so a first-time destination name would
    otherwise fail with "Unknown database".
    """
    escapedName = database.replace('`', '``')
    cmd = ['mysql', '-u', user, '-h', 'localhost', '-e',
           'CREATE DATABASE IF NOT EXISTS `{}`'.format(escapedName)]
    result = subprocess.run(cmd, env=env, stdout=subprocess.DEVNULL, stderr=subprocess.PIPE, text=True)
    if result.returncode != 0:
        raise RuntimeError("Could not create database '{}': {}".format(database, result.stderr.strip()))


def loadBackupLocalMySQL(backupPath):
    print('Enter destination database user:')
    dest_user = input().strip()
    print('Enter destination database password (will not be visible):')
    dest_password = getpass.getpass()
    print('Enter destination database name:')
    dest_database = input().strip()

    env = os.environ.copy()
    env['MYSQL_PWD'] = dest_password  # avoids password in process list / shell history

    _ensureDatabaseExists(dest_user, dest_database, env)

    cmd = ['mysql', '-u', dest_user, '-h', 'localhost',
           '--default-character-set=utf8', '--binary-mode', dest_database]

    # backupPath is the .gz file downloaded from Mission Control — piping it
    # into mysql directly feeds it raw compressed bytes instead of SQL text.
    gunzip = subprocess.Popen(['gunzip', '-c', backupPath], stdout=subprocess.PIPE)
    try:
        result = subprocess.run(cmd, stdin=gunzip.stdout, env=env)
    finally:
        gunzip.stdout.close()
        gunzip.wait()

    if gunzip.returncode != 0:
        raise RuntimeError('gunzip failed with exit code {}.'.format(gunzip.returncode))
    if result.returncode != 0:
        raise RuntimeError('mysql import failed with exit code {}.'.format(result.returncode))

    return dest_user, dest_password, dest_database


def _readFile(path):
    with open(path, 'r') as f:
        return f.read()


def _writeFile(path, content):
    with open(path, 'w') as f:
        f.write(content)


def _resolveRepositoryMysqlTemplate(templateContent, database):
    return (templateContent
            .replace('@mysql.repo.db@', database)
            .replace('@cluster.node.id@', MYSQL_LOCAL_CLUSTER_NODE_ID)
            .replace('@repo.workspace.bundle.cache@', MYSQL_LOCAL_REPO_WORKSPACE_BUNDLE_CACHE)
            .replace('@repo.versioning.bundle.cache@', MYSQL_LOCAL_REPO_VERSIONING_BUNDLE_CACHE))


def _contextXmlHasMysqlResource(contextXmlPath, database):
    resourceName = 'jdbc/{}'.format(database)
    root = ET.parse(contextXmlPath).getroot()
    return any(resource.get('name') == resourceName for resource in root.iter('Resource'))


def _buildMysqlContextResource(user, password, database):
    return (
        '    <Resource\n'
        '      name="jdbc/{database}" auth="Container" type="javax.sql.DataSource"\n'
        '      maxTotal="20" maxIdle="10" initialSize="2" maxWaitMillis="10000"\n'
        '      testWhileIdle="true" testOnBorrow="false" validationQuery="SELECT 1"\n'
        '      timeBetweenEvictionRunsMillis="10000"\n'
        '      minEvictableIdleTimeMillis="60000"\n'
        '      username="{user}" password="{password}"\n'
        '      driverClassName="{driver}"\n'
        '      url="jdbc:mysql://localhost:3306/{database}'
        '?characterEncoding=utf8&amp;useSSL=false&amp;allowPublicKeyRetrieval=true"/>\n'
    ).format(database=database, user=user, password=password, driver=MYSQL_JDBC_DRIVER_CLASS)


def _addMysqlResourceToContextXml(contextXmlPath, user, password, database):
    if _contextXmlHasMysqlResource(contextXmlPath, database):
        return False
    content = _readFile(contextXmlPath)
    resourceXml = _buildMysqlContextResource(user, password, database)
    _writeFile(contextXmlPath, content.replace('</Context>', resourceXml + '</Context>', 1))
    return True


def _addRepoConfigSystemProperty(pomXmlPath):
    content = _readFile(pomXmlPath)
    if '<repo.config>' in content:
        return False
    anchor = '<log4j.configurationFile>${project.basedir}/conf/log4j2-dev.xml</log4j.configurationFile>'
    if anchor not in content:
        log.warning("Could not find the expected cargo.run systemProperties anchor in '%s'; "
                    'skipping repo.config wiring.', pomXmlPath)
        return False
    replacement = anchor + '\n                  <repo.config>${project.basedir}/conf/repository.xml</repo.config>'
    _writeFile(pomXmlPath, content.replace(anchor, replacement, 1))
    return True


def _addMysqlConnectorDependency(cmsPomPath):
    content = _readFile(cmsPomPath)
    if '<artifactId>{}</artifactId>'.format(MYSQL_CONNECTOR_ARTIFACT_ID) in content:
        return False
    closing = '  </dependencies>'
    if closing not in content:
        log.warning("Could not find '</dependencies>' in '%s'; skipping mysql-connector-j wiring.", cmsPomPath)
        return False
    dependencyXml = (
        '    <dependency>\n'
        '      <groupId>com.mysql</groupId>\n'
        '      <artifactId>{}</artifactId>\n'
        '      <scope>provided</scope>\n'
        '    </dependency>\n'
    ).format(MYSQL_CONNECTOR_ARTIFACT_ID)
    _writeFile(cmsPomPath, content.replace(closing, dependencyXml + closing, 1))
    return True


def _fixDockerMysqlConnectorCoordinates(dockerDbLibsPath):
    content = _readFile(dockerDbLibsPath)
    stale = 'mysql:mysql-connector-java'
    if stale not in content:
        return False
    _writeFile(dockerDbLibsPath, content.replace(stale, 'com.mysql:{}'.format(MYSQL_CONNECTOR_ARTIFACT_ID)))
    return True


def configureLocalProjectForMysql(projectPath, user, password, database):
    """Wires a local checkout's cargo.run to read from a locally-imported
    MySQL backup, if it isn't already configured to.

    conf/repository.xml existing is treated as "already configured" — a
    no-op that leaves everything untouched. Each individual step below is
    independently idempotent too, so a partially-configured project only
    gets what it's missing.
    """
    repositoryXmlPath = os.path.join(projectPath, 'conf', 'repository.xml')
    if os.path.isfile(repositoryXmlPath):
        log.info("'%s' already exists; leaving local MySQL configuration as-is.", repositoryXmlPath)
        return False

    repositoryTemplatePath = os.path.join(projectPath, 'conf', 'repository-mysql.xml')
    contextTemplatePath = os.path.join(projectPath, 'conf', 'context-mysql.xml')
    if not os.path.isfile(repositoryTemplatePath) or not os.path.isfile(contextTemplatePath):
        log.warning("'%s' has no conf/repository-mysql.xml / conf/context-mysql.xml templates; "
                    'skipping local MySQL auto-configuration.', projectPath)
        return False

    _writeFile(repositoryXmlPath, _resolveRepositoryMysqlTemplate(_readFile(repositoryTemplatePath), database))
    _addMysqlResourceToContextXml(os.path.join(projectPath, 'conf', 'context.xml'), user, password, database)
    _addRepoConfigSystemProperty(os.path.join(projectPath, 'pom.xml'))
    _addMysqlConnectorDependency(os.path.join(projectPath, 'cms', 'pom.xml'))
    _fixDockerMysqlConnectorCoordinates(
        os.path.join(projectPath, 'src', 'main', 'docker', 'assembly', 'docker-db-libs.xml'))

    log.info("Configured '%s' to read from local MySQL database '%s' "
             '(run `mvn clean install` before `mvn -Pcargo.run`).', projectPath, database)
    return True


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


def _pom_local_tag(element):
    return element.tag.rsplit('}', 1)[-1]


def _normalizeJavaMajorVersion(version):
    """'1.8' -> '8', '17' -> '17', '11.0.2' -> '11'."""
    parts = version.split('.')
    if len(parts) >= 2 and parts[0] == '1':
        return parts[1]
    return parts[0]


def getRequiredJavaVersion(projectPath):
    """Best-effort read of the project's declared Java version from pom.xml.

    Returns a normalized major version string (e.g. '17') or None if the
    project has no pom.xml, or none of the commonly-used version properties
    are declared in it — callers should treat None as "unknown" and leave
    whatever JDK is already active rather than guess.
    """
    pom_path = os.path.join(projectPath, 'pom.xml')
    if not os.path.isfile(pom_path):
        return None
    try:
        root = ET.parse(pom_path).getroot()
    except ET.ParseError:
        return None
    properties = next((child for child in root if _pom_local_tag(child) == 'properties'), None)
    if properties is None:
        return None
    declared = {_pom_local_tag(prop): (prop.text or '').strip() for prop in properties}
    for name in JAVA_VERSION_PROPERTY_NAMES:
        if declared.get(name):
            return _normalizeJavaMajorVersion(declared[name])
    return None


def _sdkmanCandidateMajorVersion(candidateName):
    """SDKMAN candidate dir names look like '17.0.9-tem', '1.8.0_392-zulu'."""
    return _normalizeJavaMajorVersion(candidateName.split('-', 1)[0])


def findSdkmanJavaHome(majorVersion):
    """Find an SDKMAN-installed JDK candidate matching majorVersion (e.g. '17').

    Returns the candidate's absolute path, or None if SDKMAN isn't installed
    or no installed candidate matches.
    """
    if not os.path.isdir(SDKMAN_JAVA_CANDIDATES_DIR):
        return None
    matches = sorted(
        name for name in os.listdir(SDKMAN_JAVA_CANDIDATES_DIR)
        if name != 'current' and _sdkmanCandidateMajorVersion(name) == majorVersion
    )
    if not matches:
        return None
    return os.path.join(SDKMAN_JAVA_CANDIDATES_DIR, matches[-1])


def _mvnEnvironmentForProject(projectPath):
    """Build the subprocess environment for `mvn`, switched to the project's
    declared Java version via an SDKMAN-installed candidate when possible.

    Only affects the environment passed to the mvn subprocess — never the
    parent shell — so it composes safely regardless of whichever JDK the
    invoking shell currently has active.
    """
    env = os.environ.copy()
    requiredVersion = getRequiredJavaVersion(projectPath)
    if requiredVersion is None:
        log.info('No Java version declared in pom.xml; using the currently active JDK.')
        return env

    javaHome = findSdkmanJavaHome(requiredVersion)
    if javaHome is None:
        log.warning("Project requires Java %s but no matching SDKMAN candidate is installed "
                    "under '%s'; using the currently active JDK. Install it with "
                    "'sdk install java %s'.", requiredVersion, SDKMAN_JAVA_CANDIDATES_DIR, requiredVersion)
        return env

    log.info("Using SDKMAN-managed Java %s for the build ('%s').", requiredVersion, javaHome)
    env['JAVA_HOME'] = javaHome
    env['PATH'] = os.path.join(javaHome, 'bin') + os.pathsep + env.get('PATH', '')
    return env


def _runMavenBuild(projectPath):
    env = _mvnEnvironmentForProject(projectPath)
    subprocess.check_call(['mvn', 'clean', 'install'], cwd=projectPath, env=env)
    subprocess.check_call(['mvn', '-Pdist'], cwd=projectPath, env=env)


def buildDistributionAndCompare(projectPath, remoteExtractedPath):
    """Build local distribution and compare extracted contents with the remote."""
    _runMavenBuild(projectPath)

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
    CLIENT = args.clientAccount
    ENV = args.remoteEnv
    LOCAL_PROJECT_PATH = args.local_project_path
    API = 'https://api.{}.bloomreach.cloud'.format(CLIENT)

    # Nothing from Mission Control (a token, environment/distribution ids) is
    # needed once every artifact this run actually uses is already on disk —
    # except for --dry-run, whose entire purpose is validating that auth and
    # environment resolution succeed. --skip-dist-check never uses the
    # distribution at all, so --dist isn't part of that requirement then.
    neededArtifactsOnDisk = bool(args.backup) if args.skip_dist_check else bool(args.backup and args.dist)
    needsAuth = not neededArtifactsOnDisk or args.dry_run

    downloaded_files = []
    try:
        token = environmentId = distributionId = None
        if needsAuth:
            PASS = _resolveMissionControlPassword(USER)
            token = authenticateCloudAPI(USER, PASS)
            environments = listEnvironments(token)
            environmentId, distributionId = getEnvironmentDistributionId(environments, ENV)
            log.info("Resolved environment '%s' (id=%s, distributionId=%s)",
                     ENV, environmentId, distributionId)

            if args.dry_run:
                log.info('Dry-run mode: skipping downloads. Exiting.')
                return
        elif args.skip_dist_check:
            log.info("--backup provided with --skip-dist-check; skipping Mission Control authentication.")
        else:
            log.info("Both --backup and --dist provided; skipping Mission Control authentication.")

        if not verifyBareSystemMinimum():
            sys.exit(1)

        # --- Backup ---
        if args.backup:
            backupPath = args.backup
            log.info("Using existing backup: %s", backupPath)
        else:
            backups = listBackups(token)
            backupId = getMostRecentBackupId(backups, environmentId)
            backupDownloadLink = getBackupDownloadLink(token, backupId)
            backupPath = downloadBackup(backupDownloadLink,
                                        '{}-{}-LATESTBACKUP.gz'.format(CLIENT, ENV),
                                        dest_dir=LOCAL_PROJECT_PATH)
            downloaded_files.append(backupPath)

        if args.skip_dist_check:
            log.warning('--skip-dist-check set: loading the backup into the local checkout as-is, '
                        'without verifying it matches what is actually deployed remotely.')
            if not ensureMysqlRunning():
                log.error("Could not start MySQL. Start it manually and re-run with "
                          "--backup '%s'.", backupPath)
                sys.exit(1)
            dest_user, dest_password, dest_database = loadBackupLocalMySQL(backupPath)
            log.info("Loaded '%s' into local MySQL.", backupPath)
            configureLocalProjectForMysql(LOCAL_PROJECT_PATH, dest_user, dest_password, dest_database)
            log.info("To start the local checkout, run: mvn clean install && mvn -Pcargo.run")
            return

        # --- Distribution ---
        if args.dist:
            distributionPath = args.dist
            log.info("Using existing distribution: %s", distributionPath)
        else:
            distributionDownloadToken = getDistributionDownloadToken(distributionId, token)
            distributionPath = downloadDistribution(
                distributionDownloadToken,
                '{}-{}-LATESTDISTRIBUTION.tar.gz'.format(CLIENT, ENV),
                dest_dir=LOCAL_PROJECT_PATH)
            downloaded_files.append(distributionPath)

        extractLocation = extractDistribution(distributionPath, dest=LOCAL_PROJECT_PATH)

        if buildDistributionAndCompare(LOCAL_PROJECT_PATH, extractLocation):
            log.info('Local distribution has parity with remote distribution.')
            if not ensureMysqlRunning():
                log.error("Could not start MySQL. Start it manually and re-run with "
                          "--backup '%s' --dist '%s'.",
                          backupPath, distributionPath)
                sys.exit(1)
            dest_user, dest_password, dest_database = loadBackupLocalMySQL(backupPath)
            log.info("Loaded '%s' into local MySQL.", backupPath)
            configureLocalProjectForMysql(LOCAL_PROJECT_PATH, dest_user, dest_password, dest_database)
            log.info("To start the cloned '%s' environment, run: mvn clean install && mvn -Pcargo.run", ENV)
        else:
            log.error("Local distribution does not match remote (see '%s' for differences); "
                      'discarding the downloaded backup/distribution since they do not '
                      'correspond to this local checkout.', extractLocation)
            for f in downloaded_files:
                if os.path.exists(f):
                    log.info('Cleaning up: %s', f)
                    os.remove(f)
            sys.exit(1)

    except Exception:
        log.exception('Fatal error')
        sys.exit(1)


if __name__ == '__main__':
    main()
