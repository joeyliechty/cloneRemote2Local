import json
import argparse
import requests
import os
from dateutil import parser
from tqdm import tqdm
from shutil import which
import subprocess
import sys
import tarfile
import filecmp
import getpass

'''
 This script is meant as a jumpstart to loading a remote OD2 instance into your localhost.
 It needs a few prerequisites:
 1. python 3
 2. requests installed via pip

 It will recieve 3 required arguments:
 1. remote Environment name
 2. client account name
 3. username of admin mission control user
 
 It will do a few things:
 1. verify bare minimum system prerequisites: java, mysql, maven 
 2. prompt you for your mission control password
 3. authenticate the BRcloud API and receive token
 4. identify and store the remote environment details
 5. locate the latest db dump of the specified environment
 6. download the latest db dump of the specified environment
 7. locate the latest distribution file of the specified environment
 8. download the latest distribution file of the specified environment
 9. compare your local environment to the remote distribution to ensure parity
 10. load the remote db dump into a local mysql DB
 11. on successful parity check, prompt user to start cargo to 'clone' remote env. 
'''
argparser = argparse.ArgumentParser(description='authenticate, download latest DB, run in local')
argparser.add_argument('--remoteEnv', action="store", required=True, help="name of remote environment you wish to clone locally")
argparser.add_argument('--clientAccount', action="store", required=True, help="name of client account")
argparser.add_argument('--username', action="store", required=True, help="username to authenticate mission control cloud api")
#argparser.add_argument('--password', action="store", required=True, help="password to authenticate mission control cloud api")
argparser.set_defaults(feature=True)
args = argparser.parse_args()

USER = args.username
print("type '{}' mission control user password".format(USER))
PASS = getpass.getpass()
CLIENT = args.clientAccount
ENV = args.remoteEnv
API = "https://api.{}.bloomreach.cloud".format(CLIENT)

LOGIN = "/v3/authn/access_token"
ENVS = "/v3/environments"
BACKUPS = "/v3/backups"
DISTS = "/v3/distributions"

def verifyBareSystemMinimum():
  # check for maven, java, mysql
  if which('mysql') is not None and which('java') is not None and which('mvn') is not None:
    return True

def authenticateCloudAPI(username, password):
  URL = '{}{}'.format(API,LOGIN)
  payload = json.dumps({"username": username, "password": password})
  r = requests.post(URL, data=payload)
  if r.status_code == 200:
    token = r.json()['access_token']
    return token
  else:
    print("error {} {}".format(r.status_code, r.text))
    return False

def listEnvironments(token):
  URL = '{}{}'.format(API,ENVS)
  headers = {'Authorization': 'Bearer {}'.format(token)}
  r = requests.get(URL, headers=headers)
  if r.status_code == 200:
    environments = r.json()['items']
    return environments
  else:
    print("error {} {}".format(r.status_code, r.text))
    return False

def getEnvironmentDistributionId(environments, env):
  for e in environments:
    if e['name'] == env:
      return e['id'], e['distributionId']

def listBackups(token):
  URL = '{}{}'.format(API,BACKUPS)
  headers = {'Authorization': 'Bearer {}'.format(token)}
  r = requests.get(URL, headers=headers)
  if r.status_code == 200:
    backups = r.json()
    return backups
  else:
    print("error {} {}".format(r.status_code, r.text))
    return False

def getMostRecentBackupId(backups, environmentId):
  backup_dates = [parser.parse(i['createdAt']).date() for i in backups if i['environmentId'] == environmentId]
  max_date = max(backup_dates)
  for b in backups:
    if parser.parse(b['createdAt']).date() == max_date:
      if b['id'] != None:
        return b['id']

def getBackupDownloadLink(token, backupId):
  BACKUPLINK = "/v3/backups/{}/repositorydownloadlink".format(backupId)
  URL = '{}{}'.format(API,BACKUPLINK)
  headers = {'Authorization': 'Bearer {}'.format(token)}
  r = requests.get(URL, headers=headers)
  if r.status_code in [200,202]:
    backupDownloadLink = r.json()['url']
    return backupDownloadLink
  else:
    print("error {} {}".format(r.status_code, r.text))
    return False

def downloadBackup(backupDownloadLink, file_name):
  r = requests.get(backupDownloadLink,stream=True)
  total = int(r.headers.get('content-length', 0))
  backupPath = "{}/{}".format(os.getcwd(), file_name)
  with open(file_name, 'wb') as f,tqdm(
        desc=file_name,
        total=total,
        unit='iB',
        unit_scale=True,
        unit_divisor=1024,
    ) as bar:
    for chunk in r.iter_content(chunk_size=1024):
      if chunk: # filter out keep-alive new chunks
        size = f.write(chunk)
        bar.update(size)
  return backupPath

def assertMysqlRunning():
  """Returns True if MySQL is running, False otherwise."""
  try:
    subprocess.call(['mysql', '-u', 'root', '-e', ''])
    return True
  except subprocess.CalledProcessError:
    return False

def loadBackupLocalMySQL(backupPath):
  print("Enter destination database user:")
  dest_user = input()

  print("Enter destination database password: (Password will not be visible)")
  dest_password = getpass.getpass()

  print("Enter destination database name:")
  dest_database = input()

  os.popoen("mysql -u %s --password%s -h %s --default-character-set=utf8 %s < %s" % (dest_user,dest_password,"localhost",dest_database,backupPath))
  return True

def verifyXMProjectConfig():

  return True

def getDistributionDownloadToken(distributionId, token):
  DOWNLOADDISTTOKENURL = "/v3/distributions/{}/download-token".format(distributionId)
  URL = '{}{}'.format(API,DOWNLOADDISTTOKENURL)
  headers = {'Authorization': 'Bearer {}'.format(token)}
  r = requests.post(URL, headers=headers)
  if r.status_code in [200,202]:
    distributionDownloadToken = r.json()['token']
    return distributionDownloadToken
  else:
    print("error {} {}".format(r.status_code, r.text))
    return False

def downloadDistribution(distributionDownloadToken, file_name):
  DOWNLOADDISTRIBUTIONURL = "/v3/distributions/download/{}".format(distributionDownloadToken)
  URL = '{}{}'.format(API,DOWNLOADDISTRIBUTIONURL)
  r = requests.get(URL,stream=True)
  total = int(r.headers.get('content-length', 0))
  distributionPath = "{}/{}".format(os.getcwd(), file_name)
  with open(file_name, 'wb') as f,tqdm(
        desc=file_name,
        total=total,
        unit='iB',
        unit_scale=True,
        unit_divisor=1024,
    ) as bar:
    for chunk in r.iter_content(chunk_size=1024):
      if chunk: # filter out keep-alive new chunks
        size = f.write(chunk)
        bar.update(size)
  return distributionPath

def extractDistribution(distributionPath, dest=os.getcwd()):
  file = tarfile.open(distributionPath)
  extractLocation = "{}/{}".format(dest,"latestDist")
  file.extractall(extractLocation)
  file.close()
  return extractLocation

def buildDistributionAndCompare(rootPomPath, remoteDistributionPath):
  subprocess.check_call(['mvn', 'clean', 'install'], shell=True)
  subprocess.check_call(['mvn', '-Pdist'], shell=True)
  localDistributionPath = "{}/target/*.tar.gz".format(os.cwd())
  distributionParity = filecmp.cmp(remoteDistributionPath, localDistributionPath, shallow=True)
  return distributionParity

if __name__ == '__main__':
  verifyBareSystemMinimum()
  token = authenticateCloudAPI(USER, PASS)
  environments = listEnvironments(token)
  environmentId, distributionId = getEnvironmentDistributionId(environments, ENV)
  backups = listBackups(token)
  backupId = getMostRecentBackupId(backups, environmentId)
  backupDownloadLink = getBackupDownloadLink(token, backupId)
  backupPath = downloadBackup(backupDownloadLink, "{}-{}-LATESTBACKUP.gz".format(CLIENT,ENV))
  distributionDownloadToken = getDistributionDownloadToken(distributionId, token)
  distributionFileName = "{}-{}-LATESTDISTRIBUTION.tar.gz".format(CLIENT,ENV)
  distributionPath = downloadDistribution(distributionDownloadToken, distributionFileName)
  extractLocation = extractDistribution(distributionPath)
  if (buildDistributionAndCompare(os.getcwd(), distributionPath)):
    print("Local Distribution has parity with Remote Distribution.")
    if(assertMysqlRunning()): loadBackupLocalMySQL(backupPath)
    print("Loaded {} into local mysql.".format(backupPath))
    print("To begin 'cloned' {} environment, run 'mvn clean install && mvn -Pcargo.run'".format(ENV))
  else:
    print("Your current local distribution does not match the remote.")
    print("Inspect {} for differences. Exiting program.".format(extractLocation))
  


