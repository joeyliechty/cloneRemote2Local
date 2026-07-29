Usage:

pull down remote backup and dist
`clone-remote2local --remoteEnv {{ENV_NAME}} --clientAccount {{ACCT_NAME}} --username {{USERNAME}}`

use local pre-existing backup on alternate build/dist
`clone-remote2local --remoteEnv {{ENV_NAME}} --clientAccount {{ACCT_NAME}} --username {{USERNAME}} --backup {{LOCAL_BACKUP}} --skip-dist-check`

checker tool index and fix
`java -jar hippo-addon-checker-<version>.jar config > checker-repository.xml`
`java -jar hippo-addon-checker-<version>.jar props > checker.properties`
edit checker.properties:
> `rep.config=checker-repository.xml`
`java -jar hippo-addon-checker-<version>.jar check`
`java -jar hippo-addon-checker-<version>.jar fix`

stand it up
`mvn clean install -DskipTests && mvn -Pcargo.run -Drepo.bootstrap=full`
