Usage:

pull down remote backup and dist (5 min)

`clone-remote2local --remoteEnv {{ENV_NAME}} --clientAccount {{ACCT_NAME}} --username {{USERNAME}}`

use local pre-existing backup on alternate build/dist

`clone-remote2local --remoteEnv {{ENV_NAME}} --clientAccount {{ACCT_NAME}} --username {{USERNAME}} --backup {{LOCAL_BACKUP}} --skip-dist-check`

checker tool index and fix (30 min~)

`java -jar hippo-addon-checker-<version>.jar config > checker-repository.xml`

edit checker-repository:

> `<param name="url" value="jdbc:mysql://localhost:3306/{{DB_NAME}}"/>`
> 

`java -jar hippo-addon-checker-<version>.jar props > checker.properties`

edit checker.properties:

> `rep.config=checker-repository.xml`
> 
`java -jar hippo-addon-checker-<version>.jar check`

`java -jar hippo-addon-checker-<version>.jar fix`


stand it up (5 min)

`mvn clean install -DskipTests && mvn -Pcargo.run -Drepo.bootstrap=full`
