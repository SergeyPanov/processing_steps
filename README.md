# Description.
Programming part of thesis. More info [here](https://github.com/SergeyPanov/thesis)
# Server
## Compiling
Server application need the be compiled from `./processing_steps/7/corproc` using `mvn package` command.
## Indexation
Indexation is initiated by command `java -jar target/corpproc-1.0-SNAPSHOT-jar-with-dependencies.jar -c src/main/resources/config.yaml index path/to/collection path/to/collection/final`
## Run server
Servers run by command `java -jar target/corpproc-1.0-SNAPSHOT-jar-with-dependencies.jar -c src/main/resources/config.yaml serve path/to/collection/final`

# Client
## Compiling
Client application need to be compiled from `./processing_steps/8a/mg4jquery` using command `mvn compile assembly:single`

## Querying
Get 15 snippets containde `"nertag:person"`: `java -jar target/mg4jquery-0.0.1-SNAPSHOT-jar-with-dependencies.jar -s 15 -q  "nertag:person" -h cesta/servers.txt -p src/resources/config.xml`

Get next 15 snippets `java -jar target/mg4jquery-0.0.1-SNAPSHOT-jar-with-dependencies.jar -s 15 -q  "nertag:person" -h cesta/servers.txt -p src/resources/config.xml -n`

Get 7th document `java -jar target/mg4jquery-0.0.1-SNAPSHOT-jar-with-dependencies.jar -p src/resources/config.xml -g -k 127.0.0.1:12000 -d 7 -t 0  -h cesta/servers.txt`

