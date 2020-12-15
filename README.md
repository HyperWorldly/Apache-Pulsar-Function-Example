# JO - JOB OPENER

This repository houses the following (Java) [Apache Pulsar Function](https://pulsar.apache.org/docs/en/functions-overview/):

| Function name                                                                                                                                                          | Description                                                                                                                                                                                                                                                                                   | API used                                                                               |
| :--------------------------------------------------------------------------------------------------------------------------------------------------------------------- | :-------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- | :------------------------------------------------------------------------------------- |
| [`JobOpener`](src/main/java/job/opener/sdk/JobOpener.java) | Saves (to database all the details of) a new (instant) job request from a consumer. Then finds (5) online and in-proximity (within 10000 meters radius) workers who can perform the required tasks (marked required by the consumer). Then writes job offer to workers' topics and marks their status as ON-OFFER. (2 seconds) Later monitors the sent offer for (read) receipt, marking OFFLINE the workers who (were sent the offer but) did not respond with a (read) receipt. (After further 9 seconds) checks for acceptance by any of the workers (who received the job offer), marking as ONLINE again the workers who (received the offer but) did not accept. Responds to the consumer accordingly with success (containing job and worker IDs) or with failure (containing reason). | [Pulsar Java SDK](https://pulsar.apache.org/docs/en/functions-api/#java-sdk-functions) |

The function has been tested in [localrun mode](https://pulsar.apache.org/docs/en/functions-debug/#debug-with-localrun-mode), but can be run in Docker standalone cluster mode too (with minor changes stated in step 3 of LATER RUNS).

## SETUP

##### FIRST RUN
1.  Install [Java Runtime Environment](https://java.com/en/download/) & [Java Development Kit](https://www.oracle.com/java/technologies/javase-downloads.html).
2.  Install [Maven](https://maven.apache.org/download.cgi).
3.  Clone this repository & load it into an IDE (e.g., [Eclipse](https://www.eclipse.org/eclipseide/)).
4.  Install [Docker Desktop for Windows](https://hub.docker.com/editions/community/docker-ce-desktop-windows) (with support for Linux Containers, as opposed to Windows Containers).  
    If you have Windows Home, see [Installation for Windows Home](https://docs.docker.com/docker-for-windows/install-windows-home/).
5.  Install [Apache Pulsar](https://github.com/apache/pulsar) as a [standalone cluster in Docker](https://pulsar.apache.org/docs/en/standalone-docker/):  
    `docker run -it -p 6650:6650 -p 8080:8080 --name pulsar --mount source=pulsardata,target=/pulsar/data --mount source=pulsarconf,target=/pulsar/conf apachepulsar/pulsar:2.5.2 bin/pulsar standalone`  
    Use `apachepulsar/pulsar:latest` for the latest version.  
    NOTE: Since this is being run as an interactive terminal, it will never return for new command(s) to be issued, and will keep providing you with logs. Assuming that the installation is complete when the `INFO` messages start intervals.  
6.  Install [XAMPP](https://www.apachefriends.org/download.html).
7.  Run Apache and MySQL from XAMPP Control Panel.
8.  [Add a user account](http://localhost/phpmyadmin/server_privileges.php?adduser) named `hyperworldly`, hostname `localhost`, and password from this function's `process` method. Also check `Create database with same name and grant all privileges` on the page.

##### LATER RUNS

1.  [Import](http://localhost/phpmyadmin/db_import.php?db=hyperworldly) the latest version of [hyperworldly.sql](https://bitbucket.org/hyperworldly/sql-db-export/src/master/hyperworldly.sql).  
2.  Run [CLI Pulsar consumers](https://pulsar.apache.org/docs/en/reference-cli-tools/#consume), listening to the required topics (e.g., the topics that this function writes to, or the function's input, output and log topics, as specified in the `FunctionConfig` under `main` method):  
    1.  Open new BASH for the Pulsar (Docker) container:  
        `docker exec -it pulsar /bin/bash`  
    2.  Subscribe to and consume messages for a topic:  
        `bin/pulsar-client consume persistent://public/default/job-opener-input --num-messages 0 --subscription-name JOB-OPENER-INPUT-SUB --subscription-type Exclusive`  
    Logs for the function are printed to the IDE's output on run, so you don't need a consumer for the logs topic.  
3.  Run the function:  
    -   In [localrun mode](https://pulsar.apache.org/docs/en/functions-debug/#debug-with-localrun-mode):  
        1.  Implement a `main` method with appropriate `FunctionConfig`  
            NOTE: Already done.  
        2.  Run the function in your IDE (e.g., [Eclipse](https://www.eclipse.org/eclipseide/)).  
    -   In the Docker standalone cluster mode:  
        1. 	Remove the `main` method from the function.
        2.  Change `DB_URL` to `jdbc:mysql://host.docker.internal/hyperworldly?serverTimezone=UTC` to access XAMPP's MySQL.
        3.  Create a "fat jar" from the cloned repository:  
            `mvn package`  
            Running this command produces 2 JARs (with and without dependencies, accordingly named) in the folder named `target`.  
        4.  Copy the `...-jar-with-dependencies.jar` from the `target` folder, to the `/pulsar` folder in the Docker container named `pulsar`. From the IDE's terminal:  
            `docker cp .\target\job-opener-1.0-jar-with-dependencies.jar pulsar:/pulsar`  
        5.  Open BASH for the (Docker) container running Pulsar:  
            `docker exec -it pulsar /bin/bash`  
        6.  Create the function in the Docker container:  
            `bin/pulsar-admin functions create --jar job-opener-1.0-jar-with-dependencies.jar --classname job.opener.sdk.JobOpener --inputs persistent://public/default/job-opener-input --output persistent://public/default/job-opener-output --logTopic persistent://public/default/job-opener-logs --name JO`  
4.  Run a [CLI Pulsar producer](https://pulsar.apache.org/docs/en/reference-cli-tools/#produce) client to send messages to the function's input topic:  
    1.  Move the `job-open-request-sample.json` to the `/pulsar` folder inside the Docker container. From the IDE's terminal:  
        `docker cp .\job-open-request-sample.json pulsar:/pulsar`  
    2.  Open new BASH for the Pulsar (Docker) container:  
        `docker exec -it pulsar /bin/bash`  
    3.  Produce the JSON file as message:  
        `bin/pulsar-client produce persistent://public/default/job-opener-input --num-produce 1 --files job-open-request-sample.json`  
5.  Check the (BASH) consumers, and your IDE's console output for logs!  

## DEBUG

##### Pulsar Exception: Topic does not have a schema to check
- Check the schema for the `instant-jobs-opener-input` topic:  
	`bin/pulsar-admin schemas get job-opener-input`  
	NOTE: This should respond with HTTP 404 Not Found
- Copy schema-definition-file.json to Docker container:  
    `docker cp .\schema-definition-file.json pulsar:/pulsar`
- [Upload schema](http://pulsar.apache.org/docs/en/schema-manage/#upload-a-schema) to the input topic:  
	`bin/pulsar-admin schemas upload ob-opener-input --filename schema-definition-file.json`

## KNOWN ISSUES
No logs recorded (on the logs topic) in the Docker standalone cluster mode.  
  
Paying no attention as localrun mode is better suited for development, and production cluster will not be Docker standalone cluster.  

## OTHER / HELP

###### REMOVE A TOPIC'S DATA

1.  Make sure:  
	1.  The function is not running.  
	2.  There are no consumers listening to the topic.  
2.  From BASH for the Pulsar (Docker) container:  
	`bin/pulsar-admin persistent delete persistent://public/default/job-opener-input`  
	You can use `--force` to delete the topic if there are active producers and consumers and you have no alternative way to terminate them.
