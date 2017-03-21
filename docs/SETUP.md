### Prerequisites

Following software is required:

- [git](https://git-scm.com/book/en/v2/Getting-Started-Installing-Git)

- [Docker](https://docs.docker.com/engine/installation/) 

This configuration assumes Mac OS and `/bin` directory of the application contains pre-compiled binaries for Mac OS.
 
### Installation

#####1. Create a directory for the application, ex.

`mkdir oharlem && cd oharlem`

#####2. Clone the demo repository:

`git clone https://github.com/mpmlj/nsq-demo.git .`


#####3. Launch a database and a message queue:
 
`docker-compose up -d`

**Wait until the cluster is loaded (5sec avg.)**

#####4. Run a consumer daemon (from the same directory):

`./bin/consumer`

#####5. Optional. You may want to launch a message queue monitoring tool in a  browser:

`http://127.0.0.1:4171/topics/events`

Note, it has no auto-refresh, so you may need to refresh it manually.
Also, to experiment with queue interruptions during the ingestino process, you may want to pause/unpause the topic, etc.

#####6. Run a producer in another terminal window (same directory):

`./bin/producer --file=data/data.dump`

Note: for a different dump file use --file argument.


#####7. Consumer working
Notice consumer working and outputting stats on incoming messages in its terminal window.

#####On load success

Consumer's terminal output should end with:
"session COMPLETED
waiting for input..."

Monitoring tool page for the events topic http://127.0.0.1:4171/topics/events
should be showing:
6314 in all "Messages" columns and 0 in all "Depth" columns,
meaning we have processed 6314 messages (record batches) and no messages remains to be processed.

Optional. At this point you may exist the consumer with "Ctrl-C". 

#####8. To access the stats:

######A. Launch an API server:

`./bin/server`

Default port is 9999. To change the port use a --server_port flag, ex.

`./bin/server --server_port=8888`


######B. Open an API server in a browser.

`http://localhost:9999/`

and follow links to respective JSON endpoints with task outputs.
To change the output, use different values in "video_id" and "country" endpoint arguments.  

#####9. Cleanup

Quit the API server by "Ctrl-C".

Run `docker-compose stop` to stop the containers.

Run `docker-compose rm` (and "Y" to confirm) to remove the containers.

* * * 

- [Index](https://github.com/mpmlj/nsq-demo/wiki)
- [Overview](Overview)
- Setup instructions