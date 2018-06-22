# High Count Alert #

The application reads apache access logs from a Pravega stream and once every 2 seconds
counts the number of 500 responses in the last 30 seconds, and generates
alert when the counts of 500 responses exceed 6.

## Prerequistes ##

A Docker image containing Pravega and Logstash had been prepared to simplify the demo. Skip ahead to the **Run in Docker Container** section in this document if you have docker environment handy.

Otherwise proceed to set up Logstash and Pravega

1. Logstash installed, see [Install logstash](https://www.elastic.co/guide/en/logstash/5.6/installing-logstash.html).
2. Pravega running, see [here](http://pravega.io/docs/latest/getting-started/) for instructions.

## Start Logstash with Pravega Output Plugin ##

On the Logstash host, download the plugin gem file from [Logstash Pravega output plugin](https://github.com/pravega/logstash-output-pravega/releases), for example, `logstash-output-pravega-0.3.0.gem`.

Install the plugin, assuming Logstash is installed at `/usr/share/logstash/`
```
$ /usr/share/logstash/bin/logstash-plugin install logstash-output-pravega-0.3.0.gem
```

Copy the contents under flink-examples/doc/flink-high-error-count-alert/filters/ to the Logstash host, e.g., in directory ~/pravega.
update **pravega_endpoint** in ~/pravega/90-pravega-output.conf

```
output {
    pravega {
        pravega_endpoint => "tcp://127.0.0.1:9090"   <- update to point to your Pravega controller
        stream_name => "apacheaccess"
        scope => "examples"
    }
}
```

Start logstash, assuming it is installed at /usr/share/logstash/bin.
Note that sometimes it may take a minute or two for logstash to start. For troubleshooting, the logstash log files are 
normally at /var/log/logstash. To restart, type Ctrl-C, then re-run the command.

```
$ sudo /usr/share/logstash/bin -f ~/pravega
Sending Logstash's logs to /var/log/logstash which is now configured via log4j2.properties
```

Normally Logstash is configured to receive data from remote log shippers, such as filebeat. For simplicity in this demo
Logstash is configured read data from /tmp/access.log.

## Run in Docker Container ##

Create a file at /tmp/access.log
```
$ touch /tmp/access.log
```

Run script below to start container from prebuilt image. Adjust parameters to your need.
```
#!/bin/sh
set -u

PRAVEGA_SCOPE=${PRAVEGA_SCOPE:-examples}
PRAVEGA_STREAM=${PRAVEGA_STREAM:-apacheaccess}
CONTAINER_NAME=pravega
IMAGE_NAME=emccorp/pravega-demo

docker run -d --name $CONTAINER_NAME \
    -p 9090:9090 \
    -p 9091:9091 \
    -p 9600:9600 \
    -v /tmp/access.log:/opt/data/access.log \
    -v $PWD/logs/:/var/log/pravega/ \
    -e PRAVEGA_SCOPE=${PRAVEGA_SCOPE} \
    -e PRAVEGA_STREAM=${PRAVEGA_STREAM} \
    ${IMAGE_NAME} 
```

To check Pravega
```
$ curl localhost:9091/v1/scopes
```

To check Logstash and output plugin for Pravega via Logstash monitoring API running on port 9600. Logstash may take one or two minutes to start as a delay is introduced to wait for Pravega to start.
```
# The output should contain the name and the version (your version may differ) of the plugin  
#    "name" : "logstash-output-pravega",
#    "version" : "0.3.0"

$ curl localhost:9600/_node/plugins?pretty
```

The log files for Pravega and Logstash should be in the **logs** directory under current directory if you the script above to start the container.

More details can be found on github [pravega docker](https://github.com/pravega/logstash-output-pravega) and on dockerhub [pravega docker image](https://hub.docker.com/r/emccorp/pravega-demo/) 

## Run HighCountAlerter ##

Run the alerter. Adjust the controller and scope/stream if necessary.
```
$ cd flink-connector-examples/build/install/pravega-flink-examples
$ bin/highCountAlerter [--controller tcp://127.0.0.1:9090] [--scope examples] [--stream apacheaccess]
```

## Input Data ##

Add access logs to /tmp/access.log, e.g., by running command below every one or two seconds.
```
echo '10.1.1.11 - peter [19/Mar/2018:02:24:01 -0400] "PUT /mapping/ HTTP/1.1" 500 182 "http://example.com/myapp" "python-client"' >> /tmp/access.log
```

Logstash will push the data to Pravega in json string, e.g.,
```
{
        "request" => "/mapping/",
          "agent" => "\"python-client\"",
           "auth" => "peter",
          "ident" => "-",
           "verb" => "PUT",
        "message" => "10.1.1.11 - peter [19/Mar/2018:02:24:01 -0400] \"PUT /mapping/ HTTP/1.1\" 500 182 \"http://example.com/myapp\" \"python-client\"",
       "referrer" => "\"http://example.com/myapp\"",
     "@timestamp" => 2018-03-19T06:24:01.000Z,
       "response" => "500",
          "bytes" => "182",
       "clientip" => "10.1.1.11",
       "@version" => "1",
           "host" => "lglca061.lss.emc.com",
    "httpversion" => "1.1"
}
```

## View Alert ##
In the HighCountAlerter window, you should see output like the following. Once the 500 response counts reach 6 or above, it
should print **High 500 responses** alerts. 
```
3> Response count: 500 : 1
3> Response count: 500 : 2
3> Response count: 500 : 4
3> Response count: 500 : 6
2> High 500 responses: 500 : 6
3> Response count: 500 : 8
3> High 500 responses: 500 : 8
3> Response count: 500 : 8
2> High 500 responses: 500 : 8
3> Response count: 500 : 7
3> High 500 responses: 500 : 7
3> Response count: 500 : 5
3> Response count: 500 : 3
3> Response count: 500 : 1
```
