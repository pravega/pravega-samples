# Flink Connector Examples for Pravega
Battery of code examples to demonstrate the capabilities of Pravega as a data stream storage 
system for Apache Flink. 

## Pre-requisites
1. Pravega running (see [here](http://pravega.io/docs/latest/getting-started/) for instructions)
2. Build [flink-connectors](https://github.com/pravega/flink-connectors) repository
3. Build [pravega-samples](https://github.com/pravega/pravega-samples) repository
4. Apache Flink running


### Distributing Flink Samples
#### Assemble
Use gradle to assemble a distribution folder containing the Flink programs as a ready-to-deploy 
uber-jar called `pravega-flink-examples-0.1.0-SNAPSHOT-all.jar`:

```
$ ./gradlew installDist
...
$ ls -R flink-connector-examples/build/install/pravega-flink-examples
bin	lib

flink-connector-examples/build/install/pravega-flink-examples/bin:
run-example

flink-connector-examples/build/install/pravega-flink-examples/lib:
pravega-flink-examples-0.1.0-SNAPSHOT-all.jar
```

#### Upload
The `upload` task makes it easy to upload the sample binaries to your cluster. First, configure 
Gradle with the address of a node in your cluster.   Edit `~/.gradle/gradle.properties` to 
specify a value for `dcosAddress`.

```
$ cat ~/.gradle/gradle.properties
dcosAddress=10.240.124.164
```

Then, upload the samples to the cluster. They will be copied to `/home/centos` on the target node.
```
$ ./gradlew upload
```

---

# Examples Catalog

## Word Count

This example consists of two applications, a `WordCountWriter` that reads data from a 
network stream, transforms the data, and writes the data to a Pravega stream; and a
`WordCountReader` that reads from a Pravega stream and prints the summary of word count.

The application reads text from a socket, once every 5 seconds prints the distinct words and 
counts from the previous 5 seconds, and writes the word counts to a Pravega stream and prints 
word counts.

### Execution

The execution scripts can be found under the `flink-connector-examples` directory in:

```
flink-connector-examples/build/install/pravega-flink-examples/bin
```
You might want to run `WordCountWriter` in one window and `WordCountReader` in another.

First, use netcat to start local server via
```
$ nc -lk 9999
```

Then start the WordCountWriter
```
$ bin/wordCountWriter [-host localhost] [-port 9999] [-stream myscope/wordcount] [-collector tcp://localhost:9090]
```

All args are optional, if not specified, the defaults are:

 * host - "localhost"
 * port - "9999"
 * stream - "myscope/wordcount"
 * controller - "tcp://localhost:9090"

Now, start the `WordCountReader` application that reads and prints data from a Pravega stream.
```
$ bin/wordCountReader [-stream myscope/wordcount] [-collector tcp://localhost:9090]
```
All args are optional, if not included, the defaults are:
 * stream - "myscop/wordcount"
 * controller - "tcp://localhost:9090"

Now in the windows where netcat is running, enter some text, for example,
```
$ nc -lk 9999
aa bb cc aa
```

In the windows where wordCountReader is running, it should show output similar to the sample output below
```
4> Word: cc:  Count: 1
4> Word: aa:  Count: 2
4> Word: bb:  Count: 1
```
 

Suppose Flink is installed at `/usr/share/flink`. Before starting Flink, you will need to 
edit `/usr/share/flink/conf/flink-conf.yaml` to increase the number of task slots, for example, 4.
```
taskmanager.numberOfTaskSlots: 4
```

To proceed with the example, Flink should be running at this point.
To check this, point your browser to http://<your_flink_host>:8081 to make sure Flink is running; 
then click "Running Jobs". By default, Flink job manager runs on port 6123. 

Now, let's start `WordCountWriter` with the following commands:
```
$ cd flink-connector-examples/build/install/pravega-flink-examples
$ flink run -m localhost:6123 -c io.pravega.examples.flink.wordcount.WordCountWriter lib/pravega-flink-examples-0.2.0-SNAPSHOT-all.jar --host localhost --port 9999 --controller tcp://localhost:9090
```
The `WordCountWriter` job should show up on the Flink UI as a running job.

In a different window, we start `WordCountReader`:
```
$ cd flink-connector-examples/build/install/pravega-flink-examples
$ flink run -m localhost:6123 -c io.pravega.examples.flink.wordcount.WordCountReader lib/pravega-flink-examples-0.2.0-SNAPSHOT-all.jar --controller tcp://localhost:9090
```
The `WordCountReader` job should show up on the Flink UI as a running job.

### Output
Now in the windows where `netcat` is running, enter some text, for example,
```
$ nc -lk 9999
aa bb cc aa
```

Output similar to the sample below should show up in a flink taskmanager output file, e.g., 
`flink-ubuntu-taskmanager-0-myhostname.out`, in the Flink log directory, e.g., `/usr/share/flink/log`
```
Word: aa:  Count: 2
Word: cc:  Count: 1
Word: bb:  Count: 1
```
