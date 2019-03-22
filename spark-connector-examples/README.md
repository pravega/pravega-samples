
# Spark Connector Examples for Pravega

Battery of code examples to demonstrate the capabilities of Pravega as a data stream storage
system for Apache Spark.

## Getting Started

### Install Operating System

Install Ubuntu 18.04 LTS. Other operating systems can also be used but the commands below have only been tested
on this version.

### Install Java 8

```
apt-get install openjdk-8-jdk
```

### Install Docker and Docker Compose

See <https://docs.docker.com/install/linux/docker-ce/ubuntu/>
and <https://docs.docker.com/compose/install/>.

### Run Pravega

This will run a development instance of Pravega locally.
Note that the default *standalone* Pravega used for development is likely insufficient for testing video because
it stores all data in memory and quickly runs out of memory.
Using the procedure below, all data will be stored in a small HDFS cluster in Docker.

In the command below, replace x.x.x.x with the IP address of a local network interface such as eth0.

```
cd
git clone https://github.com/pravega/pravega
cd pravega
git checkout r0.4
cd docker/compose
export HOST_IP=x.x.x.x
docker-compose up -d
```

You can view the Pravega logs with `docker-compose logs --follow`.
You can view the stream files stored on HDFS with `docker-compose exec hdfs hdfs dfs -ls -h -R /`.

### Install Apache Spark

This will install a development instance of Spark locally.

Download https://www.apache.org/dyn/closer.lua/spark/spark-2.4.0/spark-2.4.0-bin-hadoop2.7.tgz.

```
mkdir -p ~/spark
cd ~/spark
tar -xzvf ~/Downloads/spark-2.4.0-bin-hadoop2.7.tgz
ln -s spark-2.4.0-bin-hadoop2.7 current
export PATH="$HOME/spark/current/bin:$PATH"
~/spark/current/sbin/start-all.sh
```

Confirm that you can browse to the Spark Master UI at http://localhost:8080/.

### Build and Install the Spark Connector

This will build the Spark Connector and publish it to your local Maven repository.

```
cd
git clone https://github.com/pravega/spark-connectors
cd spark-connectors
./gradlew publishToMavenLocal
ls -lhR ~/.m2/repository/io/pravega/pravega-connectors-spark
```

### Run Examples

Run a Spark Streaming job that writes test data to a Pravega stream.
```
cd ~/pravega-samples/spark-connector-examples
export HOST_IP=x.x.x.x
./write_test_to_pravega.sh
```

Run a Spark Streaming job that reads from a Pravega stream and writes to another Pravega stream.
```
./stream_pravega_to_pravega.sh
```

Run a Spark Streaming job that reads from a Pravega stream writes to the console.
```
./stream_pravega_to_console.sh
```

## (Optional) Configure Anaconda Python

These steps show how to use Anaconda Python to run PySpark applications.
Anaconda Python is configured with Numpy, Pandas, and TensorFlow.

### Install Conda

See https://www.anaconda.com/rpm-and-debian-repositories-for-miniconda/.

### Create Conda Environment

```
source /opt/conda/etc/profile.d/conda.sh
src/main/python/create_conda_env.sh
conda activate pravega-samples
```

Run the following before executing `spark-submit`.
```
export PYSPARK_PYTHON=$HOME/.conda/envs/pravega-samples/bin/python
```
