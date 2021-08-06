# Flink Pravega Table API samples for Python

This module contains examples to demonstrate the use of Flink connector Table API and it uses [NY Taxi Records](http://www.nyc.gov/html/tlc/html/about/trip_record_data.shtml) to demonstrate the usage.
```
The following files are downloaded and used for this sample application
https://s3.amazonaws.com/nyc-tlc/trip+data/yellow_tripdata_2018-01.csv
https://s3.amazonaws.com/nyc-tlc/misc/taxi+_zone_lookup.csv
```

## Pre-requisites
1. Pravega running (see [here](http://pravega.io/docs/latest/getting-started/) for instructions)(https://github.com/pravega/pravega-samples#pravega-samples-build-instructions)
2. Python and its packages, see [pyflink setup](https://ci.apache.org/projects/flink/flink-docs-stable/docs/dev/python/installation/)
3. Install Pravega Python client via `pip install pravega`
3. Get the latest Pravega Flink connectors at [the release page](https://github.com/pravega/flink-connectors/releases)
4. Apache Flink running

> If you wish to use Anaconda instead, replace `pip` with `conda` in the steps above.

## Running the samples

Navigate to the python source code. There is no need to touch the entire Java and Gradle things.
> cd $SAMPLES-HOME/scenarios/pravega-flink-connector-sql-samples/src/main/python

Usage:

```
flink run --python ./bin/<popular_destination_query.py|popular_taxi_vendor.py|max_travellers_per_destination.py> --pyFiles common.py

Additional optional parameters: --scope <scope-name> --stream <stream-name> --controller-uri <controller-uri>
```

1. Load taxi data to Pravega

    The default option assumes that the Pravega is running locally. You can override it by passing the controller URI options. It should be `ip:port` like `localhost:9090` without `tcp://` at the beginning.

    > python3 prepare_main.py

    The above command loads the taxi dataset records to Pravega and prepares the environment for testing.

2. Popular Destination

    > flink run --python ./popular_destination_query.py --pyFiles ./common.py --jarfile <path/to/pravega-flink-connector>

    The above command uses SQL to find the most popular destination (drop-off location) from the available trip records.

3. Popular Taxi Vendors

    > flink run --python ./popular_taxi_vendor.py --pyFiles ./common.py --jarfile <path/to/pravega-flink-connector>

    The above command uses Table API to find the most popular taxi vendor that was used by the commuters.

4. Maximum Travellers/Destination

    > flink run --python ./max_travellers_per_destination.py --pyFiles ./common.py --jarfile <path/to/pravega-flink-connector>

    The above command uses Table API to group the maximum number of travellers with respect to the destination/drop-off location. 
