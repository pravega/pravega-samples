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
3. Install pravega python client via `pip install pravega`
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

    The default option assumes that Pravega is running locally. You can override it by passing the controller URI options.

    > python3 prepare_data.py

    This should create the table data at `/tmp/table_data.csv`.

    > tail /tmp/table_data.csv

    > "430781","1","2018-01-31 23:05:11","2018-01-31 23:13:53","1","2.60","137","148","Manhattan","Kips Bay","Yellow Zone","Manhattan","Lower East Side","Yellow Zone"
    > "430782","1","2018-01-31 23:41:11","2018-01-31 23:45:22","1",".30","163","230","Manhattan","Midtown North","Yellow Zone","Manhattan","Times Sq/Theatre District","Yellow Zone"
    > "430783","1","2018-01-31 23:52:53","2018-01-31 23:57:32","1","1.20","43","162","Manhattan","Central Park","Yellow Zone","Manhattan","Midtown East","Yellow Zone"
    > "430784","2","2018-01-31 23:20:51","2018-01-31 23:30:51","1","1.65","234","158","Manhattan","Union Sq","Yellow Zone","Manhattan","Meatpacking/West Village West","Yellow Zone"
    > "430785","2","2018-01-31 23:57:33","2018-02-01 00:07:44","1","2.92","230","238","Manhattan","Times Sq/Theatre District","Yellow Zone","Manhattan","Upper West Side North","Yellow Zone"
    > "430786","1","2018-01-31 23:21:35","2018-01-31 23:34:20","2","2.80","158","163","Manhattan","Meatpacking/West Village West","Yellow Zone","Manhattan","Midtown North","Yellow Zone"
    > "430787","1","2018-01-31 23:35:51","2018-01-31 23:38:57","1",".60","163","162","Manhattan","Midtown North","Yellow Zone","Manhattan","Midtown East","Yellow Zone"
    > "430788","2","2018-01-31 23:28:00","2018-01-31 23:37:09","1","2.95","74","69","Manhattan","East Harlem North","Boro Zone","Bronx","East Concourse/Concourse Village","Boro Zone"
    > "430789","2","2018-01-31 23:24:40","2018-01-31 23:25:28","1",".00","7","193","Queens","Astoria","Boro Zone","Queens","Queensbridge/Ravenswood","Boro Zone"
    > "430790","2","2018-01-31 23:28:16","2018-01-31 23:28:38","1",".00","7","193","Queens","Astoria","Boro Zone","Queens","Queensbridge/Ravenswood","Boro Zone"

    Now it is time to import the data from csv to the pravega.

    > flink run --python ./prepare_main.py --pyFiles ./common.py --jarfile <path/to/pravega-flink-connector>

2. Popular Destination

    > flink run --python ./popular_destination_query.py --pyFiles ./common.py --jarfile <path/to/pravega-flink-connector>

    The above command uses SQL to find the most popular destination (drop-off location) from the available trip records.

3. Popular Taxi Vendors

    > flink run --python ./popular_taxi_vendor.py --pyFiles ./common.py --jarfile <path/to/pravega-flink-connector>

    The above command uses Table API to find the most popular taxi vendor that was used by the commuters.

4. Maximum Travellers/Destination

    > flink run --python ./max_travellers_per_destination.py --pyFiles ./common.py --jarfile <path/to/pravega-flink-connector>

    The above command uses Table API to group the maximum number of travellers with respect to the destination/drop-off location. 
