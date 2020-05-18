# Pravega-FlinkSQL-Es-Demo

Demo: Streaming application with Flink SQL and Pravega

This demo aims to build a streaming analytical application based on Pravega, MySQL, Elasticsearch, Kibana and Flink SQL.

Final Result
![result](https://github.com/AlexanderChiuluvB/Pravega-FlinkSQL-Es-Demo/blob/master/pic/Screenshot%20from%202020-04-19%2000-07-37.png)

## Prerequsites

A computer with Linux or MacOS installed with Docker and Java8

### Use Docker compose to start applications

All the applications will be started as docker container with `docker-compose up -d`

```yaml
version: '2.1'
services:
  pravega:
    image: pravega/pravega
    ports:
        - "9090:9090"
        - "12345:12345"
    command: "standalone"
    network_mode: host
  datagen:
    image: brianzhou/pravega-demo-datagen:0.1
    command: "java -classpath /opt/datagen/datagen.jar sender --input /opt/datagen/user_behavior.log --speedup 1000"
    depends_on:
      - pravega
    network_mode: host
  elasticsearch:
    image: docker.elastic.co/elasticsearch/elasticsearch:7.6.0
    environment:
      - cluster.name=docker-cluster
      - bootstrap.memory_lock=true
      - "ES_JAVA_OPTS=-Xms512m -Xmx512m"
      - discovery.type=single-node
    ports:
      - "9200:9200"
      - "9300:9300"
    ulimits:
      memlock:
        soft: -1
        hard: -1
      nofile:
        soft: 65536
        hard: 65536
  kibana:
    image: docker.elastic.co/kibana/kibana:7.6.0
    ports:
      - "5601:5601"
  mysql:
    image: brianzhou/pravega-demo-mysql:0.1
    ports:
      - "3306:3306"
    environment:
      - MYSQL_ROOT_PASSWORD=123456
    network_mode: host

```

The Docker compose include below containers:

* Datagen:

Data generator.When started, it will send data to Pravega cluster automatically. 
By default it will send 1000 data per second, lasting for about 3 hours. You can also
modify the `speedup` parameter in `docker-compose.yml` to adjust the data generatoring speed.

* Pravega:

Used as streaming data storage. Data generatored by Datagen container will be sent to Pravega.
And the data stored in Pravega will be sent to Flink using Pravega-Flink connector.(https://github.com/pravega/flink-connectors)

* MySQL:

MySQL5.7 with pre-defined `category` table

* Elasticsearch:

Storage of Flink SQL result data.

* Kibana:

Visualize Elasticsearch index data for OLAP.

Use the following command to start all the containers.
`docker-compose up -d`

You can use `docker ps` to check whether all the containers are working or visit ` http://localhost:5601/ ` to check
whether kibana is working.

Finally, you can use the following command to stop all the containers.
`docker-compose down`


### Download and install Flink 1.10

1.Download Flink 1.10.0 ：https://www.apache.org/dist/flink/flink-1.10.0/flink-1.10.0-bin-scala_2.12.tgz

2.unzip and `cd flink-1.10.0-bin-scala_2.12`

3.Download the following jars and copy them to lib/
```

wget -P ./lib/ https://repo1.maven.org/maven2/org/apache/flink/flink-json/1.10.0/flink-json-1.10.0.jar | \
wget -P ./lib/ https://repo1.maven.org/maven2/org/apache/flink/flink-sql-connector-elasticsearch6_2.12/1.10.0/flink-sql-connector-elasticsearch6_2.12-1.10.0.jar | \
wget -P ./lib/ https://repo1.maven.org/maven2/org/apache/flink/flink-jdbc_2.12/1.10.0/flink-jdbc_2.12-1.10.0.jar | \
wget -P ./lib/ https://repo1.maven.org/maven2/mysql/mysql-connector-java/5.1.48/mysql-connector-java-5.1.48.jar
wget -P ./lib/ http://oss.jfrog.org/jfrog-dependencies/io/pravega/pravega-connectors-flink-1.10_2.12/0.8.0-53.b9ececb-SNAPSHOT/pravega-connectors-flink-1.10_2.12-0.8.0-53.b9ececb-20200407.114946-1.jar

```
4.Modify ` taskmanager.numberOfTaskSlots` as 10 in `conf/flink-conf.yaml` since we will launch multiple tasks in Flink.

5.`./bin/start-cluster.sh` start the Flink cluster.
If succeed, you can visited Flink Web UI in http://localhost:8081.

6.`bin/sql-client.sh embedded ` to start the FlinkSQL client.


### Create Pravega Table with DDL

The datagen container will send data to `testSteam` stream in `examples` scope of Pravega.

(You can compare the `stream` concept in Pravega with the `topic` conecpt in Kafka, and scope is namespace in Pravega. For more
information please refer [Pravega-conecpt](http://pravega.io/docs/latest/pravega-concepts/))


```sql
CREATE TABLE user_behavior (
   user_id BIGINT,
    item_id BIGINT,
    category_id BIGINT,
    behavior STRING,
    ts TIMESTAMP(3),
    proctime as PROCTIME(),
    WATERMARK FOR ts as ts - INTERVAL '5' SECOND
) WITH (
  'update-mode' = 'append',
  'connector.type' = 'pravega',
  'connector.version' = '1',
  'connector.metrics' = 'true',
  'connector.connection-config.controller-uri' = 'tcp://localhost:9090',
  'connector.connection-config.default-scope' = 'examples',
  'connector.reader.stream-info.0.stream' = 'testStream',
  'connector.writer.stream' = 'testStream',
  'connector.writer.mode' = 'atleast_once',
  'connector.writer.txn-lease-renewal-interval' = '10000',
  'format.type' = 'json',
  'format.fail-on-missing-field' = 'false'
)
```

We defined 5 fields in the schema. Besides, we use `PROCTIME()` built-in function to create a processing field `proctime`.
We also use WATERMARK grammar to declare watermark policy on `ts` field. (Out-of-order data within 5 seconds will be tolerated.) For more information about time attrubite and DDL grammar Please refer 

* Time attribtue:
https://ci.apache.org/projects/flink/flink-docs-release-1.10/dev/table/streaming/time_attributes.html

* DDL:
https://ci.apache.org/projects/flink/flink-docs-release-1.10/dev/table/sql/create.html#create-table


Use `show tables;` and `describe user_behavior;` to check whether the table is created successfully.

Then we can run `SELECT * FROM user_behavior;` in sql-client to take a glimpse of the data.

![SAMPLE1](https://github.com/AlexanderChiuluvB/Pravega-FlinkSQL-Es-Demo/blob/master/pic/Screenshot%20from%202020-04-21%2016-44-09.png)


## Count the transaction number per hour

### Use DDL to create Elasticsearch table.

We first create a ES sink table to save the `hour_of_day` and `buy_cnt`.

```sql
CREATE TABLE buy_cnt_per_hour ( 
    hour_of_day BIGINT,
    buy_cnt BIGINT
) WITH (
    'connector.type' = 'elasticsearch', -- use elasticsearch connector
    'connector.version' = '6',  -- elasticsearch verion，6 can support es 6+ and 7+ version
    'connector.hosts' = 'http://localhost:9200',  -- elasticsearch address
    'connector.index' = 'buy_cnt_per_hour',  -- elasticsearch index name 
    'connector.document-type' = 'user_behavior', -- elasticsearch type(deprecated in es7+)
    'connector.bulk-flush.max-actions' = '1',  -- refresh with each data
    'format.type' = 'json',  -- data output format json
    'update-mode' = 'append'
);
```

We don't have to create `buy_cnt_per_hour` index beforehand, Flink job will automatically create the index in es.

**Attention**

Please do not use `SELECT * FROM buy_cnt_per_hour`. Doing so means that you regard the es table as source table but not
sink table. And es table only supports as sink table.


### Submit the Query

We need to use `TUMBLE` window function to group the time as windows(For more information about group window function please refer https://ci.apache.org/projects/flink/flink-docs-release-1.10/dev/table/sql/queries.html#group-windows ). Then we count the number of 'buy' behavior within each window and insert into the es sink table `buy_cnt_per_hour`

```sql
INSERT INTO buy_cnt_per_hour
SELECT HOUR(TUMBLE_START(ts, INTERVAL '1' HOUR)), COUNT(*)
FROM user_behavior
WHERE behavior = 'buy'
GROUP BY TUMBLE(ts, INTERVAL '1' HOUR);
```

When the query is submitted, you can see a new Flink job is submitted in Flink Web UI.

![FLINK WEB UI](https://github.com/AlexanderChiuluvB/Pravega-FlinkSQL-Es-Demo/blob/master/pic/Screenshot%20from%202020-04-21%2017-09-25.png)


### Kibana for visualization

1.Create Index Pattern in kibana with the `buy_cnt_per_hour` es index.

![index pattern](https://github.com/AlexanderChiuluvB/Pravega-FlinkSQL-Es-Demo/blob/master/pic/Screenshot%20from%202020-04-21%2017-18-54.png)

2.Create `Area` type Dashboard called `User behavior log analysis` with the `buy_cnt_per_hour` index.
The specific configuration of dashboard can be seen in the following screenshot.

![visresult1](https://github.com/AlexanderChiuluvB/Pravega-FlinkSQL-Es-Demo/blob/master/pic/Screenshot%20from%202020-04-21%2017-22-44.png)


## Count cumulative user numbers per 10 mins

First we create a es sink table called cumulative_uv

```sql
CREATE TABLE cumulative_uv (
    time_str STRING,
    uv BIGINT
) WITH (
    'connector.type' = 'elasticsearch',
    'connector.version' = '6',
    'connector.hosts' = 'http://localhost:9200',
    'connector.index' = 'cumulative_uv',
    'connector.document-type' = 'user_behavior',
    'format.type' = 'json',
    'update-mode' = 'upsert'
);
```

Here we use `CREATE VIEW` to register a logical view.

Then we use `SUBSTR`,`DATE_FORMAT` and `||` built-in function to transform the TIMESTAMP field
to a 10-minute unit time string like `12:10`, `12:20`. More information about OVER WINDOW please refer
to https://ci.apache.org/projects/flink/flink-docs-release-1.10/dev/table/sql/queries.html#aggregations

```sql
CREATE VIEW uv_per_10min AS
SELECT 
  MAX(SUBSTR(DATE_FORMAT(ts, 'HH:mm'),1,4) || '0') OVER w AS time_str, 
  COUNT(DISTINCT user_id) OVER w AS uv
FROM user_behavior
WINDOW w AS (ORDER BY proctime ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW);
```

Finally we submit the query.

```sql
INSERT INTO cumulative_uv
SELECT time_str, MAX(uv)
FROM uv_per_10min
GROUP BY time_str;
```

### Kibana for visualization

1.Create Index Pattern in kibana with the `cumulative_uv` es index.

2.Create `Line` type Dashboard called `independent_user_analysis` with the `cumulative_uv` index.
The specific configuration of dashboard can be seen in the following screenshot.

![uv_result](https://github.com/AlexanderChiuluvB/Pravega-FlinkSQL-Es-Demo/blob/master/pic/Screenshot%20from%202020-04-22%2011-26-23.png)

## Category billboard

The mysql container have already created a dimension table with the subcategory and parent category mapping data.

First we create a MySQL table in SQL CLI for dimension table query.

```sql
CREATE TABLE category_dim (
    sub_category_id BIGINT,  
    parent_category_id BIGINT 
) WITH (
    'connector.type' = 'jdbc',
    'connector.url' = 'jdbc:mysql://localhost:3306/flink',
    'connector.table' = 'category',
    'connector.driver' = 'com.mysql.jdbc.Driver',
    'connector.username' = 'root',
    'connector.password' = '123456',
    'connector.lookup.cache.max-rows' = '5000',
    'connector.lookup.cache.ttl' = '10min'
);
```

Then we create an es sink table.

```sql
CREATE TABLE top_category (
    category_name STRING,  
    buy_cnt BIGINT  
) WITH (
    'connector.type' = 'elasticsearch',
    'connector.version' = '6',
    'connector.hosts' = 'http://localhost:9200',
    'connector.index' = 'top_category',
    'connector.document-type' = 'user_behavior',
    'format.type' = 'json',
    'update-mode' = 'upsert'
);

```

Next we create a view called rich_user_behavior and use TEMPORAL JOIN grammar to associate
the dimension table. For more information about TEMPORAL JOIN grammar please refer to https://ci.apache.org/projects/flink/flink-docs-release-1.10/dev/table/streaming/joins.html#join-with-a-temporal-table.

```sql
CREATE VIEW rich_user_behavior AS
SELECT U.user_id, U.item_id, U.behavior, 
  CASE C.parent_category_id
    WHEN 1 THEN 'shoes'
    WHEN 2 THEN 'clothing'
    WHEN 3 THEN 'electrical appliances'
    WHEN 4 THEN 'beauty makeup'
    WHEN 5 THEN 'maternal and child product'
    WHEN 6 THEN '3C digital'
    WHEN 7 THEN 'outdoor gears'
    WHEN 8 THEN 'food'
    ELSE 'others'
  END AS category_name
FROM user_behavior AS U LEFT JOIN category_dim FOR SYSTEM_TIME AS OF U.proctime AS C
ON U.category_id = C.sub_category_id;
```

Finally we can group the data with category name and insert the result into sink table.

```sql
INSERT INTO top_category
SELECT category_name, COUNT(*) buy_cnt
FROM rich_user_behavior
WHERE behavior = 'buy'
GROUP BY category_name;
```

### Kibana for visualization

1.Create Index Pattern in kibana with the `top_category` es index.

2.Create `Horizontal Bar` type Dashboard called `top_category_analysis` with the `top_category` index.
The specific configuration of dashboard can be seen in the following screenshot.

![category_result](https://github.com/AlexanderChiuluvB/Pravega-FlinkSQL-Es-Demo/blob/master/pic/Screenshot%20from%202020-04-22%2012-16-31.png)


