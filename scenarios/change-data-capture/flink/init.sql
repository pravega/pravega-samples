CREATE TABLE stock (
    id VARCHAR(10) NOT NULL PRIMARY KEY,
    `value` DOUBLE NOT NULL
) WITH (
    'connector' = 'pravega',  -- indicate to source to be Pravega
    'controller-uri' = 'tcp://pravega:9090',  -- pravega container address
    'scope' = 'stock',  -- scope where the changelog is written to
    'scan.execution.type' = 'streaming',  -- continuous data
    'scan.streams' = 'dbserver1.stock.stock',  -- changelog stream for the stock table
    'format' = 'debezium-json'  -- in debezium format
);

CREATE TABLE metadata (
    id VARCHAR(10) NOT NULL PRIMARY KEY,
    sector STRING NOT NULL
) WITH (
    'connector' = 'jdbc',
    'url' = 'jdbc:mysql://mysql:3306/stock',
    'table-name' = 'metadata',
    'username' = 'root',
    'password' = 'dbz',
    'lookup.cache.max-rows' = '50',
    'lookup.cache.ttl' = '60min'
);

CREATE TABLE `index` (
    sector STRING NOT NULL,
    `index` DOUBLE NOT NULL
) WITH (
    'connector' = 'print'
);
