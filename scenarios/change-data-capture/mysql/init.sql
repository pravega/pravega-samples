create database stock;
use stock;

CREATE TABLE stock (
    id VARCHAR(10) NOT NULL PRIMARY KEY,
    value DOUBLE NOT NULL
);

CREATE TABLE metadata (
    id VARCHAR(10) NOT NULL PRIMARY KEY,
    sector VARCHAR(255) NOT NULL
);

INSERT INTO metadata
VALUES ("AAPL", "Technology"),
	("IBM", "Technology"),
	("MU", "Technology"),
	("BA", "Industrials"),
	("TSLA", "Consumer Cyclical"),
	("NKE", "Consumer Cyclical"),
	("GE", "Industrials"),
	("MMM", "Industrials");
