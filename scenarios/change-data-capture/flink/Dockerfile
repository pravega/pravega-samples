FROM flink:1.13.2-scala_2.12

MAINTAINER Pravega

EXPOSE 8081

RUN wget -P /opt/flink/lib/ https://github.com/pravega/flink-connectors/releases/download/v0.10.1/pravega-connectors-flink-1.13_2.12-0.10.1.jar \
  && wget -P /opt/flink/lib/ https://repo.maven.apache.org/maven2/org/apache/flink/flink-connector-jdbc_2.11/1.13.2/flink-connector-jdbc_2.11-1.13.2.jar \
  && wget -P /opt/flink/lib/ https://repo.maven.apache.org/maven2/mysql/mysql-connector-java/8.0.27/mysql-connector-java-8.0.27.jar
COPY flink-conf.yaml /opt/flink/conf/
COPY init.sql /opt/flink/

WORKDIR /opt/flink

ENTRYPOINT ["tail", "-f", "/dev/null"]
