FROM flink:1.13.2-scala_2.12

MAINTAINER Pravega

EXPOSE 8081

RUN wget -P /opt/flink/lib/ https://github.com/pravega/flink-connectors/releases/download/v0.10.1/pravega-connectors-flink-1.13_2.12-0.10.1.jar
COPY flink-conf.yaml /opt/flink/conf/
COPY init.sql /opt/flink/

WORKDIR /opt/flink

ENTRYPOINT ["tail", "-f", "/dev/null"]
