###############################################################################
#
# Copyright Pravega Authors.
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
#
###############################################################################

###############################################################################
# Build Click Count Job
###############################################################################

FROM maven:3.6-jdk-8-slim AS builder

###############################################################################
# Build SQL Playground Image
###############################################################################

FROM flink:1.13.1-scala_2.12

# Copy sql-client script
COPY bin/* /opt/sql-client/
RUN mkdir -p /opt/sql-client/lib

# Download related libraries
RUN wget -P /opt/sql-client/lib/ https://repo1.maven.org/maven2/org/apache/flink/flink-avro/1.13.1/flink-avro-1.13.1.jar; \
    wget -P /opt/sql-client/lib/ https://repo1.maven.org/maven2/io/pravega/schemaregistry-serializers/0.3.0/schemaregistry-serializers-0.3.0-all.jar; \
    wget -P /opt/sql-client/lib/ https://github.com/pravega/flink-connectors/releases/download/v0.10.1/pravega-connectors-flink-1.13_2.12-0.10.1.jar;

# Copy configuration
COPY conf/* /opt/flink/conf/

WORKDIR /opt/sql-client
ENV SQL_CLIENT_HOME /opt/sql-client

COPY docker-entrypoint.sh /
ENTRYPOINT ["/docker-entrypoint.sh"]