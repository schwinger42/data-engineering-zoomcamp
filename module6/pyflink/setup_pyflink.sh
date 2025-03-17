#!/bin/bash

# Install Python and required packages
apt-get update -y
apt-get install -y python3 python3-pip python3-dev

# Install Python dependencies
pip3 install apache-flink==1.16.0
pip3 install protobuf==4.21.9
pip3 install kafka-python psycopg2-binary

# Install connector libraries
wget -P /opt/flink/lib/ https://repo.maven.apache.org/maven2/org/apache/flink/flink-json/1.16.0/flink-json-1.16.0.jar
wget -P /opt/flink/lib/ https://repo.maven.apache.org/maven2/org/apache/flink/flink-sql-connector-kafka/1.16.0/flink-sql-connector-kafka-1.16.0.jar
wget -P /opt/flink/lib/ https://repo.maven.apache.org/maven2/org/apache/flink/flink-connector-jdbc/1.16.0/flink-connector-jdbc-1.16.0.jar
wget -P /opt/flink/lib/ https://repo1.maven.org/maven2/org/postgresql/postgresql/42.2.24/postgresql-42.2.24.jar

echo "PyFlink setup complete!"