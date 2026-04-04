# Create a separate download stage to get the PostgreSQL connector and AWS connector
FROM alpine:latest AS downloader
ARG POSTGRESQL_CONNECTOR_VERSION=42.7.4
RUN apk add --no-cache curl
WORKDIR /downloads
RUN curl -L -o postgresql-${POSTGRESQL_CONNECTOR_VERSION}.jar \
    https://repo1.maven.org/maven2/org/postgresql/postgresql/${POSTGRESQL_CONNECTOR_VERSION}/postgresql-${POSTGRESQL_CONNECTOR_VERSION}.jar

# Back to main image
FROM apache/hive:4.0.0
ARG POSTGRESQL_CONNECTOR_VERSION=42.7.4

# Copy the PostgreSQL connector from the downloader stage
COPY --from=downloader /downloads/postgresql-${POSTGRESQL_CONNECTOR_VERSION}.jar /opt/hive/lib/
ENV HADOOP_CLASSPATH=${HADOOP_CLASSPATH}:/opt/hadoop/share/hadoop/tools/lib/hadoop-aws-3.3.6.jar
ENV HADOOP_CLASSPATH=${HADOOP_CLASSPATH}:/opt/hadoop/share/hadoop/tools/lib/aws-java-sdk-bundle-1.12.367.jar

COPY metastore-site.xml /opt/hive/conf/

# Set the entrypoint
ENTRYPOINT ["/entrypoint.sh"]