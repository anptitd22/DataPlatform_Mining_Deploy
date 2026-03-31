FROM apache/spark:3.5.3-java17

USER root

RUN apt-get update && \
    apt-get install -y python3.11 python3.11-dev python3-pip && \
    update-alternatives --install /usr/bin/python3 python3 /usr/bin/python3.11 1 && \
    python3 -m pip install --upgrade pip

ENV PYSPARK_PYTHON=/usr/bin/python3
ENV PYSPARK_DRIVER_PYTHON=/usr/bin/python3

ENV ICEBERG_VERSION=1.6.1
ENV HADOOP_VERSION=3.3.4
ENV AWS_SDK_VERSION=1.12.367

# Iceberg Spark Runtime
ADD https://repo1.maven.org/maven2/org/apache/iceberg/iceberg-spark-runtime-3.5_2.12/${ICEBERG_VERSION}/iceberg-spark-runtime-3.5_2.12-${ICEBERG_VERSION}.jar /opt/spark/jars/

# Iceberg Hive Runtime
ADD https://repo1.maven.org/maven2/org/apache/iceberg/iceberg-hive-runtime/${ICEBERG_VERSION}/iceberg-hive-runtime-${ICEBERG_VERSION}.jar /opt/spark/jars/

# AWS & Hadoop S3
ADD https://repo1.maven.org/maven2/org/apache/hadoop/hadoop-aws/${HADOOP_VERSION}/hadoop-aws-${HADOOP_VERSION}.jar /opt/spark/jars/
ADD https://repo1.maven.org/maven2/com/amazonaws/aws-java-sdk-bundle/${AWS_SDK_VERSION}/aws-java-sdk-bundle-${AWS_SDK_VERSION}.jar /opt/spark/jars/

ADD https://repo1.maven.org/maven2/org/apache/iceberg/iceberg-aws-bundle/${ICEBERG_VERSION}/iceberg-aws-bundle-${ICEBERG_VERSION}.jar /opt/spark/jars/

RUN chmod 644 /opt/spark/jars/*.jar

RUN mkdir -p /home/spark/.cache && \
    chmod -R 777 /home/spark && \
    mkdir -p /opt/spark/spark-events && \
    chmod 777 /opt/spark/spark-events

ENV HF_HOME=/home/spark/.cache
ENV XDG_CACHE_HOME=/home/spark/.cache

RUN pip install torch sentence-transformers && \
    pip install Pillow pymilvus

USER spark

WORKDIR /opt/spark

# FROM apache/spark:3.5.3-java17

# USER root

# RUN apt-get update && \
#     apt-get install -y python3.11 python3.11-dev python3-pip && \
#     update-alternatives --install /usr/bin/python3 python3 /usr/bin/python3.11 1 && \
#     python3 -m pip install --upgrade pip

# ENV PYSPARK_PYTHON=/usr/bin/python3
# ENV PYSPARK_DRIVER_PYTHON=/usr/bin/python3

# ENV ICEBERG_VERSION=1.10.1
# ENV HADOOP_VERSION=3.4.0
# ENV SCALA_VERSION=2.12
# ENV SPARK_VERSION=3.5

# ADD https://repo1.maven.org/maven2/org/apache/iceberg/iceberg-spark-runtime-${SPARK_VERSION}_${SCALA_VERSION}/${ICEBERG_VERSION}/iceberg-spark-runtime-${SPARK_VERSION}_${SCALA_VERSION}-${ICEBERG_VERSION}.jar /opt/spark/jars/
# ADD https://repo1.maven.org/maven2/org/apache/hadoop/hadoop-aws/${HADOOP_VERSION}/hadoop-aws-${HADOOP_VERSION}.jar /opt/spark/jars/

# RUN chmod 644 /opt/spark/jars/*.jar

# RUN mkdir -p /opt/spark/spark-events && chmod 777 /opt/spark/spark-events

# USER spark

# WORKDIR /opt/spark