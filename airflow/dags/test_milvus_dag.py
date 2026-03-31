from airflow import DAG
from airflow.providers.apache.spark.operators.spark_submit import SparkSubmitOperator
from datetime import datetime

with DAG(
    "milvus_test_paper_pipeline",
    start_date=datetime(2026, 3, 31),
    schedule=None,
    catchup=False,
) as dag:
    test_milvus = SparkSubmitOperator(
        task_id="test_milvus_insert",
        conn_id="spark_default",
        packages="org.apache.iceberg:iceberg-spark-runtime-3.5_2.12:1.6.1,org.apache.hadoop:hadoop-aws:3.3.4,com.amazonaws:aws-java-sdk-bundle:1.12.367",
        application="/opt/airflow/src/test_milvus_logic.py",
        conf={
            "spark.sql.catalog.metastore.io-impl": "org.apache.iceberg.hadoop.HadoopFileIO",
            "spark.sql.catalog.metastore": "org.apache.iceberg.spark.SparkCatalog",
            "spark.sql.catalog.metastore.catalog-impl": "org.apache.iceberg.hive.HiveCatalog",
            "spark.sql.catalog.metastore.uri": "thrift://metastore:9083",
            "spark.sql.catalog.metastore.warehouse": "s3a://lakehouse/",
            "spark.hadoop.fs.s3a.endpoint": "http://minio-nginx:9000",
            "spark.hadoop.fs.s3a.access.key": "admin",
            "spark.hadoop.fs.s3a.secret.key": "admin12345",
            "spark.hadoop.fs.s3a.path.style.access": "true",
            "spark.hadoop.fs.s3a.impl": "org.apache.hadoop.fs.s3a.S3AFileSystem",
            "spark.hadoop.fs.s3a.connection.ssl.enabled": "false",
            "spark.sql.extensions": "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions",
        },
        verbose=True,
    )
