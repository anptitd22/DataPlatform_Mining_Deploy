from airflow import DAG
from airflow.providers.apache.spark.operators.spark_submit import SparkSubmitOperator
from datetime import datetime, timedelta

default_args = {
    'owner': 'an_data_eng',
    'start_date': datetime(2026, 3, 29),
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

with DAG(
    'image_to_vector_iceberg_pipeline',
    default_args=default_args,
    schedule='@daily',
    catchup=False,
    tags=['lakehouse', 'iceberg', 'milvus']
) as dag:

    # Task này sẽ gửi Job xử lý ảnh vào Spark Cluster
    process_images_to_vector = SparkSubmitOperator(
        task_id='process_images_to_vector',
        conn_id='spark_default',
        # Do An đã ADD sẵn jar vào image Spark, không cần dùng tham số 'packages' nữa
        # nhưng Airflow Worker cần nhìn thấy file để submit, nên tốt nhất vẫn khai báo:
        packages="org.apache.iceberg:iceberg-spark-runtime-3.5_2.12:1.6.1,org.apache.hadoop:hadoop-aws:3.3.4,com.amazonaws:aws-java-sdk-bundle:1.12.367",
        application='/opt/airflow/src/image_to_vector.py',
        conf={
            # 1. THAY ĐỔI QUAN TRỌNG NHẤT: Đổi từ S3FileIO sang HadoopFileIO
            "spark.sql.catalog.metastore.io-impl": "org.apache.iceberg.hadoop.HadoopFileIO",
            
            # 2. Các cấu hình Catalog giữ nguyên
            "spark.sql.catalog.metastore": "org.apache.iceberg.spark.SparkCatalog",
            "spark.sql.catalog.metastore.catalog-impl": "org.apache.iceberg.hive.HiveCatalog",
            "spark.sql.catalog.metastore.uri": "thrift://metastore:9083",
            "spark.sql.catalog.metastore.warehouse": "s3a://lakehouse/",
            
            # 3. Cấu hình S3A (MinIO)
            "spark.hadoop.fs.s3a.endpoint": "http://minio-nginx:9000",
            "spark.hadoop.fs.s3a.access.key": "admin",
            "spark.hadoop.fs.s3a.secret.key": "admin12345",
            "spark.hadoop.fs.s3a.path.style.access": "true",
            "spark.hadoop.fs.s3a.impl": "org.apache.hadoop.fs.s3a.S3AFileSystem",
            "spark.hadoop.fs.s3a.connection.ssl.enabled": "false",
            
            # 4. Extension
            "spark.sql.extensions": "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions"
        },
        verbose=True
    )