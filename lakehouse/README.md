# Test Command Spark MinIO

docker exec -it spark-master /opt/spark/bin/pyspark \
  --master spark://spark-master:7077

# Tạo data test
data = [("MinIO_Test", 100), ("Spark_S3_Connection", 200), ("All_Systems_Go", 300)]
df = spark.createDataFrame(data, ["Name", "Value"])

# Ghi trực tiếp vào bucket 'lakehouse' trên MinIO
df.write.mode("overwrite").parquet("s3a://lakehouse/test_connection")

# Đọc lại để kiểm tra
check_df = spark.read.parquet("s3a://lakehouse/test_connection")
check_df.show()

# Tạo database thông qua Catalog 'metastore'
spark.sql("CREATE DATABASE IF NOT EXISTS metastore.db_test")

# Tạo bảng Iceberg
spark.sql("CREATE TABLE metastore.db_test.iceberg_minio (id bigint, data string) USING iceberg")

# Insert dữ liệu
spark.sql("INSERT INTO metastore.db_test.iceberg_minio VALUES (1, 'Iceberg_MinIO_HMS_Success')")

# Truy vấn kiểm tra
spark.table("metastore.db_test.iceberg_minio").show()

-- Kiểm tra bảng đã tạo từ PySpark
SELECT * FROM metastore.db_test.iceberg_minio;

-- Tạo bảng mới từ DBeaver
CREATE TABLE metastore.db_test.dbeaver_test (id int, note string) USING iceberg;
INSERT INTO metastore.db_test.dbeaver_test VALUES (100, 'Created from DBeaver');
