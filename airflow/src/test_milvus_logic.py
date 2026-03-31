from pymilvus import MilvusClient, DataType
from pyspark.sql import SparkSession

# --- 1. Khởi tạo Spark Session ---
spark = SparkSession.builder.appName("Milvus_Test_Paper").getOrCreate()

COLLECTION_NAME = "demo_vectors"
MILVUS_URI = "http://milvus-standalone:19530"
DIM = 2  # Giống hệt paper bạn gửi

# --- 2. Khởi tạo Collection trên Driver ---
client = MilvusClient(uri=MILVUS_URI)

if client.has_collection(COLLECTION_NAME):
    client.drop_collection(COLLECTION_NAME)

# Định nghĩa Schema y hệt paper
schema = client.create_schema(auto_id=False, enable_dynamic_field=True)
schema.add_field(field_name="id", datatype=DataType.INT64, is_primary=True)
schema.add_field(field_name="vector", datatype=DataType.FLOAT_VECTOR, dim=DIM)
schema.add_field(field_name="text", datatype=DataType.VARCHAR, max_length=512)

# Indexing
index_params = client.prepare_index_params()
index_params.add_index(field_name="vector", index_type="FLAT", metric_type="L2")

client.create_collection(
    collection_name=COLLECTION_NAME, schema=schema, index_params=index_params
)
client.close()

# --- 3. Tạo dữ liệu giả lập bằng Spark ---
data = [
    (1, [0.1, 0.2], "sample 1"),
    (2, [0.2, 0.3], "sample 2"),
    (3, [0.3, 0.4], "sample 3"),
    (4, [0.4, 0.5], "sample 4"),
    (5, [0.5, 0.6], "sample 5"),
]
df = spark.createDataFrame(data, ["id", "vector", "text"])


# --- 4. Hàm Insert trên Worker ---
def insert_to_milvus(partition):
    client_worker = MilvusClient(uri=MILVUS_URI)
    batch = []
    for row in partition:
        batch.append({"id": row.id, "vector": row.vector, "text": row.text})

    count = 0
    if batch:
        res = client_worker.insert(collection_name=COLLECTION_NAME, data=batch)
        count = res.get("insert_count", len(batch))
    client_worker.close()
    return [count]


results = df.rdd.mapPartitions(insert_to_milvus).collect()
print(f"Successfully inserted {sum(results)} test vectors.")

# --- 5. Load để sẵn sàng Search ---
client_final = MilvusClient(uri=MILVUS_URI)
client_final.load_collection(COLLECTION_NAME)
client_final.close()

spark.stop()
