# import io
# import torch
# import torchvision.models as models
# import torchvision.transforms as transforms
# from PIL import Image
# from pyspark.sql import SparkSession
# from pyspark.sql.functions import udf, col, current_timestamp
# from pyspark.sql.types import ArrayType, FloatType
# from pymilvus import MilvusClient, DataType

# # --- 1. Khởi tạo Spark Session ---
# spark = SparkSession.builder.appName("Image_To_Lakehouse").getOrCreate()

# # --- 2. Xử lý Model AI (Singleton trên Worker) ---
# _MODEL = None


# def get_model():
#     global _MODEL
#     if _MODEL is None:
#         weights = models.ResNet50_Weights.DEFAULT
#         base_model = models.resnet50(weights=weights)
#         base_model.eval()
#         _MODEL = torch.nn.Sequential(*(list(base_model.children())[:-1]))
#     return _MODEL


# def extract_vector(content):
#     if content is None:
#         return None
#     try:
#         model = get_model()
#         img = Image.open(io.BytesIO(content)).convert("RGB")
#         preprocess = transforms.Compose(
#             [
#                 transforms.Resize(256),
#                 transforms.CenterCrop(224),
#                 transforms.ToTensor(),
#                 transforms.Normalize(
#                     mean=[0.485, 0.456, 0.406], std=[0.229, 0.224, 0.225]
#                 ),
#             ]
#         )
#         input_tensor = preprocess(img).unsqueeze(0)
#         with torch.no_grad():
#             output = model(input_tensor)
#         return output.flatten().numpy().tolist()
#     except Exception as e:
#         print(f"Error extracting vector: {e}")
#         return None


# extract_vector_udf = udf(extract_vector, ArrayType(FloatType()))

# # --- 3. Luồng dữ liệu (S3A -> Spark) ---
# image_df = (
#     spark.read.format("binaryFile")
#     .option("pathGlobFilter", "*.jpg")
#     .load("s3a://lakehouse/images/")
# )

# raw_image_count = image_df.count()
# print(f"--- DEBUG: Spark found {raw_image_count} images in S3 path ---")

# if raw_image_count == 0:
#     print(
#         "--- ERROR: No images to process. Check your MinIO bucket 'lakehouse/images/' ---"
#     )
#     spark.stop()
#     exit(1)

# vector_df = (
#     image_df.withColumn("vector", extract_vector_udf(col("content")))
#     .filter(col("vector").isNotNull())
#     .withColumn("processed_at", current_timestamp())
#     .select("path", "vector", "processed_at")
#     .repartition(2)
# )

# # --- 4. Lưu Metadata vào Iceberg ---
# spark.sql("CREATE DATABASE IF NOT EXISTS metastore.db")
# spark.sql("""
#     CREATE TABLE IF NOT EXISTS metastore.db.image_metadata (
#         path STRING,
#         processed_at TIMESTAMP
#     ) USING iceberg
# """)

# vector_df.select("path", "processed_at").writeTo("metastore.db.image_metadata").append()

# # --- 5. Lưu Vector vào Milvus ---

# COLLECTION_NAME = "image_embeddings"
# MILVUS_URI = "http://milvus-standalone:19530"

# # BƯỚC 5.1: Khởi tạo Schema CHI TIẾT trên DRIVER
# client_driver = MilvusClient(uri=MILVUS_URI)
# if client_driver.has_collection(COLLECTION_NAME):
#     client_driver.drop_collection(COLLECTION_NAME)

# # 2. Định nghĩa Schema
# schema = client_driver.create_schema(auto_id=True, enable_dynamic_field=True)
# schema.add_field(field_name="id", datatype=DataType.INT64, is_primary=True)
# schema.add_field(field_name="vector", datatype=DataType.FLOAT_VECTOR, dim=2048)
# schema.add_field(field_name="path", datatype=DataType.VARCHAR, max_length=500)

# # 3. Định nghĩa Index Params
# index_params = client_driver.prepare_index_params()
# index_params.add_index(
#     field_name="vector", index_type="IVF_FLAT", metric_type="L2", params={"nlist": 128}
# )

# # 4. Tạo Collection
# client_driver.create_collection(
#     collection_name=COLLECTION_NAME, schema=schema, index_params=index_params
# )
# print(f"--- SUCCESS: Collection {COLLECTION_NAME} created with proper schema ---")
# client_driver.close()


# # BƯỚC 5.2: Hàm Insert chạy trên WORKER
# def insert_to_milvus(partition):
#     client = MilvusClient(uri=MILVUS_URI)
#     batch_data = []

#     for row in partition:
#         if row.vector is not None and len(row.vector) > 0:
#             batch_data.append(
#                 {"path": str(row.path), "vector": [float(x) for x in row.vector]}
#             )
#         else:
#             # In ra log của Spark Worker để debug
#             print(
#                 f"--- WARNING: Found null vector for path {row.path}. Is ResNet weight downloaded? ---"
#             )

#     total_count = 0
#     if batch_data:
#         try:
#             res = client.insert(collection_name=COLLECTION_NAME, data=batch_data)
#             # MilvusClient insert trả về dict, ta lấy số lượng ID sinh ra
#             if isinstance(res, dict) and "insert_count" in res:
#                 total_count = res["insert_count"]
#             elif isinstance(res, dict) and "ids" in res:
#                 total_count = len(res["ids"])
#             else:
#                 total_count = len(batch_data)
#         except Exception as e:
#             print(f"Worker insert error details: {e}")

#     client.close()
#     return [total_count]


# # Kích hoạt quá trình ghi Milvus
# inserted_counts = vector_df.rdd.mapPartitions(insert_to_milvus).collect()
# print(f"--- SUCCESS: Inserted {sum(inserted_counts)} vectors to Milvus ---")

# spark.stop()

import io
from PIL import Image
from pyspark.sql import SparkSession
from pyspark.sql.functions import udf, col, current_timestamp
from pyspark.sql.types import ArrayType, FloatType
from pymilvus import MilvusClient, DataType
from sentence_transformers import SentenceTransformer

# --- 1. Khởi tạo Spark Session ---
spark = SparkSession.builder.appName("Image_To_Milvus_CLIP").getOrCreate()

# --- 2. Xử lý Model CLIP (Singleton trên Worker) ---
_MODEL = None


def get_model():
    global _MODEL
    if _MODEL is None:
        # Load model CLIP (Model này convert cả Text và Image sang cùng không gian vector)
        # CLIP-ViT-B-32 cho ra vector 512 chiều
        _MODEL = SentenceTransformer("clip-ViT-B-32")
    return _MODEL


def extract_vector(content):
    if content is None:
        return None
    try:
        model = get_model()
        img = Image.open(io.BytesIO(content)).convert("RGB")
        # CLIP của sentence-transformers xử lý luôn khâu preprocess
        vector = model.encode(img)
        return [float(x) for x in vector]
    except Exception as e:
        print(f"CLIP Error on Worker: {e}")
        return None


extract_vector_udf = udf(extract_vector, ArrayType(FloatType()))

# --- 3. Luồng dữ liệu (S3A -> Spark) ---
image_df = (
    spark.read.format("binaryFile")
    .option("pathGlobFilter", "*.jpg")
    .load("s3a://lakehouse/images/")
)

# Kiểm tra số lượng ảnh
raw_count = image_df.count()
print(f"--- DEBUG: Found {raw_count} images in S3 ---")

vector_df = (
    image_df.withColumn("vector", extract_vector_udf(col("content")))
    .filter(col("vector").isNotNull())
    .withColumn("processed_at", current_timestamp())
    .select("path", "vector", "processed_at")
    .repartition(2)
)

# --- 4. Lưu Metadata vào Iceberg ---
spark.sql("CREATE DATABASE IF NOT EXISTS metastore.db")
spark.sql(
    "CREATE TABLE IF NOT EXISTS metastore.db.image_metadata (path STRING, processed_at TIMESTAMP) USING iceberg"
)
vector_df.select("path", "processed_at").writeTo("metastore.db.image_metadata").append()

# --- 5. Lưu Vector vào Milvus ---
COLLECTION_NAME = "image_embeddings_clip"
MILVUS_URI = "http://milvus-standalone:19530"
DIM = 512  # CLIP-ViT-B-32 cho ra 512 chiều

client_driver = MilvusClient(uri=MILVUS_URI)
if client_driver.has_collection(COLLECTION_NAME):
    client_driver.drop_collection(COLLECTION_NAME)

# Schema cho CLIP
schema = client_driver.create_schema(auto_id=True, enable_dynamic_field=True)
schema.add_field(field_name="id", datatype=DataType.INT64, is_primary=True)
schema.add_field(field_name="vector", datatype=DataType.FLOAT_VECTOR, dim=DIM)
schema.add_field(field_name="path", datatype=DataType.VARCHAR, max_length=500)

index_params = client_driver.prepare_index_params()
index_params.add_index(
    field_name="vector",
    index_type="IVF_FLAT",
    metric_type="COSINE",
    params={"nlist": 128},
)

client_driver.create_collection(
    collection_name=COLLECTION_NAME, schema=schema, index_params=index_params
)
client_driver.close()


# BƯỚC 5.2: Worker Insert
def insert_to_milvus(partition):
    client_worker = MilvusClient(uri=MILVUS_URI)
    batch_data = []
    for row in partition:
        batch_data.append({"path": str(row.path), "vector": row.vector})

    total = 0
    if batch_data:
        try:
            res = client_worker.insert(collection_name=COLLECTION_NAME, data=batch_data)
            total = res.get("insert_count", len(res.get("ids", [])))
        except Exception as e:
            print(f"Milvus Insert Error: {e}")
    client_worker.close()
    return [total]


inserted_counts = vector_df.rdd.mapPartitions(insert_to_milvus).collect()
print(f"--- SUCCESS: Inserted {sum(inserted_counts)} CLIP vectors to Milvus ---")

# Load để sẵn sàng search
client_final = MilvusClient(uri=MILVUS_URI)
client_final.load_collection(COLLECTION_NAME)
client_final.close()

spark.stop()
