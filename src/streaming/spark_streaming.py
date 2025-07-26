from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, StringType, DoubleType, TimestampType, LongType
import pandas as pd

# Import các hàm từ module hiện có
from src.etl.transform.data_cleaning import clean_data
from src.analyst.eda import run_eda_realtime  # Giả định hàm này tồn tại
from src.analyst.customer_segmentation import run_kmeans_realtime  # Hàm từ file của bạn

# Khởi tạo Spark Session
spark = SparkSession.builder \
    .appName("RealTimeUserBehavior") \
    .master("local[*]") \
    .config("spark.hadoop.fs.defaultFS", "file:///") \
    .config("spark.hadoop.fs.hdfs.impl", "org.apache.hadoop.fs.LocalFileSystem") \
    .getOrCreate()

# Định nghĩa schema cho dữ liệu
schema = StructType([
    StructField("User_ID", StringType(), True),
    StructField("Product_ID", StringType(), True),
    StructField("Category_ID", StringType(), True),
    StructField("Behavior", StringType(), True),
    StructField("Timestamp", LongType(), True)
])

# Đọc dữ liệu stream từ file CSV
streaming_df = spark.readStream \
    .option("header", "true") \
    .schema(schema) \
    .csv("file:///home/hungits/Study/DATN/Project/data/stream_input/")

# Hàm xử lý micro-batch
def process_batch(batch_df, batch_id):
    # Làm sạch dữ liệu bằng hàm clean_data từ src/etl/transform/data_cleaning.py
    cleaned_df = clean_data(batch_df)

    # Chuyển Spark DataFrame thành pandas DataFrame
    pandas_df = cleaned_df.toPandas()

    if not pandas_df.empty:
        # Thực hiện EDA bằng hàm từ src/analysis/eda.py
        run_eda_realtime(pandas_df, output_dir="data/eda")
        # Thực hiện RFM và phân khúc bằng hàm run_kmeans từ src/analysis/customer_segmentation.py
        # Giả định run_kmeans trả về biểu đồ hoặc lưu trực tiếp
        run_kmeans_realtime(pandas_df, output_dir="data/kmeans", n_clusters=3)  # Nếu hàm lưu ảnh vào file, cần đảm bảo đường dẫn

        print(f"Batch {batch_id} processed.")

# Áp dụng hàm xử lý cho mỗi micro-batch
query = streaming_df.writeStream \
    .foreachBatch(process_batch) \
    .outputMode("append") \
    .option("checkpointLocation", "data/checkpoints/streaming/") \
    .start()

# Chờ đến khi dừng thủ công
query.awaitTermination()