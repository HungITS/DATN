# from pyspark.sql import SparkSession

# def read_from_s3(spark: SparkSession, path: str):
#     print(f"Reading data from: {path}")
#     column_names = ["User_ID", "Product_ID", "Category_ID", "Behavior", "Timestamp"]
#     return spark.read.option("header", False).csv(path).toDF(*column_names)

from pyspark.sql import SparkSession
import os

def read_from_local(spark: SparkSession, input_path: str):
    local_path = f"file://{os.path.abspath(input_path)}"
    column_names = ["User_ID", "Product_ID", "Category_ID", "Behavior", "Timestamp"]
    return spark.read.option("header", False).csv(local_path).toDF(*column_names)