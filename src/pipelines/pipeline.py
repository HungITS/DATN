from pyspark.sql import SparkSession
from src.analyst.eda import run_eda
from analyst.customer_segmentation import run_kmeans
from src.etl.extract.spark_reader import read_from_local
from src.etl.transform.data_cleaning import clean_data
from src.etl.load.spark_loader import write_to_local
from src.utils.config_loader import load_config
from src.utils.logger import get_logger
logger = get_logger(__name__)


# Main ETL pipeline for user behavior data
# This script orchestrates the ETL process by reading data from S3, cleaning it, and writing it back to S3. 

if __name__ == "__main__":
    # config = load_config("config/s3_config.yaml")
    config = load_config("config/local_config.yaml")
    spark = SparkSession.builder \
        .appName("UserBehaviorETLPipeline") \
        .config("spark.driver.memory", "2g") \
        .config("spark.executor.memory", "2g") \
        .config("spark.hadoop.fs.defaultFS", "file:///") \
        .getOrCreate()

    # input_path = config["input"]["s3_path"]
    # output_path = config["output"]["s3_clean_path"]
    input_path = config["input"]["local_path"]
    output_path = config["output"]["local_clean_path"]


    logger.info(f"Reading data from: {input_path}")
    df_raw = read_from_local(spark, input_path)
    logger.info("Data read successfully.")

    logger.info("Starting data cleaning...")
    df_clean = clean_data(df_raw)
    logger.info("Data cleaning completed.")

    logger.info(f"Writing cleaned data to: {output_path}")
    write_to_local(df_clean, output_path)
    logger.info("Data written successfully.")

    logger.info("Running EDA, RFM, and K-means...")
    run_eda(output_path, output_dir="data/eda")
    run_kmeans(output_path, output_dir="data/kmeans", n_clusters=4)
    logger.info("EDA, RFM, and K-means completed.")
    
    spark.stop()
    # Return paths for app.py
    results = {
        "processed": output_path,
        "eda": "data/eda/",
        "kmeans": "data/kmeans/kmeans_clusters.png"
    }
    print("ETL pipeline completed successfully.")