from pyspark.sql.functions import col, from_unixtime, to_date, hour, date_format, count, when, to_timestamp
from pyspark.sql import functions as F
def clean_data(df):
    print("Cleaning and transforming data...")

    sampled_users_df = df.select("User_ID").distinct().sample(withReplacement=False, fraction=0.05, seed=42)
    df = df.join(sampled_users_df, on="User_ID", how="inner")

    # Đổi tên hành vi
    df = df.withColumn("Behavior",
        F.when(F.col("Behavior") == "pv", "PageView")
        .when(F.col("Behavior") == "buy", "Buy")
        .when(F.col("Behavior") == "cart", "AddToCart")
        .when(F.col("Behavior") == "fav", "Favorite")
        .otherwise(F.col("Behavior"))
    )


    #
    df = df.withColumn("Datetime", from_unixtime(col("Timestamp").cast("long")).cast("timestamp"))

    # Thêm Hour, Day_of_Week, Date
    df = df \
        .withColumn("Hour", hour("Datetime")) \
        .withColumn("Day_of_Week", date_format("Datetime", "EEEE")) \
        .withColumn("Date", to_date("Datetime"))
    
    # Chuyển 'Behavior' và 'Day_of_Week' thành kiểu category (trong Spark gọi là string hoặc dùng StringIndexer nếu cần)
    df = df.withColumn("Behavior", col("Behavior").cast("string"))
    df = df.withColumn("Day_of_Week", col("Day_of_Week").cast("string"))

    # Chuyển 'Datetime' sang kiểu timestamp
    df = df.withColumn("Datetime", to_timestamp("Datetime"))

    # Chuyển 'Date' sang kiểu date
    df = df.withColumn("Date", to_date("Date"))

    # Xóa các dòng trùng lặp
    df = df.dropDuplicates()

    # Kiểm tra số lượng null trong tất cả các cột
    null_counts = df.select([
        count(when(col(c).isNull(), c)).alias(c) for c in df.columns
    ])

    # Lấy số lượng null dưới dạng dictionary
    null_summary = null_counts.collect()[0].asDict()

    # Tìm các cột có 0 < số lượng null < 100
    columns_to_drop = [col_name for col_name, null_count in null_summary.items() if 0 < null_count < 100]

    # Xóa các hàng có null trong các cột thỏa mãn
    if columns_to_drop:
        df_cleaned = df.na.drop(subset=columns_to_drop)
    else:
        df_cleaned = df
    
    return df_cleaned
