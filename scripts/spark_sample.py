from pyspark.sql import SparkSession
from pyspark.sql.types import (
    StructType, StructField,
    StringType, IntegerType, DoubleType, BooleanType, TimestampType
)
from pyspark.sql.functions import col, avg, max, min, to_timestamp

spark = (
    SparkSession.builder
    .appName("WeatherSparkToMinIOTest")
    .config("spark.hadoop.fs.s3a.endpoint", "http://minio:9000")
    .config("spark.hadoop.fs.s3a.access.key", "minioadmin")
    .config("spark.hadoop.fs.s3a.secret.key", "minioadmin123")
    .config("spark.hadoop.fs.s3a.path.style.access", "true")
    .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem")
    .getOrCreate()
)


# -----------------------------
# 1. Định nghĩa schema
# -----------------------------
schema = StructType([
    StructField("time", StringType(), True),
    StructField("province", StringType(), True),
    StructField("city", StringType(), True),
    StructField("temperature", DoubleType(), True),
    StructField("temp_min", DoubleType(), True),
    StructField("temp_max", DoubleType(), True),
    StructField("humidity", IntegerType(), True),
    StructField("feels_like", DoubleType(), True),
    StructField("visibility", IntegerType(), True),
    StructField("precipitation", DoubleType(), True),
    StructField("cloudcover", IntegerType(), True),
    StructField("wind_speed", DoubleType(), True),
    StructField("wind_gust", DoubleType(), True),
    StructField("wind_direction", IntegerType(), True),
    StructField("pressure", IntegerType(), True),
    StructField("is_day", BooleanType(), True),
    StructField("weather_code", IntegerType(), True),
    StructField("weather_main", StringType(), True),
    StructField("weather_description", StringType(), True),
    StructField("weather_icon", StringType(), True),
])

# -----------------------------
# 2. Data giả (đủ để test pipeline)
# -----------------------------
data = [
    (
        "2026-02-11 14:00:00", "HCM", "Ho Chi Minh",
        32.5, 30.0, 34.0, 65, 35.0,
        10000, 0.0, 40, 3.5, 5.0, 180,
        1012, True, 800, "Clear", "clear sky", "01d"
    ),
    (
        "2026-02-11 14:00:00", "HN", "Ha Noi",
        18.2, 17.0, 19.0, 80, 18.0,
        8000, 0.5, 75, 2.0, 3.0, 90,
        1018, True, 500, "Rain", "light rain", "10d"
    ),
]

df = spark.createDataFrame(data, schema=schema)

# Cast time -> timestamp cho đàng hoàng
df = df.withColumn("time", to_timestamp(col("time")))

df.printSchema()
df.show(truncate=False)

# -----------------------------
# 3. Xử lý thử (cho thấy Spark có làm việc)
# -----------------------------
agg_df = (
    df.groupBy("province", "city")
    .agg(
        avg("temperature").alias("avg_temperature"),
        max("temp_max").alias("max_temperature"),
        min("temp_min").alias("min_temperature"),
        avg("humidity").alias("avg_humidity"),
        avg("wind_speed").alias("avg_wind_speed"),
    )
)

agg_df.show()

# -----------------------------
# 4. Ghi ra MinIO
# -----------------------------
output_path = "s3a://iceberg/weather/processed"

(
    agg_df
    .write
    .mode("overwrite")
    .parquet(output_path)
)

print("✅ Weather data written to MinIO")

spark.stop()
