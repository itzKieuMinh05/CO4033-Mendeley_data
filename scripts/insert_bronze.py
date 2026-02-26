from pyspark.sql import SparkSession
from pyspark.sql.functions import to_timestamp, current_timestamp, col

spark = SparkSession.builder \
    .appName("InsertBronzeWeather") \
    .config("spark.sql.extensions", "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions") \
    .config("spark.sql.catalog.iceberg", "org.apache.iceberg.spark.SparkCatalog") \
    .config("spark.sql.catalog.iceberg.type", "rest") \
    .config("spark.sql.catalog.iceberg.uri", "http://iceberg-rest:8181") \
    .config("spark.sql.catalog.iceberg.warehouse", "s3://iceberg/warehouse") \
    .config("spark.sql.catalog.iceberg.io-impl", "org.apache.iceberg.aws.s3.S3FileIO") \
    .config("spark.sql.catalog.iceberg.s3.endpoint", "http://minio:9000") \
    .config("spark.sql.catalog.iceberg.s3.path-style-access", "true") \
    .config("spark.sql.catalog.iceberg.s3.access-key-id", "minioadmin") \
    .config("spark.sql.catalog.iceberg.s3.secret-access-key", "minioadmin123") \
    .config("spark.sql.catalog.iceberg.s3.region", "us-east-1") \
    .config("spark.sql.catalog.iceberg.s3.ssl-enabled", "false") \
    .config("spark.sql.defaultCatalog", "iceberg") \
    .getOrCreate()

# Example source file in mounted volume: /opt/spark/data/processed_weather.csv
df = spark.read.option("header", True).csv("/opt/spark/data/processed/processed_weather.csv")

df2 = (
    df.withColumn("time", to_timestamp(col("time")))
      .withColumn("temperature", col("temperature").cast("double"))
      .withColumn("temp_min", col("temp_min").cast("double"))
      .withColumn("temp_max", col("temp_max").cast("double"))
      .withColumn("humidity", col("humidity").cast("double"))
      .withColumn("feels_like", col("feels_like").cast("double"))
      .withColumn("visibility", col("visibility").cast("double"))
      .withColumn("precipitation", col("precipitation").cast("double"))
      .withColumn("cloudcover", col("cloudcover").cast("double"))
      .withColumn("wind_speed", col("wind_speed").cast("double"))
      .withColumn("wind_gust", col("wind_gust").cast("double"))
      .withColumn("wind_direction", col("wind_direction").cast("double"))
      .withColumn("pressure", col("pressure").cast("double"))
      .withColumn("is_day", col("is_day").cast("boolean"))
      .withColumn("weather_code", col("weather_code").cast("int"))
      .withColumn("load_at", current_timestamp())
)

df2.writeTo("iceberg.bronze.weather").append()
print("Inserted rows into iceberg.bronze.weather")

spark.stop()