from pyspark.sql import SparkSession
from pyspark.sql.functions import col, when, current_timestamp, round as spark_round

# Initialize SparkSession with Iceberg
spark = SparkSession.builder \
    .appName("SilverWeatherTransformation") \
    .config("spark.sql.extensions", "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions") \
    .config("spark.sql.catalog.iceberg", "org.apache.iceberg.spark.SparkCatalog") \
    .config("spark.sql.catalog.iceberg.type", "rest") \
    .config("spark.sql.catalog.iceberg.uri", "http://iceberg-rest:8181") \
    .config("spark.sql.catalog.iceberg.table-default.format-version", "2") \
    .config("spark.sql.defaultCatalog", "iceberg") \
    .getOrCreate()

spark.sparkContext.setLogLevel("ERROR")

namespace = "silver"
spark.sql(f"CREATE NAMESPACE IF NOT EXISTS {namespace}")

# Create silver weather table
print("Creating silver.weather table...")
spark.sql("""
    CREATE TABLE IF NOT EXISTS iceberg.silver.weather (
        time TIMESTAMP,
        province STRING,
        city STRING,
        temperature DOUBLE,
        temp_min DOUBLE,
        temp_max DOUBLE,
        humidity DOUBLE,
        feels_like DOUBLE,
        visibility DOUBLE,
        precipitation DOUBLE,
        cloudcover DOUBLE,
        wind_speed DOUBLE,
        wind_gust DOUBLE,
        wind_direction DOUBLE,
        pressure DOUBLE,
        is_day BOOLEAN,
        weather_code INT,
        weather_main STRING,
        weather_description STRING,
        weather_icon STRING,
        update_at TIMESTAMP
    ) USING iceberg
    PARTITIONED BY (days(time))
""")
print("✅ Table iceberg.silver.weather created successfully")

# Read streaming from bronze.weather
print("📖 Reading stream from bronze.weather...")
bronze_df = spark.readStream \
    .format("iceberg") \
    .table("iceberg.bronze.weather")

# Clean and transform data
print("🔄 Applying transformations...")
transformed_df = bronze_df \
    .withColumn("temperature", when(col("temperature").isNull(), 0.0).otherwise(spark_round(col("temperature"), 2))) \
    .withColumn("temp_min", when(col("temp_min").isNull(), 0.0).otherwise(spark_round(col("temp_min"), 2))) \
    .withColumn("temp_max", when(col("temp_max").isNull(), 0.0).otherwise(spark_round(col("temp_max"), 2))) \
    .withColumn("humidity", when(col("humidity").isNull(), 0.0).otherwise(spark_round(col("humidity"), 2))) \
    .withColumn("feels_like", when(col("feels_like").isNull(), 0.0).otherwise(spark_round(col("feels_like"), 2))) \
    .withColumn("visibility", when(col("visibility").isNull(), 0.0).otherwise(spark_round(col("visibility"), 2))) \
    .withColumn("precipitation", when(col("precipitation").isNull(), 0.0).otherwise(spark_round(col("precipitation"), 2))) \
    .withColumn("cloudcover", when(col("cloudcover").isNull(), 0.0).otherwise(spark_round(col("cloudcover"), 2))) \
    .withColumn("wind_speed", when(col("wind_speed").isNull(), 0.0).otherwise(spark_round(col("wind_speed"), 2))) \
    .withColumn("wind_gust", when(col("wind_gust").isNull(), 0.0).otherwise(spark_round(col("wind_gust"), 2))) \
    .withColumn("wind_direction", when(col("wind_direction").isNull(), 0.0).otherwise(spark_round(col("wind_direction"), 2))) \
    .withColumn("pressure", when(col("pressure").isNull(), 0.0).otherwise(spark_round(col("pressure"), 2))) \
    .withColumn("is_day", when(col("is_day").isNull(), False).otherwise(col("is_day"))) \
    .withColumn("weather_code", when(col("weather_code").isNull(), -1).otherwise(col("weather_code"))) \
    .withColumn("update_at", current_timestamp())

# Select columns for silver table
selected_columns = [
    "time", "province", "city", "temperature", "temp_min", "temp_max",
    "humidity", "feels_like", "visibility", "precipitation", "cloudcover",
    "wind_speed", "wind_gust", "wind_direction", "pressure", "is_day",
    "weather_code", "weather_main", "weather_description", "weather_icon",
    "update_at"
]

transformed_df = transformed_df.select(*selected_columns)

print("🚀 Starting streaming transformation and write to iceberg.silver.weather...")

# Write batch to Iceberg
def write_batch_to_iceberg(batch_df, batch_id):
    """Write each batch to the silver.weather Iceberg table"""
    print(f"📝 Processing batch {batch_id}...")
    rec_count = batch_df.count()
    print(f"[INFO] Batch {batch_id}: received {rec_count} records")
    
    if rec_count > 0:
        print(f"[DEBUG] Showing up to 10 rows from batch {batch_id}:")
        batch_df.show(10, truncate=False)
        
        batch_df.write \
            .format("iceberg") \
            .mode("append") \
            .saveAsTable("iceberg.silver.weather")
        
        print(f"✅ Batch {batch_id}: successfully wrote {rec_count} records to silver.weather")
    else:
        print(f"ℹ️ Batch {batch_id}: no records to write")

# Start streaming query
query = transformed_df.writeStream \
    .foreachBatch(write_batch_to_iceberg) \
    .outputMode("append") \
    .option("checkpointLocation", "s3a://lake/silver/weather/_checkpoints") \
    .start()

print("✅ Streaming write started. Awaiting termination...")

try:
    query.awaitTermination()
except KeyboardInterrupt:
    print("\n⚠️ Streaming stopped by user")
    query.stop()
finally:
    spark.stop()
    print("🛑 Spark session stopped")