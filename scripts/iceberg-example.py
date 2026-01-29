#!/usr/bin/env python3
"""
Simple Apache Iceberg Script
============================
Direct script to create table and insert 3 sample records
"""

from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, TimestampType, DoubleType
from datetime import datetime

print("🚀 Simple Apache Iceberg Script")
print("=" * 40)

# Create Spark session
print("⚡ Creating Spark session...")
spark = SparkSession.builder \
    .appName("SimpleIcebergScript") \
    .config("spark.sql.extensions", "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions") \
    .config("spark.sql.catalog.iceberg", "org.apache.iceberg.spark.SparkCatalog") \
    .config("spark.sql.catalog.iceberg.type", "rest") \
    .config("spark.sql.catalog.iceberg.uri", "http://iceberg-rest:8181") \
    .config("spark.sql.catalog.iceberg.table-default.format-version", "2") \
    .config("spark.sql.defaultCatalog", "iceberg") \
    .getOrCreate()

try:
    # Create namespace
    print("📋 Creating namespace...")
    spark.sql("CREATE NAMESPACE IF NOT EXISTS iceberg.demo")
    
    # Create table with format version 2
    print("📋 Creating BusGPS table with Iceberg v2 format...")
    spark.sql("""
        CREATE TABLE IF NOT EXISTS iceberg.demo.bus_gps (
            bus_id STRING,
            route_id STRING,
            latitude DOUBLE,
            longitude DOUBLE,
            speed DOUBLE,
            timestamp TIMESTAMP
        ) USING iceberg
        TBLPROPERTIES (
            'format-version' = '2',
            'write.delete.mode' = 'merge-on-read',
            'write.update.mode' = 'merge-on-read'
        )
    """)
    print("✅ Table created!")
    
    # Create sample data
    print("📊 Creating 3 sample records...")
    data = [
        ("BUS_001", "ROUTE_1", 10.7769, 106.7009, 45.5, datetime(2024, 10, 29, 10, 30, 0)),
        ("BUS_002", "ROUTE_2", 10.7829, 106.6819, 38.2, datetime(2024, 10, 29, 10, 31, 0)),
        ("BUS_003", "ROUTE_1", 10.7909, 106.6919, 52.1, datetime(2024, 10, 29, 10, 32, 0))
    ]
    
    # Define schema
    schema = StructType([
        StructField("bus_id", StringType(), True),
        StructField("route_id", StringType(), True),
        StructField("latitude", DoubleType(), True),
        StructField("longitude", DoubleType(), True),
        StructField("speed", DoubleType(), True),
        StructField("timestamp", TimestampType(), True)
    ])
    
    # Create DataFrame
    df = spark.createDataFrame(data, schema)
    
    # Write to Iceberg
    print("💾 Writing data to Iceberg table...")
    df.write \
        .format("iceberg") \
        .mode("append") \
        .saveAsTable("iceberg.demo.bus_gps")
    print("✅ Data written!")
    
    # Read and display data
    print("🔍 Reading data from table...")
    result_df = spark.table("iceberg.demo.bus_gps")
    print(f"📈 Total records: {result_df.count()}")
    
    print("\n📋 All BusGPS data:")
    result_df.show(truncate=False)
    
    # Simple query
    print("\n🚍 Buses on ROUTE_1:")
    spark.sql("""
        SELECT bus_id, speed, latitude, longitude 
        FROM iceberg.demo.bus_gps 
        WHERE route_id = 'ROUTE_1'
    """).show()
    
    print("\n🎉 Script completed successfully!")
    
except Exception as e:
    print(f"❌ Error: {str(e)}")
    raise
finally:    
    spark.stop()