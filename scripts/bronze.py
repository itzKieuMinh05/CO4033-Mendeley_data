from pyspark.sql import SparkSession

spark = SparkSession.builder \
    .appName("BronzeTable") \
    .config("spark.sql.extensions", "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions") \
    .config("spark.sql.catalog.iceberg", "org.apache.iceberg.spark.SparkCatalog") \
    .config("spark.sql.catalog.iceberg.type", "rest") \
    .config("spark.sql.catalog.iceberg.uri", "http://iceberg-rest:8181") \
    .config("spark.sql.catalog.iceberg.table-default.format-version", "2") \
    .config("spark.sql.defaultCatalog", "iceberg") \
    .getOrCreate()

spark.sparkContext.setLogLevel("ERROR")

namespace = "bronze"
spark.sql(f"CREATE NAMESPACE IF NOT EXISTS {namespace}")

tables = [
    ("weather", """
        CREATE TABLE IF NOT EXISTS iceberg.bronze.weather (
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
            load_at TIMESTAMP
        ) USING iceberg
        PARTITIONED BY (days(time))
    """)
]

# Create tables
failed_tables = []

for table_name, ddl in tables:
    try:
        print(f"Creating table: {table_name}")
        spark.sql(ddl)
        print(f"✅ Table {table_name} created successfully")
    except Exception as e:
        failed_tables.append((table_name, str(e)))
        print(f"❌ Error creating table {table_name}: {str(e)}")

if failed_tables:
    print("\n❌ Failed to create one or more bronze tables:")
    for table_name, err in failed_tables:
        print(f"- {table_name}: {err}")
    spark.stop()
    raise RuntimeError("Bronze table creation failed")

print(f"\n✅ All bronze tables created in namespace: {namespace}")
spark.stop()