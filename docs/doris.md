1. Kiểm tra IP của Doris FE:
docker inspect -f "{{range .NetworkSettings.Networks}}{{.IPAddress}}{{end}}" doris-fe


Ví dụ:
172.18.0.2

Thay vào: 
doris-be:
    image: apache/doris:be-2.1.9
    container_name: doris-be
    ports:
      - "8040:8040"   # BE HTTP
      - "9050:9050"   # BE Internal
    depends_on:
      - doris-fe
    environment:
      FE_SERVERS: fe1:172.20.0.98:9010 # Thay IP ở đây
      BE_ADDR: 0.0.0.0:9050

2. Sử dụng MySQL Client (để kết nối Doris FE qua MySQL)
docker exec -it doris-fe mysql -h 127.0.0.1 -P 9030 -u root


[Hoặc sử dụng web UI: ](http://localhost:8030/Playground/result/bus_db-undefined)


3. Tạo database trong doris tên là bus_db và
   
   CREATE TABLE bus_gps (
    `datetime` DATETIME,
    `vehicle` VARCHAR(20),
    `date` DATE,
    `lng` DOUBLE,
    `lat` DOUBLE,
    `driver` VARCHAR(50),
    `speed` DOUBLE,
    `door_up` BOOLEAN,
    `door_down` BOOLEAN
)
DUPLICATE KEY(`datetime`, `vehicle`)
DISTRIBUTED BY HASH(`vehicle`) BUCKETS 3
PROPERTIES (
    "replication_num" = "1"
);

4. Tạo rountine


CREATE ROUTINE LOAD bus_gps_job ON bus_gps
COLUMNS(datetime, vehicle, date, lng, lat, driver, speed, door_up, door_down)
PROPERTIES
(
    "format" = "json",
    "max_batch_interval" = "10",
    "max_batch_rows" = "200100"
)
FROM KAFKA
(
    "kafka_broker_list" = "kafka-broker-1:29092,kafka-broker-2:29094",
    "kafka_topic" = "bus_gps_raw",
    "property.group.id" = "doris_gps_consumer_group_v2",
    "property.kafka_default_offsets" = "OFFSET_BEGINNING"
);

5. Kiểm tra routine
   
SHOW ROUTINE LOAD FOR bus_gps_job;



6. Tạo external Catalog cho Doris kt nối với Lakehouse
CREATE CATALOG iceberg PROPERTIES (
    'type'='iceberg',
    'iceberg.catalog.type'='rest',
    'uri'='http://iceberg-rest:8181',
    'warehouse'='s3a://lake/',
    's3.endpoint'='http://minio:9000',
    's3.access_key'='minioadmin',
    's3.secret_key'='minioadmin123',
    's3.path_style'='true'
);

'uri' = 'http://iceberg-rest:8181',



-- 1. Thoát ra ngoài để tránh lỗi session
SWITCH internal;

-- 2. Xóa catalog cấu hình sai
DROP CATALOG IF EXISTS iceberg;

-- 3. Tạo lại với cấu hình Path Style Access (Bắt buộc cho MinIO)
CREATE CATALOG iceberg PROPERTIES (
    "type" = "iceberg",
    "iceberg.catalog.type" = "rest",
    "uri" = "http://iceberg-rest:8181",
    "s3.endpoint" = "http://minio:9000",
    "s3.access_key" = "minioadmin",
    "s3.secret_key" = "minioadmin123",
    "s3.region" = "us-east-1",
    "use_path_style" = "true",
    "s3.connection.ssl.enabled" = "false"
);

SHOW CREATE CATALOG iceberg

SELECT * FROM iceberg.test.bus_gps LIMIT 10;

7.