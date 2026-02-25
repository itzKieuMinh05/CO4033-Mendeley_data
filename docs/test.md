
docker exec -it spark-master spark-submit /opt/spark/scripts/rest-example.py


docker exec -it kafka-broker-1 kafka-topics    --create    --topic bus_gps_raw    --bootstrap-server kafka-broker-1:29092    --partitions 3    --replication-factor 2


docker exec -it kafka-broker-1 kafka-console-consumer `
   --bootstrap-server kafka-broker-1:29092 `
   --topic bus_gps_raw `
   --from-beginning