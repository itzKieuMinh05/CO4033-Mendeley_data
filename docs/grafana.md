1. Kết nối




2.

SELECT
  datetime AS time,
  vehicle AS name,
  lat,
  lng
FROM bus_db.bus_gps
WHERE $__timeFilter(datetime)
ORDER BY datetime ASC


SELECT
  t1.datetime AS time,
  t1.vehicle AS name,
  t1.lat,
  t1.lng,
  t1.speed
FROM bus_db.bus_gps t1
JOIN (
    SELECT vehicle, MAX(datetime) as max_time
    FROM bus_db.bus_gps
    WHERE $__timeFilter(datetime)
    GROUP BY vehicle
) t2 ON t1.vehicle = t2.vehicle AND t1.datetime = t2.max_time