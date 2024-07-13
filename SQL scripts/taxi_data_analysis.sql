SELECT * FROM `taxi-dataengg-proj.taxi_data.main_table` LIMIT 1000;

SELECT rate_code_name, COUNT(m.total_amount)
FROM `taxi-dataengg-proj.taxi_data.main_table` AS m
JOIN `taxi-dataengg-proj.taxi_data.rate_code_dim` AS rate ON m.rate_code_id = rate.rate_code_id
GROUP BY rate.rate_code_name 
LIMIT 100;

SELECT payment_type_name, ROUND(SUM(m.total_amount),2) as Total_Cost
FROM `taxi-dataengg-proj.taxi_data.main_table` AS m
JOIN `taxi-dataengg-proj.taxi_data.payment_type_dim` AS pay ON m.payment_type_id = pay.payment_type_id
GROUP BY pay.payment_type_name;

SELECT payment_type_name, COUNT(m.total_amount) as Total_payments
FROM `taxi-dataengg-proj.taxi_data.main_table` AS m
JOIN `taxi-dataengg-proj.taxi_data.payment_type_dim` AS pay ON m.payment_type_id = pay.payment_type_id
GROUP BY pay.payment_type_name;


SELECT AVG(trip_distance) as Avg_distance
FROM `taxi-dataengg-proj.taxi_data.main_table` AS m
JOIN `taxi-dataengg-proj.taxi_data.trip_distance_dim` AS trip ON m.trip_distance_id = trip.trip_distance_id


