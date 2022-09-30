DESC hive.fraud.fraud_data;

SELECT DISTINCT label
FROM hive.fraud.fraud_data;

SELECT DISTINCT trans_type
FROM hive.fraud.fraud_data;

SELECT trans_type, COUNT(timestamp)
FROM hive.fraud.fraud_data
GROUP BY trans_type
ORDER BY 2 DESC LIMIT 3;

-- For the next query, as explained here -> https://github.com/trinodb/trino/issues/9097
-- it's needed to turn off the statistics, otherwise it fails
SET SESSION hive.parquet_ignore_statistics = true;
SELECT trans_type, COUNT(timestamp)
FROM hive.fraud.fraud_data
WHERE hive.fraud.fraud_data.label = 'fraud'
GROUP BY trans_type
ORDER BY 2 DESC LIMIT 3;

SELECT trans_type, COUNT(timestamp)
FROM hive.fraud.fraud_data
WHERE hive.fraud.fraud_data.label = 'legitimate'
GROUP BY trans_type
ORDER BY 2 DESC LIMIT 3;

-- Missing the TOTAL, and the PERCENTAGE
SELECT label, trans_type, count(timestamp)
FROM hive.fraud.fraud_data
GROUP BY label, trans_type
ORDER BY 1, 2;
/*
WITH totalFraud AS (SELECT 'fraud' as label, count(timestamp) as total FROM hive.fraud.fraud_data WHERE label='fraud'),
     totalLegit AS (SELECT 'legitimate' as label, count(timestamp) as total FROM hive.fraud.fraud_data WHERE label='legitimate')
SELECT label, trans_type, count(timestamp), totalFraud.total, totalLegit.total
FROM hive.fraud.fraud_data
JOIN totalFraud USING (label)
JOIN totalLegit USING (label)
GROUP BY label, trans_type
ORDER BY 1, 2;
*/

/*
--- Not working the quantiles.
SELECT label, values_at_quantiles(timestamp, 0.01, 0.05, 0.1, 0.25, 0.5, 0.75, 0.9, 0.95, 0.99)
FROM hive.fraud.fraud_data
GROUP BY label;
*/