SELECT 'satellite_A' AS provider, *
FROM 'http://127.0.0.1:8001/observations.csv'
UNION ALL
SELECT 'satellite_B' AS provider, *
FROM 'http://127.0.0.1:8002/observations.csv'
UNION ALL
SELECT 'ground_station' AS provider, *
FROM 'http://127.0.0.1:8003/observations.csv';
