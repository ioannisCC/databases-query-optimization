
show data_directory;

-- table creation --


CREATE TABLE VesselTypes (
	code INT PRIMARY KEY,
	description VARCHAR(255)
);


CREATE TABLE vessels (
	id VARCHAR(64) PRIMARY KEY,
	type INT REFERENCES VesselTypes(code),
	flag VARCHAR(50)
);


CREATE TABLE positions (
	id SERIAL PRIMARY KEY,
	vessel_id VARCHAR(64) REFERENCES vessels(id),
	t TIMESTAMP,
	lon NUMERIC,
	lat NUMERIC,
	heading INT,
	course NUMERIC,
	speed NUMERIC
);


-- load data from csv files --


COPY VesselTypes FROM 'data/dataset/VesselTypes.csv' WITH CSV HEADER;
COPY VesselTypes FROM 'data/dataset/Vessels.csv' WITH CSV HEADER;
COPY VesselTypes FROM 'data/dataset/Positions.csv' WITH CSV HEADER;


-- query 1 --


SELECT
  DATE(t) AS calendar_day,
  COUNT(*) AS num_points
FROM
  positions
GROUP BY
  DATE(t)
ORDER BY
  num_points DESC;
  
  
-- query 2 --


SELECT
  type,
  COUNT(id) AS num_vessels
FROM
  vessels
WHERE
  flag = 'Greece'
GROUP BY
  type;

  
-- query 3a --


SELECT
DISTINCT
p.vessel_id,
v.type
FROM
positions AS p,vessels AS v
WHERE
p.speed>30 AND p.vessel_id = v.id

-- query 3b --


SELECT
  type,
  COUNT(*) AS type_count
FROM (
  SELECT DISTINCT
	p.vessel_id,
	v.type
  FROM
	positions AS p
	JOIN vessels AS v ON p.vessel_id = v.id
  WHERE
	p.speed > 30
) AS result_set
GROUP BY
  type;

 
-- query 4 --


SELECT
DATE(t),
count(*)
FROM positions INNER JOIN vessels ON positions.vessel_id = vessels.id
WHERE type IN (SELECT code FROM vesseltypes WHERE description LIKE 'Passenger%') AND t BETWEEN '2019-08-14 00:00:00' AND '2019-08-18 23:59:59'
GROUP BY
DATE(t)


-- query 5a --


SELECT
DISTINCT
vessel_id
FROM positions INNER JOIN vessels ON positions.vessel_id = vessels.id
WHERE type IN (SELECT code FROM vesseltypes WHERE description LIKE 'Cargo%') AND t BETWEEN '2019-08-15 00:00:00' AND '2019-08-18 23:59:59' AND speed = 0

--query 5b --


SELECT
vessel_id,
sum
FROM
(SELECT
vessel_id,
sum(speed)
FROM positions INNER JOIN vessels ON positions.vessel_id = vessels.id
WHERE type IN (SELECT code FROM vesseltypes WHERE description LIKE 'Cargo%') AND t BETWEEN '2019-08-12 00:00:00' AND '2019-08-19 23:59:59'
GROUP BY
vessel_id)
WHERE SUM<1
ORDER BY sum ASC



-- 2 --


ALTER SYSTEM SET shared_buffers = '8GB';

show shared_buffers;


-- 3 --


SET max_parallel_workers_per_gather = 1024;

show max_parallel_workers_per_gather;


-- 4 --


SET enable_seqscan = off;


CREATE INDEX IF NOT EXISTS idx_speed
    ON positions USING btree
    (speed ASC NULLS LAST); -- asc means ascending order

CREATE INDEX IF NOT EXISTS idx_lon_lat
    ON positions USING btree
    (lon,lat ASC NULLS LAST); -- null last means that the null rows will appear last when using order by

CREATE INDEX IF NOT EXISTS idx_time
    ON positions USING btree
    (t ASC NULLS LAST);

CREATE INDEX IF NOT EXISTS idx_flag
    ON vessels USING hash
    (flag);

CREATE INDEX IF NOT EXISTS idx_description
    ON vesseltypes USING btree
    (description ASC NULLS LAST);


-- 5 --


DROP INDEX IF EXISTS idx_lon_lat;

CREATE TABLE positions_part (
	id SERIAL,
	vessel_id VARCHAR(64) REFERENCES vessels(id),
	t TIMESTAMP,
	lon NUMERIC,
	lat NUMERIC,
	heading INT,
	course NUMERIC,
	speed NUMERIC
) PARTITION BY RANGE (EXTRACT(DAY FROM t));


-- create partitions for each day --

CREATE TABLE positions_part_01 PARTITION OF positions_part
	FOR VALUES FROM (1) TO (2);

CREATE TABLE positions_part_02 PARTITION OF positions_part
    FOR VALUES FROM (2) TO (3);

CREATE TABLE positions_part_03 PARTITION OF positions_part
    FOR VALUES FROM (3) TO (4);

CREATE TABLE positions_part_04 PARTITION OF positions_part
    FOR VALUES FROM (4) TO (5);

CREATE TABLE positions_part_05 PARTITION OF positions_part
    FOR VALUES FROM (5) TO (6);

CREATE TABLE positions_part_06 PARTITION OF positions_part
    FOR VALUES FROM (6) TO (7);

CREATE TABLE positions_part_07 PARTITION OF positions_part
    FOR VALUES FROM (7) TO (8);

CREATE TABLE positions_part_08 PARTITION OF positions_part
    FOR VALUES FROM (8) TO (9);

CREATE TABLE positions_part_09 PARTITION OF positions_part
    FOR VALUES FROM (9) TO (10);

CREATE TABLE positions_part_10 PARTITION OF positions_part
    FOR VALUES FROM (10) TO (11);

CREATE TABLE positions_part_11 PARTITION OF positions_part
    FOR VALUES FROM (11) TO (12);

CREATE TABLE positions_part_12 PARTITION OF positions_part
    FOR VALUES FROM (12) TO (13);

CREATE TABLE positions_part_13 PARTITION OF positions_part
    FOR VALUES FROM (13) TO (14);

CREATE TABLE positions_part_14 PARTITION OF positions_part
    FOR VALUES FROM (14) TO (15);

CREATE TABLE positions_part_15 PARTITION OF positions_part
    FOR VALUES FROM (15) TO (16);

CREATE TABLE positions_part_16 PARTITION OF positions_part
    FOR VALUES FROM (16) TO (17);

CREATE TABLE positions_part_17 PARTITION OF positions_part
    FOR VALUES FROM (17) TO (18);

CREATE TABLE positions_part_18 PARTITION OF positions_part
    FOR VALUES FROM (18) TO (19);

CREATE TABLE positions_part_19 PARTITION OF positions_part
    FOR VALUES FROM (19) TO (20);

CREATE TABLE positions_part_20 PARTITION OF positions_part
    FOR VALUES FROM (20) TO (21);

CREATE TABLE positions_part_21 PARTITION OF positions_part
    FOR VALUES FROM (21) TO (22);

CREATE TABLE positions_part_22 PARTITION OF positions_part
    FOR VALUES FROM (22) TO (23);

CREATE TABLE positions_part_23 PARTITION OF positions_part
    FOR VALUES FROM (23) TO (24);

CREATE TABLE positions_part_24 PARTITION OF positions_part
    FOR VALUES FROM (24) TO (25);

CREATE TABLE positions_part_25 PARTITION OF positions_part
    FOR VALUES FROM (25) TO (26);

CREATE TABLE positions_part_26 PARTITION OF positions_part
    FOR VALUES FROM (26) TO (27);

CREATE TABLE positions_part_27 PARTITION OF positions_part
    FOR VALUES FROM (27) TO (28);

CREATE TABLE positions_part_28 PARTITION OF positions_part
    FOR VALUES FROM (28) TO (29);

CREATE TABLE positions_part_29 PARTITION OF positions_part
    FOR VALUES FROM (29) TO (30);

CREATE TABLE positions_part_30 PARTITION OF positions_part
    FOR VALUES FROM (30) TO (31);

CREATE TABLE positions_part_31 PARTITION OF positions_part
    FOR VALUES FROM (31) TO (32);


-- insert data for each day --

INSERT INTO positions_part_01
SELECT * FROM positions
WHERE DATE(t) = '2019-08-01';

INSERT INTO positions_part_02
SELECT * FROM positions
WHERE DATE(t) = '2019-08-02';

INSERT INTO positions_part_03
SELECT * FROM positions
WHERE DATE(t) = '2019-08-03';

INSERT INTO positions_part_04
SELECT * FROM positions
WHERE DATE(t) = '2019-08-04';

INSERT INTO positions_part_05
SELECT * FROM positions
WHERE DATE(t) = '2019-08-05';

INSERT INTO positions_part_06
SELECT * FROM positions
WHERE DATE(t) = '2019-08-06';

INSERT INTO positions_part_07
SELECT * FROM positions
WHERE DATE(t) = '2019-08-07';

INSERT INTO positions_part_08
SELECT * FROM positions
WHERE DATE(t) = '2019-08-08';

INSERT INTO positions_part_09
SELECT * FROM positions
WHERE DATE(t) = '2019-08-09';

INSERT INTO positions_part_10
SELECT * FROM positions
WHERE DATE(t) = '2019-08-10';

INSERT INTO positions_part_11
SELECT * FROM positions
WHERE DATE(t) = '2019-08-11';

INSERT INTO positions_part_12
SELECT * FROM positions
WHERE DATE(t) = '2019-08-12';

INSERT INTO positions_part_13
SELECT * FROM positions
WHERE DATE(t) = '2019-08-13';

INSERT INTO positions_part_14
SELECT * FROM positions
WHERE DATE(t) = '2019-08-14';

INSERT INTO positions_part_15
SELECT * FROM positions
WHERE DATE(t) = '2019-08-15';

INSERT INTO positions_part_16
SELECT * FROM positions
WHERE DATE(t) = '2019-08-16';

INSERT INTO positions_part_17
SELECT * FROM positions
WHERE DATE(t) = '2019-08-17';

INSERT INTO positions_part_18
SELECT * FROM positions
WHERE DATE(t) = '2019-08-18';

INSERT INTO positions_part_19
SELECT * FROM positions
WHERE DATE(t) = '2019-08-19';

INSERT INTO positions_part_20
SELECT * FROM positions
WHERE DATE(t) = '2019-08-20';

INSERT INTO positions_part_21
SELECT * FROM positions
WHERE DATE(t) = '2019-08-21';

INSERT INTO positions_part_22
SELECT * FROM positions
WHERE DATE(t) = '2019-08-22';

INSERT INTO positions_part_23
SELECT * FROM positions
WHERE DATE(t) = '2019-08-23';

INSERT INTO positions_part_24
SELECT * FROM positions
WHERE DATE(t) = '2019-08-24';

INSERT INTO positions_part_25
SELECT * FROM positions
WHERE DATE(t) = '2019-08-25';

INSERT INTO positions_part_26
SELECT * FROM positions
WHERE DATE(t) = '2019-08-26';

INSERT INTO positions_part_27
SELECT * FROM positions
WHERE DATE(t) = '2019-08-27';

INSERT INTO positions_part_28
SELECT * FROM positions
WHERE DATE(t) = '2019-08-28';

INSERT INTO positions_part_29
SELECT * FROM positions
WHERE DATE(t) = '2019-08-29';

INSERT INTO positions_part_30
SELECT * FROM positions
WHERE DATE(t) = '2019-08-30';

INSERT INTO positions_part_31
SELECT * FROM positions
WHERE DATE(t) = '2019-08-31';


-- query 1 --


SELECT
  DATE(t) AS calendar_day,
  COUNT(*) AS num_points
FROM
  positions_part
GROUP BY
  DATE(t)
ORDER BY
  num_points DESC;
  
  
-- query 2 --


SELECT
  type,
  COUNT(id) AS num_vessels
FROM
  vessels
WHERE
  flag = 'Greece'
GROUP BY
  type;

  
-- query 3a --


SELECT DISTINCT
  p.vessel_id,
  v.type
FROM
  positions_part AS p
INNER JOIN
  vessels AS v ON p.vessel_id = v.id
WHERE
  p.speed > 30;


-- query 3b --


SELECT
  type,
  COUNT(*) AS type_count
FROM (
  SELECT DISTINCT
    p.vessel_id,
    v.type
  FROM
    positions_part AS p
  JOIN
    vessels AS v ON p.vessel_id = v.id
  WHERE
    p.speed > 30
) AS result_set
GROUP BY
  type;

 
-- query 4 --


SELECT
  DATE(t),
  COUNT(*)
FROM
  positions_part
INNER JOIN
  vessels ON positions_part.vessel_id = vessels.id
WHERE
  type IN (SELECT code FROM vesseltypes WHERE description LIKE 'Passenger%') AND t BETWEEN '2019-08-14 00:00:00' AND '2019-08-18 23:59:59'
GROUP BY
  DATE(t);


-- query 5a --


SELECT DISTINCT
  vessel_id
FROM
  positions_part
INNER JOIN
  vessels ON positions_part.vessel_id = vessels.id
WHERE
  type IN (SELECT code FROM vesseltypes WHERE description LIKE 'Cargo%') AND t BETWEEN '2019-08-15 00:00:00' AND '2019-08-18 23:59:59' AND speed = 0;


--query 5b --


SELECT
  vessel_id,
  SUM(speed) AS sum
FROM
  positions_part
INNER JOIN
  vessels ON positions_part.vessel_id = vessels.id
WHERE
  type IN (SELECT code FROM vesseltypes WHERE description LIKE 'Cargo%') AND t BETWEEN '2019-08-12 00:00:00' AND '2019-08-19 23:59:59'
GROUP BY
  vessel_id
HAVING
  SUM(speed) < 1
ORDER BY
  sum ASC;


-- end --