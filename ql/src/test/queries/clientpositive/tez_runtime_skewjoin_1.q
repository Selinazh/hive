CREATE TABLE DEST1(key STRING, value BIGINT) STORED AS TEXTFILE;

FROM SRC
INSERT OVERWRITE TABLE DEST1 SELECT SRC.key, COUNT(1) GROUP BY SRC.key limit 10;

CREATE TABLE SRCPART_LIMIT (key STRING, value STRING) PARTITIONED BY (ds STRING, hr STRING);
ALTER TABLE SRCPART_LIMIT ADD PARTITION (ds='2008-04-08', hr='11');
ALTER TABLE SRCPART_LIMIT ADD PARTITION (ds='2008-04-08', hr='12');

LOAD DATA LOCAL INPATH '../../data/files/kv1.txt' INTO TABLE SRCPART_LIMIT PARTITION (ds='2008-04-08', hr='11');
LOAD DATA LOCAL INPATH '../../data/files/kv1.txt' INTO TABLE SRCPART_LIMIT PARTITION (ds='2008-04-08', hr='12');

EXPLAIN SELECT a.key, a.value, b.value bvalue FROM SRC a JOIN DEST1 b ON a.key = b.key ORDER BY a.key;
SELECT a.key, a.value, b.value bvalue FROM SRC a JOIN DEST1 b ON a.key = b.key ORDER BY a.key;

SELECT a.key, a.value, b.value bvalue FROM SRC a LEFT OUTER JOIN DEST1 b ON a.key = b.key ORDER BY a.key LIMIT 10;

SET hive.skewjoin.key=2;
SET hive.exec.reducers.bytes.per.reducer=1000;
SET hive.optimize.skewjoin=true;
SET hive.auto.convert.join=false;
SET hive.vectorized.execution.enabled=true;
EXPLAIN SELECT a.key, a.value, b.value bvalue FROM SRC a JOIN DEST1 b ON a.key = b.key ORDER BY a.key;
SELECT a.key, a.value, b.value bvalue FROM SRC a JOIN DEST1 b ON a.key = b.key ORDER BY a.key;

EXPLAIN SELECT a.key, a.value, b.value bvalue FROM SRC a LEFT OUTER JOIN DEST1 b ON a.key = b.key ORDER BY a.key LIMIT 10;
SELECT a.key, a.value, b.value bvalue FROM SRC a LEFT OUTER JOIN DEST1 b ON a.key = b.key ORDER BY a.key LIMIT 10;

-- with partition prunner 
EXPLAIN EXTENDED SELECT * FROM SRCPART_LIMIT JOIN DEST1 ON DEST1.key = SRCPART_LIMIT.key AND SRCPART_LIMIT.hr='11';

-- with constant filter
EXPLAIN SELECT * FROM SRCPART_LIMIT JOIN DEST1 ON DEST1.key = SRCPART_LIMIT.key AND SRCPART_LIMIT.hr='11' AND SRCPART_LIMIT.key=0;
SELECT * FROM SRCPART_LIMIT JOIN DEST1 ON DEST1.key = SRCPART_LIMIT.key AND SRCPART_LIMIT.hr='11' AND SRCPART_LIMIT.key=0;

-- no support if key column data type no match
EXPLAIN SELECT * FROM SRC JOIN DEST1 on SRC.key = DEST1.value LIMIT 10; 
-- no support if RIGHT OUTER JOIN
EXPLAIN SELECT a.key, a.value, b.value bvalue FROM SRC a RIGHT OUTER JOIN DEST1 b ON a.key = b.key;


