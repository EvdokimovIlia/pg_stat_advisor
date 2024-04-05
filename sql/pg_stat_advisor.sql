LOAD 'pg_stat_advisor';
SET pg_stat_advisor.analyze_scale_factor = 0.4;
SET pg_stat_advisor.suggest_statistics_threshold = 0.11;

CREATE TABLE my_tbl(fld_1 INTEGER, fld_2 BIGINT) WITH (autovacuum_enabled = false);
INSERT INTO my_tbl (fld_1, fld_2)
SELECT
     i/100 as fld_1,
     i/500 as fld_2
FROM generate_series(1, 10000000) s(i);
ANALYZE my_tbl;
INSERT INTO my_tbl (fld_1, fld_2)
SELECT
     i/100 as fld_1,
     i/500 as fld_2
FROM generate_series(1, 10000000) s(i);

EXPLAIN (ANALYZE, COSTS OFF, SUMMARY OFF, TIMING OFF) SELECT * FROM my_tbl WHERE fld_1 = 1 AND fld_2 = 0;

ANALYZE my_tbl;
EXPLAIN (ANALYZE, COSTS OFF, SUMMARY OFF, TIMING OFF) SELECT * FROM my_tbl WHERE fld_1 = 1 AND fld_2 = 0;

CREATE STATISTICS my_tbl_fld_1_fld_2 ON fld_1, fld_2 FROM my_tbl;
EXPLAIN (ANALYZE, COSTS OFF, SUMMARY OFF, TIMING OFF) SELECT * FROM my_tbl WHERE fld_1 = 1 AND fld_2 = 0;