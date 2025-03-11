LOAD 'pg_stat_advisor';
SET pg_stat_advisor.suggest_statistics_threshold = 1.0;

--
-- Prepare 2-column table
--

CREATE TABLE t (i INT, j INT) WITH (autovacuum_enabled = false);
INSERT INTO t SELECT i/10, i/100 FROM  GENERATE_SERIES(1,1000000) i;

-- Check without ANALYZE
EXPLAIN (ANALYZE, COSTS OFF, TIMING OFF, SUMMARY OFF) SELECT * FROM t WHERE i = 100 AND j = 10;
SELECT pg_sleep(1);
SELECT stxname, stxkeys, stxkind FROM pg_statistic_ext;
SELECT analyze_count FROM pg_stat_all_tables WHERE relname = 't';

-- Check creating statistics with ANALYZE
VACUUM ANALYZE t;
SELECT analyze_count FROM pg_stat_all_tables WHERE relname = 't';
EXPLAIN (ANALYZE, COSTS OFF, TIMING OFF, SUMMARY OFF) SELECT * FROM t WHERE i = 100 AND j = 10;
SELECT pg_sleep(1);
SELECT stxname, stxkeys, stxkind FROM pg_statistic_ext;
SELECT analyze_count FROM pg_stat_all_tables WHERE relname = 't';
DROP STATISTICS t_i_j_stat;

-- Check order of name
EXPLAIN (ANALYZE, COSTS OFF, TIMING OFF, SUMMARY OFF) SELECT * FROM t WHERE j = 10 AND i = 100;
SELECT pg_sleep(1);
SELECT stxname, stxkeys, stxkind FROM pg_statistic_ext;
SELECT analyze_count FROM pg_stat_all_tables WHERE relname = 't';
DROP STATISTICS t_i_j_stat;

-- estimation gretaer than actual
EXPLAIN (ANALYZE, COSTS OFF, TIMING OFF, SUMMARY OFF) SELECT * FROM t WHERE i = 100 OR j = 10;
SELECT pg_sleep(1);
SELECT stxname, stxkeys, stxkind FROM pg_statistic_ext;
SELECT analyze_count FROM pg_stat_all_tables WHERE relname = 't';

-- Check temporary tables
CREATE TEMP TABLE temp (i INT, j INT) WITH (autovacuum_enabled = false);
INSERT INTO temp SELECT i/10, i/100 FROM  GENERATE_SERIES(1,1000000) i;
VACUUM ANALYZE temp;
SELECT analyze_count FROM pg_stat_all_tables WHERE relname = 'temp';
EXPLAIN (ANALYZE, COSTS OFF, TIMING OFF, SUMMARY OFF) SELECT * FROM temp WHERE i = 100 AND j = 10;
SELECT pg_sleep(1);
SELECT stxname, stxkeys, stxkind FROM pg_statistic_ext;
SELECT analyze_count FROM pg_stat_all_tables WHERE relname = 'temp';

-- Check part-analyzed table
CREATE TABLE t2 (i INT, j INT, k INT) WITH (autovacuum_enabled = false);
INSERT INTO t2 SELECT i/10, i/100, i/10 FROM  GENERATE_SERIES(1,1000000) i;
VACUUM ANALYZE t2(i, j);
SELECT analyze_count FROM pg_stat_all_tables WHERE relname = 't2';
EXPLAIN (ANALYZE, COSTS OFF, TIMING OFF, SUMMARY OFF) SELECT * FROM t2 WHERE i = 100 AND j = 10;
SELECT pg_sleep(1);
SELECT stxname, stxkeys, stxkind FROM pg_statistic_ext;
SELECT analyze_count FROM pg_stat_all_tables WHERE relname = 't2';

-- Check column with ndistinct = 1
TRUNCATE t2;
INSERT INTO t2 SELECT i/10, i/100, 1 FROM  GENERATE_SERIES(1,1000000) i;
VACUUM ANALYZE t2;
SELECT analyze_count FROM pg_stat_all_tables WHERE relname = 't2';
EXPLAIN (ANALYZE, COSTS OFF, TIMING OFF, SUMMARY OFF) SELECT * FROM t2 WHERE i = 100 AND j = 10 AND k = 1;
SELECT pg_sleep(1);
SELECT stxname, stxkeys, stxkind FROM pg_statistic_ext;
SELECT analyze_count FROM pg_stat_all_tables WHERE relname = 't2';
