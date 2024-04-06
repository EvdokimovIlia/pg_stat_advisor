## pg_stat_advisor - PostgreSQL advisor to create statistics

- [pg_stat_advisor - PostgreSQL advisor to create and update statistics](#credcheck---postgresql-usernamepassword-checks)
	- [Description](#description)
	- [Installation](#installation)
	- [Using](#using)
	- [Examples](#examples)
	- [Authors](#authors)


### [Description](#description)

pg_stat_advisor is a PostgreSQL extension designed to analyze query performance and recommend the creation of extended statistics to improve rows estimates and update statistics.

### [Installation](#installation)

if this extension is compiled in contrib directory, so the pg_config tool must be available from your PATH environment variable. And then do "make
, and then "sudo make install".

```
make
sudo make install
```
if this extension is compiled with PGXS-infrastructure, so the pg_config tool must be available from your PATH environment variable. And then do "USE_PGXS=1 make
, and then "sudo USE_PGXS=1 make install".
```
USE_PGXS=1 make
sudo USE_PGXS=1 make install
```

Append pg_stat_advisor to shared_preload_libraries configuration parameter in your postgresql.conf file then restart the PostgreSQL database to apply the changes. Or you can use "LOAD 'pg_stat_advisor';"command
```
LOAD 'pg_stat_advisor';
```

### [Using](#using)

There is also the pg_stat_advisor.suggest_statistics_threshold GUC that can be used to set a suggest_statistics_threshold. For example:
```
SET pg_stat_advisor.suggest_statistics_threshold = 1.0;
```
There is the pg_stat_advisor.analyze_scale_factor GUC that can be used to set analyze_scale_factor. For example:
```
SET pg_stat_advisor.analyze_scale_factor = 0.3;
```

### [Examples](#examples)


```
postgres=# LOAD 'pg_stat_advisor';
LOAD
postgres=# SET pg_stat_advisor.analyze_scale_factor = 0.1;
SET
postgres=# CREATE TABLE my_tbl(fld_1 INTEGER, fld_2 BIGINT) WITH (autovacuum_enabled = false);
CREATE TABLE
postgres=# INSERT INTO my_tbl (fld_1, fld_2)
SELECT
     i/100 as fld_1,
     i/500 as fld_2
FROM generate_series(1, 10000000) s(i);
INSERT 0 1000000
postgres=# ANALYZE my_tbl;
ANALYZE
postgres=# INSERT INTO my_tbl (fld_1, fld_2)
SELECT
     i/100 as fld_1,
     i/500 as fld_2
FROM generate_series(1, 10000000) s(i);
INSERT 0 1000000
postgres=# EXPLAIN ANALYZE SELECT * FROM my_tbl WHERE fld_1 = 500 AND fld_2 = 100;
NOTICE:  pg_stat_advisor suggestion: 'ANALYZE my_tbl'
                                                   QUERY PLAN                                 
                  
----------------------------------------------------------------------------------------------
------------------
 Gather  (cost=1000.00..11675.10 rows=1 width=8) (actual time=0.526..61.564 rows=100 loops=1)
   Workers Planned: 2
   Workers Launched: 2
   ->  Parallel Seq Scan on t  (cost=0.00..10675.00 rows=1 width=8) (actual time=35.369..54.44
7 rows=3 loops=3)
         Filter: ((i = 100) AND (j = 10))
         Rows Removed by Filter: 333330
 Planning Time: 0.148 ms
 Execution Time: 61.589 ms
(8 rows)

postgres=# ANALYZE my_tbl;
ANALYZE
postgres=# set pg_stat_advisor.suggest_statistics_threshold = 0.2;
SET
postgres=# EXPLAIN ANALYZE SELECT * FROM my_tbl WHERE fld_1 = 500 AND fld_2 = 100;
NOTICE:  pg_stat_advisor suggestion: CREATE STATISTICS my_tbl_fld_1_fld_2 ON fld_1, fld_2 FROM my_tbl
                                                   QUERY PLAN                                 
                  
----------------------------------------------------------------------------------------------
------------------
 Gather  (cost=1000.00..11675.10 rows=1 width=8) (actual time=0.400..59.292 rows=100 loops=1)
   Workers Planned: 2
   Workers Launched: 2
   ->  Parallel Seq Scan on t  (cost=0.00..10675.00 rows=1 width=8) (actual time=35.614..54.29
1 rows=3 loops=3)
         Filter: ((i = 100) AND (j = 10))
         Rows Removed by Filter: 333330
 Planning Time: 0.081 ms
 Execution Time: 59.413 ms
(8 rows)
```

### [Authors]

Ilia Evdokimov
