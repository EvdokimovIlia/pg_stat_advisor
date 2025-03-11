## pg_stat_advisor - PostgreSQL advisor to create extended statistics

- [pg_stat_advisor - PostgreSQL advisor to create extended statistics](#description)
	- [Description](#description)
  - [Limitations](#description)
  - [Versions](#description)
	- [Installation](#installation)
	- [Using](#using)
	- [Examples](#examples)
	- [Authors](#authors)


### [Description](#description)

pg_stat_advisor is a PostgreSQL extension designed to analyze query performance and create additional statistics to improve query plan.

### [Limitations](#description)


For extended statistics to be created, the following conditions must be met:
1.	max_parallel_workers_per_gather must be set to 0 (parallel workers disabled).
2.	The SQL statement must not be an UPDATE or DELETE.
3.	The execution plan cannot include NestedLoop, MergeJoin, or HashJoin nodes.
4.	The query must be run against a non-temporary table.
5.	The table has been analyzed and at least one column does have ndistinct != 1.
6.	Extended statistics must not already exist for these columns on the table.

### [Versions](#description)

PostgreSQL 17

### [Installation](#installation)

In order to build and install the extension do "make", and then "sudo make install".

```
make
sudo make install
```
If this extension is compiled with PGXS-infrastructure, so the pg_config tool must be available from your PATH environment variable. And then do "USE_PGXS=1 make, and then "sudo USE_PGXS=1 make install".
```
USE_PGXS=1 make
sudo USE_PGXS=1 make install
```

Append pg_stat_advisor to shared_preload_libraries configuration parameter in your postgresql.conf file then restart the PostgreSQL database to apply the changes. 

- Note: You cannot use LOAD 'pg_stat_advisor'; because this extension relies on hooks that must be initialized at server startup.
```
shared_preload_libraries = 'pg_stat_advisor';
```

### [Using](#using)

There is the pg_stat_advisor.suggest_statistics_threshold GUC that can be used to set a suggest_statistics_threshold. It is the the ratio of the planned rows compared to the total tuples produced. If parameter is set by 0, the printing switches off.

For example:
```
SET pg_stat_advisor.suggest_statistics_threshold = 0.1;
```


The second GUC parameter, pg_stat_advisor.ring_buffer_capacity, specifies the size of the static shared-memory ring buffer used by the global queue to coordinate communication between the ExecutorEndHook in all backends and a single background worker — BackgroundTaskManager (which creates dynamic database workers and forwards commands to them for background execution). This parameter must be set in postgresql.conf and cannot be changed dynamically.

### [Examples](#examples)


```
SET pg_stat_advisor.suggest_statistics_threshold = 0.1;

CREATE TABLE t (i INT, j INT);
INSERT INTO t SELECT i/10, i/100 FROM generate_series(1, 1000000) i;
ANALYZE t;
EXPLAIN ANALYZE SELECT * FROM t WHERE i = 100 AND j = 10;
                                                   QUERY PLAN

----------------------------------------------------------------------------------------------
------------------
 Gather  (cost=1000.00..11675.10 rows=1 width=8) (actual time=0.526..61.564 rows=10 loops=1)
   Workers Planned: 2
   Workers Launched: 2
   ->  Parallel Seq Scan on t  (cost=0.00..10675.00 rows=1 width=8) (actual time=35.369..54.44
7 rows=3 loops=3)
         Filter: ((i = 100) AND (j = 10))
         Rows Removed by Filter: 333330
 Planning Time: 0.148 ms
 Execution Time: 61.589 ms
(8 rows)


EXPLAIN ANALYZE SELECT * FROM t WHERE i = 100 AND j = 10;
                                                   QUERY PLAN
----------------------------------------------------------------------------------------------
------------------
 Gather  (cost=1000.00..11675.10 rows=10 width=8) (actual time=0.400..59.292 rows=10 loops=1)
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

### [Authors](#authors)

Ilia Evdokimov
