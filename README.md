## pg_stat_advisor - PostgreSQL advisor to create statistics

- [pg_stat_advisor - PostgreSQL advisor to create statistics](#credcheck---postgresql-usernamepassword-checks)
	- [Description](#description)
	- [Installation](#installation)
	- [Using](#using)
	- [Examples](#examples)
	- [Authors](#authors)


### [Description](#description)

pg_stat_advisor is a PostgreSQL extension designed to analyze query performance and recommend the creation of additional statistics to improve execution speed.

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

Append credcheck to shared_preload_libraries configuration parameter in your postgresql.conf file then restart the PostgreSQL database to apply the changes. Or you can use "LOAD 'pg_stat_advisor';"command
```
LOAD 'pg_stat_advisor';
```

### [Using](#using)

There is also the pg_stat_advisor.add_statistics_threshold GUC that can be used to set a add_statistics_threshold. For example:
```
SET pg_stat_advisor.add_statistics_threshold = 1.0;
```

### [Examples](#examples)


```
postgres=# create table t (i int, j int);
CREATE TABLE
postgres=# insert into t select i/10, i/100 from generate_series(1, 1000000) i;
INSERT 0 1000000
postgres=# analyze t;
ANALYZE
postgres=# explain analyze select * from t where i = 100 and j = 10;
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


postgres=# load 'pg_stat_advisor';
LOAD
postgres=# set pg_stat_advisor.add_statistics_threshold = 0.1;
SET
postgres=# explain analyze select * from t where i = 100 and j = 10;
NOTICE:  pg_stat_advisor suggestion: CREATE STATISTICS t_i_j ON i, j FROM t
                                                   QUERY PLAN                                 
                  
----------------------------------------------------------------------------------------------
------------------
 Gather  (cost=1000.00..11675.10 rows=1 width=8) (actual time=0.400..59.292 rows=10 loops=1)
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
