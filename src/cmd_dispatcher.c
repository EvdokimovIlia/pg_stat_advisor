#include "include/cmd_dispatcher.h"
#include "libpq/pqformat.h"
#include "miscadmin.h"
#include "storage/ipc.h"
#include "nodes/parsenodes.h"
#include "storage/lockdefs.h"
#include "utils/rel.h"
#include "access/table.h"
#include "utils/memutils.h"

#include "commands/vacuum.h"
#include "storage/bufmgr.h"
#include "utils/lsyscache.h"
#include "nodes/makefuncs.h"
#include "statistics/extended_stats_internal.h"
#include "catalog/pg_statistic_ext.h"
#include "utils/syscache.h"

#include "access/xact.h"
#include "executor/spi.h"
#include "commands/defrem.h"

static void HandleQueryCommand(char *sql);
static void HandleStatAdvisorCommand(Oid table_id, Bitmapset *columns);
static void HandleExitCommand(void);

StringInfo CreateQueryCommand(Oid dbid, Oid userid, Size sql_len, char *sql)
{
    StringInfo buf;
    buf = makeStringInfo();

    pq_sendint32(buf, (int32)QUERY);
    pq_sendint32(buf, (int32)dbid);
    pq_sendint32(buf, (int32)userid);

    pq_sendint32(buf, (int32)sql_len);
    pq_sendbytes(buf, sql, sql_len);

    return buf;
}

StringInfo CreateStatAdvisorCommand(Oid dbid, Oid userid, Oid table_id, Bitmapset *columns)
{
    int nwords;
    StringInfo buf = makeStringInfo();

    pq_sendint32(buf, (int32)STAT_ADVISOR);
    pq_sendint32(buf, (int32)dbid);
    pq_sendint32(buf, (int32)userid);
    pq_sendint32(buf, (int32)table_id);

    // put bitmapset data
    pq_sendint32(buf, (int32)columns->type);
    nwords = columns->nwords;
    pq_sendint32(buf, (int32)nwords);
    for (int i = 0; i < nwords; i++)
    {
        pq_sendint64(buf, (int64)columns->words[i]);
    }

    return buf;
}

StringInfo CreateExitCommand(void)
{
    StringInfo buf = makeStringInfo();

    pq_sendint32(buf, (int32)EXIT);

    return buf;
}

void HandleCommand(const StringInfo command_data)
{
    CommandType type = pq_getmsgint(command_data, 4);

    switch (type)
    {
        case QUERY:
        {
            int32   sql_len;
            char   *sql;

            pq_getmsgint(command_data, 4); /* dbid */
            pq_getmsgint(command_data, 4); /* userid */
            sql_len = pq_getmsgint(command_data, 4);
            sql = palloc0(sql_len + 1);
            memcpy(sql, pq_getmsgbytes(command_data, sql_len), sql_len);
            sql[sql_len] = '\0';

            HandleQueryCommand(sql);

            pfree(sql);
            break;
        }
        case STAT_ADVISOR:
        {
            Oid         table_id;
            NodeTag     bms_type;
            int         nwords;
            Size        size;
            Bitmapset  *columns_bms;

            pq_getmsgint(command_data, 4); /* dbid */
            pq_getmsgint(command_data, 4); /* userid */

            table_id = pq_getmsgint(command_data, 4);
            bms_type = pq_getmsgint(command_data, 4);
            nwords = pq_getmsgint(command_data, 4);

            size = offsetof(Bitmapset, words) + nwords * sizeof(bitmapword);
            columns_bms = (Bitmapset *)palloc(size);

            columns_bms->type = bms_type;
            columns_bms->nwords = nwords;
            for (int i = 0; i < nwords; i++)
                columns_bms->words[i] = (bitmapword)pq_getmsgint64(command_data);

            HandleStatAdvisorCommand(table_id, columns_bms);

            pfree(columns_bms);
            break;
        }
        case EXIT:
        {
            HandleExitCommand();
            break;
        }
        default:
        {
            ereport(WARNING, (errmsg("Unknown command type: %d", type)));
            break;
        }
    }
}

bool IsDatabaseCommand(StringInfo buf, Oid *dbid, Oid *userid)
{
    CommandType type = pq_getmsgint(buf, 4);

    switch (type)
    {
        case QUERY:
        case STAT_ADVISOR:
        {
            *dbid = pq_getmsgint(buf, 4);
            *userid = pq_getmsgint(buf, 4);

            buf->cursor = 0;
            return true;
        }
        default:
            return false;
    }
}

// -------------------------

static void HandleExitCommand(void)
{
    elog(LOG, "Exiting...");
    proc_exit(0);
}

// -------------------------

static void HandleQueryCommand(char *sql)
{
    int ret;

    /* Start a transaction */
    StartTransactionCommand();

    /* Initialize SPI for executing SQL commands */
    if (SPI_connect() != SPI_OK_CONNECT)
        ereport(ERROR, (errmsg("SPI_connect failed")));

    /* Set up a snapshot for consistent data visibility */
    PushActiveSnapshot(GetTransactionSnapshot());

    /* Process the received SQL command */
    elog(LOG, "Executing query %s...", sql);

    /* Execute the SQL command */
    ret = SPI_execute(sql, false, 0);

    SPI_finish();
    PopActiveSnapshot();

    if (ret < 0)
    {
        elog(ERROR, "SPI_execute failed: %d", ret);
        AbortCurrentTransaction();
    }
    else
    {
        elog(LOG, "SQL command executed successfully");
        CommitTransactionCommand();
    }
}

// -------------------------

static void
do_analyze(Oid relOid, RangeVar *relvar, List *va_cols)
{
#if PG_VERSION_NUM >= 160000
    MemoryContext vac_context;
#endif
    VacuumParams params;
    VacuumRelation *rel;

    params.options = VACOPT_ANALYZE;
    params.freeze_min_age = -1;
    params.freeze_table_age = -1;
    params.multixact_freeze_min_age = -1;
    params.multixact_freeze_table_age = -1;
    params.is_wraparound = false;
    params.log_min_duration = -1;

    rel = makeNode(VacuumRelation);
    rel->relation = relvar;
    rel->oid = relOid;
    rel->va_cols = va_cols;
#if PG_VERSION_NUM >= 160000
    vac_context = AllocSetContextCreate(CurrentMemoryContext, "Vacuum", ALLOCSET_DEFAULT_SIZES);
    vacuum(list_make1(rel), &params,
           GetAccessStrategy(BAS_VACUUM), vac_context, false);

    MemoryContextDelete(vac_context);
#else
    vacuum(list_make1(rel), &params,
           GetAccessStrategy(BAS_VACUUM), false);
#endif
}

static void HandleStatAdvisorCommand(Oid table_id, Bitmapset *columns)
{
    int             bit = -1;
    char            *rel_name,
                    *rel_namespace,
                    *create_stat_stmt = "";
    List            *def_names = NIL;
    List            *va_cols = NIL;
    RangeVar        *rel;
    CreateStatsStmt *stats;

    elog(LOG, "Received Stat Advisor command");

    StartTransactionCommand();

    while ((bit = bms_next_member(columns, bit)) >= 0)
    {
        char        *colname;
        StatsElem   *selem;

        colname = get_attname(table_id, bit, false);
        selem = makeNode(StatsElem);

        selem->name = colname;
        selem->expr = NULL;

        def_names = lappend(def_names, selem);
        va_cols = lappend(va_cols, makeString(pstrdup(colname)));
    }

    rel_name = get_rel_name(table_id);
    rel_namespace = get_namespace_name(get_rel_namespace(table_id));
    rel = makeRangeVar(rel_namespace, rel_name, 0);

    stats = makeNode(CreateStatsStmt);
    stats->defnames = NULL;
    stats->stat_types = lappend(stats->stat_types, makeString("ndistinct"));
    stats->stat_types = lappend(stats->stat_types, makeString("dependencies"));
    stats->stat_types = lappend(stats->stat_types, makeString("mcv"));
    stats->if_not_exists = true;
    stats->relations = list_make1(rel);
    stats->exprs = def_names;
    stats->stxcomment = NULL;
    stats->transformed = true; /* don't need transformStatsStmt again */

    CreateStatistics(stats);

    CommandCounterIncrement();

    do_analyze(table_id, rel, va_cols);

    elog(LOG, "pg_stat_advisor: successfully created extended statistics %s from %s.%s", create_stat_stmt, rel_namespace, rel_name);
    CommitTransactionCommand();
}
