#include "postgres.h"

#include "include/pg_stat_advisor_bgw.h"

#include "access/hash.h"
#include "access/table.h"
#include "catalog/pg_statistic_ext.h"
#include "commands/explain.h"
#include "commands/defrem.h"
#include "executor/spi.h"
#include "optimizer/optimizer.h"
#include "parser/parsetree.h"
#include "statistics/statistics.h"
#include "statistics/extended_stats_internal.h"
#include "utils/guc.h"
#include "utils/fmgroids.h"
#include "utils/syscache.h"
#include "utils/lsyscache.h"
#include "utils/pgstat_internal.h"
#include "nodes/primnodes.h"
#include "nodes/nodeFuncs.h"
#include "nodes/makefuncs.h"
#include "storage/ipc.h"

PG_MODULE_MAGIC;

/*---- Local variables ----*/

/* Current nesting depth of planner/ExecutorRun/ProcessUtility calls */
static int nesting_level = 0;
static volatile sig_atomic_t shutdown_requested = false;

/*---- GUC variables ----*/
static double pg_stat_advisor_suggest_statistics_threshold = 0.0;
extern int ring_buffer_capacity;

#define pg_stat_advisor_enabled(level) \
		(!IsParallelWorker() && ((level) == 0) && \
		(pg_stat_advisor_suggest_statistics_threshold > 0.0))

/*---- Function declarations ----*/

/* Saved hook values in case of unload */
shmem_request_hook_type prev_shmem_request_hook = NULL;
shmem_startup_hook_type prev_shmem_startup_hook = NULL;
static ExecutorStart_hook_type prev_ExecutorStart = NULL;
static ExecutorRun_hook_type prev_ExecutorRun = NULL;
static ExecutorFinish_hook_type prev_ExecutorFinish = NULL;
static ExecutorEnd_hook_type prev_ExecutorEnd = NULL;

static void explain_ExecutorStart(QueryDesc *queryDesc, int eflags);
static void explain_ExecutorRun(QueryDesc *queryDesc,
								ScanDirection direction,
								uint64 count, bool execute_once);
static void explain_ExecutorFinish(QueryDesc *queryDesc);
static void explain_ExecutorEnd(QueryDesc *queryDesc);
static void SuggestMultiColumnStatisticsForQual(List *qual, ExplainState *es);
static void SuggestMultiColumnStatisticsForSubPlans(List *plans, ExplainState *es);
static void SuggestMultiColumnStatisticsForNode(PlanState *planstate, ExplainState *es);
static void SuggestMultiColumnStatisticsForMemberNodes(PlanState **planstates,
														int nsubnodes,
														ExplainState *es);

/*
 * Module load callback
 */
void _PG_init(void)
{
	/* Define custom GUC variables. */
	DefineCustomRealVariable("pg_stat_advisor.suggest_statistics_threshold",
							 "Set the threshold for estimated/actual rows",
							 "0 disables suggestion of creating statistics",
							 &pg_stat_advisor_suggest_statistics_threshold,
							 0.0,
							 0.0,
							 1.0,
							 PGC_SUSET,
							 0,
							 NULL,
							 NULL,
							 NULL);

	DefineCustomIntVariable("pg_stat_advisor.ring_buffer_capacity",
							"Set the size for global queue ring buffer",
							NULL,
							&ring_buffer_capacity,
							10240,
							1024,
							INT_MAX,
							PGC_POSTMASTER,
							0,
							NULL,
							NULL,
							NULL);

	MarkGUCPrefixReserved("pg_stat_advisor");

	/* Install hooks. */
	prev_shmem_request_hook = shmem_request_hook;
	shmem_request_hook = bgtm_shmem_request;
	prev_shmem_startup_hook = shmem_startup_hook;
	shmem_startup_hook = bgtm_shmem_startup;
	prev_ExecutorStart = ExecutorStart_hook;
	ExecutorStart_hook = explain_ExecutorStart;
	prev_ExecutorRun = ExecutorRun_hook;
	ExecutorRun_hook = explain_ExecutorRun;
	prev_ExecutorFinish = ExecutorFinish_hook;
	ExecutorFinish_hook = explain_ExecutorFinish;
	prev_ExecutorEnd = ExecutorEnd_hook;
	ExecutorEnd_hook = explain_ExecutorEnd;

	/* Register background worker */
	RegisterBackgroundTaskManager();
}

/*
 * ExecutorStart hook: start up suggesting if needed
 */
static void
explain_ExecutorStart(QueryDesc *queryDesc, int eflags)
{
	if (prev_ExecutorStart)
		prev_ExecutorStart(queryDesc, eflags);
	else
		standard_ExecutorStart(queryDesc, eflags);

	if (pg_stat_advisor_enabled(nesting_level) && queryDesc->totaltime == NULL)
	{
		MemoryContext oldcxt;

		oldcxt = MemoryContextSwitchTo(queryDesc->estate->es_query_cxt);
		queryDesc->totaltime = InstrAlloc(1, INSTRUMENT_ALL, false);
		MemoryContextSwitchTo(oldcxt);
	}
}

/*
 * ExecutorRun hook: all we need do is suggest nesting depth
 */
static void
explain_ExecutorRun(QueryDesc *queryDesc, ScanDirection direction,
					uint64 count, bool execute_once)
{
	nesting_level++;
	PG_TRY();
	{
		if (prev_ExecutorRun)
			prev_ExecutorRun(queryDesc, direction, count, execute_once);
		else
			standard_ExecutorRun(queryDesc, direction, count, execute_once);
	}
	PG_FINALLY();
	{
		nesting_level--;
	}
	PG_END_TRY();
}

/*
 * ExecutorFinish hook: all we need do is suggest nesting depth
 */
static void
explain_ExecutorFinish(QueryDesc *queryDesc)
{
	nesting_level++;
	PG_TRY();
	{
		if (prev_ExecutorFinish)
			prev_ExecutorFinish(queryDesc);
		else
			standard_ExecutorFinish(queryDesc);
	}
	PG_FINALLY();
	{
		nesting_level--;
	}
	PG_END_TRY();
}

/*
 * ExecutorEnd hook: suggest extended statsitics if needed
 */
static void
explain_ExecutorEnd(QueryDesc *queryDesc)
{
	if (queryDesc->totaltime && pg_stat_advisor_enabled(nesting_level))
	{
		MemoryContext oldcxt;
		ExplainState *es;
		PlannedStmt	*pstmt = NULL;

		/*
		 * Make sure we operate in the per-query context, so any cruft will be
		 * discarded later during ExecutorEnd.
		 */
		oldcxt = MemoryContextSwitchTo(queryDesc->estate->es_query_cxt);

		/*
		 * Make sure stats accumulation is done.  (Note: it's okay if several
		 * levels of hook all do this.)
		 */
		InstrEndLoop(queryDesc->totaltime);

		es = NewExplainState();

		es->analyze = queryDesc->instrument_options;

		ExplainBeginOutput(es);
		ExplainQueryText(es, queryDesc);
		ExplainPrintPlan(es, queryDesc);
		if (es->analyze)
			ExplainPrintTriggers(es, queryDesc);
		if (es->costs)
			ExplainPrintJITSummary(es, queryDesc);
		ExplainEndOutput(es);

		pstmt = queryDesc->plannedstmt;

		/*
		* When in a parallel worker, we should do nothing, which we can implement
		* cheaply by pretending we decided not to suggest the current statement.
		* If EXPLAIN is active in the parent session, data will be collected and
		* reported back to the parent, and it's no business of ours to interfere.
		*/
		if (!IsParallelWorker() && pstmt->commandType != CMD_UPDATE &&
			pstmt->commandType != CMD_DELETE)
			SuggestMultiColumnStatisticsForNode(queryDesc->planstate, es);

		MemoryContextSwitchTo(oldcxt);
	}

	if (prev_ExecutorEnd)
		prev_ExecutorEnd(queryDesc);
	else
		standard_ExecutorEnd(queryDesc);
}

/*
 * Suggest statistics for qual
 */
static void
SuggestMultiColumnStatisticsForQual(List *qual, ExplainState *es)
{
	List		   *vars = NULL;
	ListCell	   *l;
	ListCell	   *cell;
	List 		   *cols = NULL;
	Index 			relno = 0;
	Bitmapset 	   *colmap = NULL;
	RangeTblEntry  *rte = NULL;
	List            *statistics = NULL;
	Relation        pg_stextdata;
	StringInfo		command_data;
    ListCell   *oid;
	char		   *nspName;
	int i = 0;

	foreach (l, qual)
	{
		Node *clause = (Node *)lfirst(l);
		vars = list_concat(vars, pull_vars_of_level(clause, 0));
	}

	/* Contruct list of unique vars.
	 * 
	 * Vars must be from one table. There are no nessecary to create statistics on
	 * temporary tables due to temporary existing.
	 */
	foreach (cell, vars)
	{
		Node *node = (Node *)lfirst(cell);

		if (IsA(node, Var))
		{
			Var *var = (Var *)node;
			int varno = IS_SPECIAL_VARNO(var->varno) ? var->varnosyn : var->varno;

			if (cols == NULL || varno == relno)
			{
				int varattno = IS_SPECIAL_VARNO(var->varno) ? var->varattnosyn : var->varattno;

				relno = varno;
				if (var->varattno > 0 &&
					!bms_is_member(varattno, colmap) &&
					varno >= 1 && /* not synthetic var */
					varno <= list_length(es->rtable) &&
					list_length(cols) < STATS_MAX_DIMENSIONS)
				{
					rte = rt_fetch(varno, es->rtable);
					nspName = get_namespace_name(get_rel_namespace(rte->relid));

					if (rte->rtekind == RTE_RELATION)
					{
						if (nspName && strncmp(nspName, "pg_temp", 7) != 0)
						{
							ColumnRef *col = makeNode(ColumnRef);
							char *colname = get_rte_attribute_name(rte, varattno);

							col->fields = list_make1(makeString(colname));
							cols = lappend(cols, col);
							colmap = bms_add_member(colmap, varattno);
						}
						else
						{
							bms_free(colmap);
							list_free(cols);

							return;
						}
					}
				}
			}
			else
				continue;
		}
		vars = foreach_delete_current(vars, cell);
	}

	list_free(vars);
	list_free(cols);

	/* To suggest statitics we need to have from 2 to 8 columns */
	if (relno < 1 || relno > list_length(es->rtable) ||
		bms_num_members(colmap) < 2 || bms_num_members(colmap) > STATS_MAX_DIMENSIONS)
	{
		bms_free(colmap);
		return;
	}

	pg_stextdata = table_open(rte->relid, ShareUpdateExclusiveLock);
	if (pg_stextdata->rd_rel->relpersistence == RELPERSISTENCE_TEMP ||
		pg_stextdata->rd_rel->relpersistence == RELKIND_PARTITIONED_TABLE)
    {
        table_close(pg_stextdata, ShareUpdateExclusiveLock);
		bms_free(colmap);
        return;
    }

    for (i = 0; i < RelationGetNumberOfAttributes(pg_stextdata); i++)
    {
 		HeapTuple	tp;
		Form_pg_statistic stat;

 		tp = SearchSysCache3(STATRELATTINH,
 						 ObjectIdGetDatum(rte->relid), i + 1,
 						 BoolGetDatum(false));

 		if (!HeapTupleIsValid(tp))
 		{
			char *rel_name = get_rel_name(rte->relid);

			ereport(NOTICE, (errmsg("pg_stat_advisor suggestion: 'ANALYZE %s'", rel_name),
														errhidestmt(true)));

            table_close(pg_stextdata, ShareUpdateExclusiveLock);
			bms_free(colmap);
 			return;
     	}

		/* If ndistinc of column is 1, do not create extended statitics. */
		stat = (Form_pg_statistic) GETSTRUCT(tp);
		if (bms_is_member(i + 1, colmap) && stat->stadistinct == 1.0)
			colmap = bms_del_member(colmap, i + 1);

		if (bms_num_members(colmap) < 2 || bms_num_members(colmap) > STATS_MAX_DIMENSIONS)
		{
			ReleaseSysCache(tp);
			table_close(pg_stextdata, ShareUpdateExclusiveLock);
			bms_free(colmap);
			return;
		}

 		ReleaseSysCache(tp);
 	}

    statistics = RelationGetStatExtList(pg_stextdata);
    foreach(oid, statistics)
	{
		Oid			statOid = lfirst_oid(oid);
		Form_pg_statistic_ext staForm;
		HeapTuple	htup;
		Bitmapset  *keys = NULL;

		/* Read from pg_statistic_ext */
		htup = SearchSysCache1(STATEXTOID, ObjectIdGetDatum(statOid));
		if (!HeapTupleIsValid(htup))
			elog(ERROR, "cache lookup failed for statistics object %u", statOid);
		staForm = (Form_pg_statistic_ext) GETSTRUCT(htup);

		for (i = 0; i < staForm->stxkeys.dim1; i++)
			keys = bms_add_member(keys, staForm->stxkeys.values[i]);

		/* If we have statistics for current columns */
		if (!bms_compare(keys, colmap))
		{
			ReleaseSysCache(htup);
			table_close(pg_stextdata, ShareUpdateExclusiveLock);
			bms_free(keys);
			bms_free(colmap);

			return;
		}

		ReleaseSysCache(htup);
		bms_free(keys);
	}

    table_close(pg_stextdata, ShareUpdateExclusiveLock);

	ereport(LOG, (errmsg("pg_stat_advisor: %s", es->str->data), errhidestmt(true)));

	command_data = CreateStatAdvisorCommand(MyDatabaseId, GetAuthenticatedUserId(), rte->relid, colmap);
	ExecuteCommandInBackground(command_data);

	pfree(command_data->data);
	pfree(command_data);

	bms_free(colmap);
}

/**
 * Suggest statistics for specified subplans.
 */
static void
SuggestMultiColumnStatisticsForSubPlans(List *plans, ExplainState *es)
{
	ListCell *lst;

	foreach (lst, plans)
	{
		SubPlanState *sps = (SubPlanState *)lfirst(lst);

		SuggestMultiColumnStatisticsForNode(sps->planstate, es);
	}
}

/**
 * Suggest statistics for plan subnodes.
 */
static void
SuggestMultiColumnStatisticsForMemberNodes(PlanState **planstates, int nsubnodes,
										   ExplainState *es)
{
	int j;

	for (j = 0; j < nsubnodes; j++)
		SuggestMultiColumnStatisticsForNode(planstates[j], es);
}

/**
 * Suggest statistics for node
 */
static void
SuggestMultiColumnStatisticsForNode(PlanState *planstate, ExplainState *es)
{
	Plan *plan = NULL;
	bool enabled = false;

	Assert(planstate != NULL);

	plan = planstate->plan;
	enabled = plan != NULL && planstate->instrument != NULL &&
			  planstate->instrument->ntuples != 0.0 && plan->plan_rows != 0.0;

	if (enabled)
	{
		/* For loops > 1 take the average rows. */
		if (planstate->instrument->nloops > 0.0)
		{
			double rows = planstate->instrument->ntuples / planstate->instrument->nloops;
			enabled = plan->plan_rows / rows &&
					  plan->plan_rows / rows < pg_stat_advisor_suggest_statistics_threshold;
		}
		else
		{
			enabled = plan->plan_rows < planstate->instrument->ntuples &&
				plan->plan_rows / planstate->instrument->ntuples < pg_stat_advisor_suggest_statistics_threshold;
		}
	}

	if (enabled)
	{
		elog(DEBUG1, "pg_stat_advisor: suggest statistics due to following parameters \
			 estimated=%f, actual=%f, ratio=%f",
			 plan->plan_rows,
			 planstate->instrument->ntuples,
			 plan->plan_rows / planstate->instrument->ntuples);
		/* quals, sort keys, etc */
		switch (nodeTag(plan))
		{
			case T_IndexScan:
				SuggestMultiColumnStatisticsForQual(((IndexScan *)plan)->indexqualorig, es);
				break;
			case T_IndexOnlyScan:
				SuggestMultiColumnStatisticsForQual(((IndexOnlyScan *)plan)->indexqual, es);
				break;
			case T_BitmapIndexScan:
				SuggestMultiColumnStatisticsForQual(((BitmapIndexScan *)plan)->indexqualorig, es);
				break;
			default:
				break;
		}
		SuggestMultiColumnStatisticsForQual(plan->qual, es);
	}

	/* initPlan-s */
	if (planstate->initPlan)
		SuggestMultiColumnStatisticsForSubPlans(planstate->initPlan, es);

	/* lefttree */
	if (outerPlanState(planstate))
		SuggestMultiColumnStatisticsForNode(outerPlanState(planstate), es);

	/* righttree */
	if (innerPlanState(planstate))
		SuggestMultiColumnStatisticsForNode(innerPlanState(planstate), es);

	/* special child plans */
	switch (nodeTag(plan))
	{
		case T_Append:
			SuggestMultiColumnStatisticsForMemberNodes(((AppendState *)planstate)->appendplans,
													((AppendState *)planstate)->as_nplans,
													es);
			break;
		case T_MergeAppend:
			SuggestMultiColumnStatisticsForMemberNodes(((MergeAppendState *)planstate)->mergeplans,
													((MergeAppendState *)planstate)->ms_nplans,
													es);
			break;
		case T_BitmapAnd:
			SuggestMultiColumnStatisticsForMemberNodes(((BitmapAndState *)planstate)->bitmapplans,
													((BitmapAndState *)planstate)->nplans,
													es);
			break;
		case T_BitmapOr:
			SuggestMultiColumnStatisticsForMemberNodes(((BitmapOrState *)planstate)->bitmapplans,
													((BitmapOrState *)planstate)->nplans,
													es);
			break;
		case T_SubqueryScan:
			SuggestMultiColumnStatisticsForNode(((SubqueryScanState *)planstate)->subplan, es);
			break;
		default:
			break;
	}
}
