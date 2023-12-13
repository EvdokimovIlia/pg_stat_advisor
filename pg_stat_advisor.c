/*-------------------------------------------------------------------------
 *
 * pg_stat_advisor.c
 *
 *
 * Copyright (c) 2008-2023, PostgreSQL Global Development Group
 *
 * IDENTIFICATION
 *	  contrib/pg_stat_advisor/pg_stat_advisor.c
 *
 *-------------------------------------------------------------------------
 */
#include "postgres.h"

#include "access/hash.h"
#include "access/table.h"
#include "access/tableam.h"
#include "catalog/pg_statistic_ext.h"
#include "commands/explain.h"
#include "common/pg_prng.h"
#include "nodes/params.h"
#include "optimizer/optimizer.h"
#include "parser/parsetree.h"
#include "statistics/statistics.h"
#include "utils/fmgroids.h"
#include "utils/guc.h"
#include "utils/lsyscache.h"

PG_MODULE_MAGIC;

/* GUC variables */
static int	  pg_stat_advisor_log_min_duration = -1; /* msec or -1 */
static bool   pg_stat_advisor_log_analyze = false;
static bool   pg_stat_advisor_log_buffers = false;
static bool   pg_stat_advisor_log_wal = false;
static bool   pg_stat_advisor_log_timing = true;
static bool   pg_stat_advisor_log_nested_statements = false;
static double pg_stat_advisor_sample_rate = 1;
static double pg_stat_advisor_add_statistics_threshold = 0.0;


/* Current nesting depth of ExecutorRun calls */
static int	nesting_level = 0;

/* Is the current top-level query to be sampled? */
static bool current_query_sampled = false;

#define pg_stat_advisor_enabled() \
	(pg_stat_advisor_log_min_duration >= 0 && \
	 (nesting_level == 0 || pg_stat_advisor_log_nested_statements) && \
	 current_query_sampled)

/* Saved hook values in case of unload */
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

static void AddMultiColumnStatisticsForNode(PlanState *planstate, ExplainState *es);

/*
 * Module load callback
 */
void
_PG_init(void)
{
	/* Define custom GUC variables. */
	DefineCustomIntVariable("pg_stat_advisor.log_min_duration",
							"Sets the minimum execution time above which plans will be logged.",
							"Zero prints all plans. -1 turns this feature off.",
							&pg_stat_advisor_log_min_duration,
							-1,
							-1, INT_MAX,
							PGC_SUSET,
							GUC_UNIT_MS,
							NULL,
							NULL,
							NULL);

	DefineCustomBoolVariable("pg_stat_advisor.log_analyze",
							 "Use EXPLAIN ANALYZE for plan logging.",
							 NULL,
							 &pg_stat_advisor_log_analyze,
							 false,
							 PGC_SUSET,
							 0,
							 NULL,
							 NULL,
							 NULL);

	DefineCustomBoolVariable("pg_stat_advisor.log_buffers",
							 "Log buffers usage.",
							 NULL,
							 &pg_stat_advisor_log_buffers,
							 false,
							 PGC_SUSET,
							 0,
							 NULL,
							 NULL,
							 NULL);

	DefineCustomBoolVariable("pg_stat_advisor.log_wal",
							 "Log WAL usage.",
							 NULL,
							 &pg_stat_advisor_log_wal,
							 false,
							 PGC_SUSET,
							 0,
							 NULL,
							 NULL,
							 NULL);

	DefineCustomBoolVariable("pg_stat_advisor.log_nested_statements",
							 "Log nested statements.",
							 NULL,
							 &pg_stat_advisor_log_nested_statements,
							 false,
							 PGC_SUSET,
							 0,
							 NULL,
							 NULL,
							 NULL);

	DefineCustomBoolVariable("pg_stat_advisor.log_timing",
							 "Collect timing data, not just row counts.",
							 NULL,
							 &pg_stat_advisor_log_timing,
							 true,
							 PGC_SUSET,
							 0,
							 NULL,
							 NULL,
							 NULL);

	DefineCustomRealVariable("pg_stat_advisor.sample_rate",
							 "Fraction of queries to process.",
							 NULL,
							 &pg_stat_advisor_sample_rate,
							 1.0,
							 0.0,
							 1.0,
							 PGC_SUSET,
							 0,
							 NULL,
							 NULL,
							 NULL);

	DefineCustomRealVariable("pg_stat_advisor.add_statistics_threshold",
							 "Sets the threshold for actual/estimated #rows ratio triggering creation of multicolumn statistic for the related columns.",
							 "Zero disables implicit creation of multicolumn statistic.",
							 &pg_stat_advisor_add_statistics_threshold,
							 0.0,
							 0.0,
							 INT_MAX,
							 PGC_SUSET,
							 0,
							 NULL,
							 NULL,
							 NULL);

	MarkGUCPrefixReserved("pg_stat_advisor");

	/* Install hooks. */
    prev_ExecutorStart = ExecutorStart_hook;
	ExecutorStart_hook = explain_ExecutorStart;
	prev_ExecutorRun = ExecutorRun_hook;
	ExecutorRun_hook = explain_ExecutorRun;
	prev_ExecutorFinish = ExecutorFinish_hook;
	ExecutorFinish_hook = explain_ExecutorFinish;
	prev_ExecutorEnd = ExecutorEnd_hook;
	ExecutorEnd_hook = explain_ExecutorEnd;
}

/*
 * ExecutorStart hook: start up logging if needed
 */
static void
explain_ExecutorStart(QueryDesc *queryDesc, int eflags)
{
	/*
	 * At the beginning of each top-level statement, decide whether we'll
	 * sample this statement.  If nested-statement explaining is enabled,
	 * either all nested statements will be explained or none will.
	 *
	 * When in a parallel worker, we should do nothing, which we can implement
	 * cheaply by pretending we decided not to sample the current statement.
	 * If EXPLAIN is active in the parent session, data will be collected and
	 * reported back to the parent, and it's no business of ours to interfere.
	 */
	if (nesting_level == 0)
	{
		if (pg_stat_advisor_log_min_duration >= 0 && !IsParallelWorker())
			current_query_sampled = (pg_prng_double(&pg_global_prng_state) < pg_stat_advisor_sample_rate);
		else
			current_query_sampled = false;
	}

	if (pg_stat_advisor_enabled())
	{
		/* Enable per-node instrumentation iff log_analyze is required. */
		if (pg_stat_advisor_log_analyze && (eflags & EXEC_FLAG_EXPLAIN_ONLY) == 0)
		{
			if (pg_stat_advisor_log_timing)
				queryDesc->instrument_options |= INSTRUMENT_TIMER;
			else
				queryDesc->instrument_options |= INSTRUMENT_ROWS;
			if (pg_stat_advisor_log_buffers)
				queryDesc->instrument_options |= INSTRUMENT_BUFFERS;
			if (pg_stat_advisor_log_wal)
				queryDesc->instrument_options |= INSTRUMENT_WAL;
		}
	}

	if (prev_ExecutorStart)
		prev_ExecutorStart(queryDesc, eflags);
	else
		standard_ExecutorStart(queryDesc, eflags);

	if (pg_stat_advisor_enabled())
	{
		/*
		 * Set up to track total elapsed time in ExecutorRun.  Make sure the
		 * space is allocated in the per-query context so it will go away at
		 * ExecutorEnd.
		 */
		if (queryDesc->totaltime == NULL)
		{
			MemoryContext oldcxt;

			oldcxt = MemoryContextSwitchTo(queryDesc->estate->es_query_cxt);
			queryDesc->totaltime = InstrAlloc(1, INSTRUMENT_ALL, false);
			MemoryContextSwitchTo(oldcxt);
		}
	}
}

/*
 * ExecutorRun hook: all we need do is track nesting depth
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
 * ExecutorFinish hook: all we need do is track nesting depth
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

/**
 * Try to add multicolumn statistics for specified subplans.
 */
static void
AddMultiColumnStatisticsForSubPlans(List *plans, ExplainState *es)
{
	ListCell   *lst;

	foreach(lst, plans)
	{
		SubPlanState *sps = (SubPlanState *) lfirst(lst);

		AddMultiColumnStatisticsForNode(sps->planstate, es);
	}
}

/**
 * Try to add multicolumn statistics for plan subnodes.
 */
static void
AddMultiColumnStatisticsForMemberNodes(PlanState **planstates, int nsubnodes,
									   ExplainState *es)
{
	int			j;

	for (j = 0; j < nsubnodes; j++)
		AddMultiColumnStatisticsForNode(planstates[j], es);
}

/**
 * Comparator used to sort Vars by name
 */
static int
vars_list_comparator(const ListCell *a, const ListCell *b)
{
	char* va = strVal(linitial(((ColumnRef *)lfirst(a))->fields));
	char* vb = strVal(linitial(((ColumnRef *)lfirst(b))->fields));
	return strcmp(va, vb);
}

/**
 * Try to add multicolumn statistics for qual
 */
static void
AddMultiColumnStatisticsForQual(List* qual, ExplainState *es)
{
	List *vars = NULL;
	ListCell* lc;

	/* Extract vars from all quals */
	foreach (lc, qual)
	{
		Node* node = (Node*)lfirst(lc);
		if (IsA(node, RestrictInfo))
			node = (Node*)((RestrictInfo*)node)->clause;
		vars = list_concat(vars, pull_vars_of_level(node, 0));
	}

	/* Loop until we considered all vars */
	while (vars != NULL)
	{
		ListCell *cell;
		List *cols = NULL;
	Index relno = 0;
		Bitmapset* colmap = NULL;

		/* Contruct list of unique vars */
		foreach (cell, vars)
		{
			Node* node = (Node *) lfirst(cell);
			if (IsA(node, Var))
			{
				Var *var = (Var *) node;
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
						RangeTblEntry *rte = rt_fetch(varno, es->rtable);
						if (rte->rtekind == RTE_RELATION)
						{
							ColumnRef  *col = makeNode(ColumnRef);
							char *colname = get_rte_attribute_name(rte, varattno);
							col->fields = list_make1(makeString(colname));
							cols = lappend(cols, col);
							colmap = bms_add_member(colmap, varattno);
						}
					}
				}
				else
				{
					continue;
				}
			}
			vars = foreach_delete_current(vars, cell);
		}
		/* To create multicolumn statitics we need to have at least 2 columns */
		if (list_length(cols) >= 2)
		{
			RangeTblEntry *rte = rt_fetch(relno, es->rtable);
			char *rel_name = get_rel_name(rte->relid);
			char* stat_name = rel_name;
			char* create_stat_stmt = (char*)"";
			char const* sep = "ON";
			ScanKeyData entry[2];
			TableScanDesc scan;
			Relation stat_rel;
			size_t name_len;
			TupleTableSlot *slot;

			/* Sort variables by name */
			list_sort(cols, vars_list_comparator);

			/* Construct name for statistic by concatenating relation name with all columns */
			foreach (cell, cols)
			{
				char* col_name = strVal(linitial(((ColumnRef *)lfirst(cell))->fields));
				stat_name = psprintf("%s_%s", stat_name, col_name);
				create_stat_stmt = psprintf("%s%s %s", create_stat_stmt, sep, col_name);
				sep = ",";
			}

			name_len = strlen(stat_name);
			/* Truncate name if it doesn't fit in NameData */
			if (name_len >= NAMEDATALEN)
				stat_name = psprintf("%.*s_%08x", NAMEDATALEN - 10, stat_name, (unsigned)hash_any((uint8*)stat_name, name_len));

			ScanKeyInit(&entry[0],
						Anum_pg_statistic_ext_stxname,
						BTEqualStrategyNumber, F_NAMEEQ,
						CStringGetDatum(stat_name));
			ScanKeyInit(&entry[1],
						Anum_pg_statistic_ext_stxnamespace,
						BTEqualStrategyNumber, F_OIDEQ,
						ObjectIdGetDatum(get_rel_namespace(rte->relid)));

			/*
			 * Prevent concurrent access to extended statistic table
			 */
			stat_rel = table_open(StatisticExtRelationId, AccessExclusiveLock);
			slot = table_slot_create(stat_rel, NULL);
			scan = table_beginscan_catalog(stat_rel, 2, entry);

			/*
			 * Check if multicolumn statistic object with such name already exists.
			 * Most likely if was already created by auto_explain, but either ANALYZE was not performed since
			 * this time, either presence of this multicolumn statistic doesn't help to provide more precise estimation.
			 * Despite to the fact that we create statistics with "if_not_exist" option, presence of such check
			 * allows to eliminate notice message that statistics object already exists.
			 */
			if (!table_scan_getnextslot(scan, ForwardScanDirection, slot))
			{
                ereport(NOTICE, (errmsg("pg_stat_advisor suggestion: CREATE STATISTICS %s %s FROM %s",
                                        stat_name, create_stat_stmt, rel_name),
                                    errhidestmt(true)));
			}
			table_endscan(scan);
			ExecDropSingleTupleTableSlot(slot);
			table_close(stat_rel, AccessExclusiveLock);
		}
	}
}

/**
 * Try to add multicolumn statistics for node
 */
static void
AddMultiColumnStatisticsForNode(PlanState *planstate, ExplainState *es)
{
	Plan	   *plan = planstate->plan;

	if (planstate->instrument && plan->plan_rows != 0)
	{
		if (pg_stat_advisor_add_statistics_threshold > 0
			&& planstate->instrument->ntuples / plan->plan_rows >= pg_stat_advisor_add_statistics_threshold)
		{
			elog(DEBUG1, "Estimated=%f, actual=%f, error=%f: plan=%s", plan->plan_rows, planstate->instrument->ntuples, planstate->instrument->ntuples / plan->plan_rows, nodeToString(plan));
			/* quals, sort keys, etc */
			switch (nodeTag(plan))
			{
			  case T_IndexScan:
				AddMultiColumnStatisticsForQual(((IndexScan *) plan)->indexqualorig, es);
				break;
			  case T_IndexOnlyScan:
				AddMultiColumnStatisticsForQual(((IndexOnlyScan *) plan)->indexqual, es);
				break;
			  case T_BitmapIndexScan:
				AddMultiColumnStatisticsForQual(((BitmapIndexScan *) plan)->indexqualorig, es);
				break;
			  case T_NestLoop:
				AddMultiColumnStatisticsForQual(((NestLoop *) plan)->join.joinqual, es);
				break;
			  case T_MergeJoin:
				AddMultiColumnStatisticsForQual(((MergeJoin *) plan)->mergeclauses, es);
				AddMultiColumnStatisticsForQual(((MergeJoin *) plan)->join.joinqual, es);
				break;
			  case T_HashJoin:
				AddMultiColumnStatisticsForQual(((HashJoin *) plan)->hashclauses, es);
				AddMultiColumnStatisticsForQual(((HashJoin *) plan)->join.joinqual, es);
				break;
			  default:
				break;
			}
			AddMultiColumnStatisticsForQual(plan->qual, es);
		}
	}

	/* initPlan-s */
	if (planstate->initPlan)
		AddMultiColumnStatisticsForSubPlans(planstate->initPlan, es);

	/* lefttree */
	if (outerPlanState(planstate))
		AddMultiColumnStatisticsForNode(outerPlanState(planstate), es);

	/* righttree */
	if (innerPlanState(planstate))
		AddMultiColumnStatisticsForNode(innerPlanState(planstate), es);

	/* special child plans */
switch (nodeTag(plan))
	{
        // TODO:
		// case T_ModifyTable:
		// 	AddMultiColumnStatisticsForMemberNodes(((ModifyTableState *) planstate)->mt_plans,
		// 										   ((ModifyTableState *) planstate)->mt_nplans,
		// 										   es);
		// 	break;
		case T_Append:
			AddMultiColumnStatisticsForMemberNodes(((AppendState *) planstate)->appendplans,
												   ((AppendState *) planstate)->as_nplans,
												   es);
			break;
		case T_MergeAppend:
			AddMultiColumnStatisticsForMemberNodes(((MergeAppendState *) planstate)->mergeplans,
												   ((MergeAppendState *) planstate)->ms_nplans,
												   es);
			break;
		case T_BitmapAnd:
			AddMultiColumnStatisticsForMemberNodes(((BitmapAndState *) planstate)->bitmapplans,
												   ((BitmapAndState *) planstate)->nplans,
												   es);
			break;
		case T_BitmapOr:
			AddMultiColumnStatisticsForMemberNodes(((BitmapOrState *) planstate)->bitmapplans,
												   ((BitmapOrState *) planstate)->nplans,
												   es);
			break;
		case T_SubqueryScan:
			AddMultiColumnStatisticsForNode(((SubqueryScanState *) planstate)->subplan, es);
			break;
		default:
			break;
	}
}

/*
 * ExecutorEnd hook: log results if needed
 */
static void
explain_ExecutorEnd(QueryDesc *queryDesc)
{
	if (queryDesc->totaltime && pg_stat_advisor_enabled())
	{
		MemoryContext oldcxt;
		double		msec;

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

		/* Log plan if duration is exceeded. */
		msec = queryDesc->totaltime->total * 1000.0;
		if (msec >= pg_stat_advisor_log_min_duration)
		{
			ExplainState *es = NewExplainState();

			es->analyze = queryDesc->instrument_options;
			es->buffers = es->analyze;
			es->wal = es->analyze;
			es->timing = es->analyze;
			es->summary = es->analyze;

			ExplainBeginOutput(es);
			ExplainQueryText(es, queryDesc);
			ExplainQueryParameters(es, queryDesc->params, -1);
			ExplainPrintPlan(es, queryDesc);
			if (es->analyze)
				ExplainPrintTriggers(es, queryDesc);
			if (es->costs)
				ExplainPrintJITSummary(es, queryDesc);
			ExplainEndOutput(es);

			/* Add multicolumn statistic if requested */
			if (pg_stat_advisor_add_statistics_threshold && !IsParallelWorker())
				AddMultiColumnStatisticsForNode(queryDesc->planstate, es);
		}

		MemoryContextSwitchTo(oldcxt);
	}

	if (prev_ExecutorEnd)
		prev_ExecutorEnd(queryDesc);
	else
		standard_ExecutorEnd(queryDesc);
}
