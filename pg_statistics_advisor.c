/*-------------------------------------------------------------------------
 *
 * pg_statistics_advisor.c
 *
 *
 * Copyright (c) 2008-2023, PostgreSQL Global Development Group
 *
 * IDENTIFICATION
 *	  contrib/pg_statistics_advisor/pg_statistics_advisor.c
 *
 *-------------------------------------------------------------------------
 */
#include "postgres.h"

#include <limits.h>
#include <math.h>

#include "access/hash.h"
#include "access/parallel.h"
#include "access/relscan.h"
#include "access/skey.h"
#include "access/table.h"
#include "access/tableam.h"
#include "catalog/pg_statistic_ext.h"
#include "commands/explain.h"
#include "commands/defrem.h"
#include "common/pg_prng.h"
#include "executor/instrument.h"
#include "jit/jit.h"
#include "nodes/makefuncs.h"
#include "nodes/nodeFuncs.h"
#include "optimizer/cost.h"
#include "optimizer/optimizer.h"
#include "optimizer/planmain.h"
#include "parser/parsetree.h"
#include "storage/ipc.h"
#include "statistics/statistics.h"
#include "utils/fmgroids.h"
#include "nodes/params.h"
#include "utils/guc.h"
#include "utils/syscache.h"
#include "utils/lsyscache.h"
#include "utils/ruleutils.h"

PG_MODULE_MAGIC;

/* GUC variables */
static bool pg_statistics_advisor_add_statistics_suggest_only = false;
static double pg_statistics_advisor_add_statistics_threshold = 0.0;


/* Saved hook values in case of unload */
static ExecutorEnd_hook_type prev_ExecutorEnd = NULL;

static void explain_ExecutorEnd(QueryDesc *queryDesc);

static void AddMultiColumnStatisticsForNode(PlanState *planstate, ExplainState *es);


/*
 * Module load callback
 */
void
_PG_init(void)
{
	/* Define custom GUC variables. */
	DefineCustomRealVariable("pg_statistics_advisor.add_statistics_threshold",
							 "Sets the threshold for actual/estimated #rows ratio triggering creation of multicolumn statistic for the related columns.",
							 "Zero disables implicit creation of multicolumn statistic.",
							 &pg_statistics_advisor_add_statistics_threshold,
							 0.0,
							 0.0,
							 INT_MAX,
							 PGC_SUSET,
							 0,
							 NULL,
							 NULL,
							 NULL);

	DefineCustomBoolVariable("pg_statistics_advisor.add_statistics_suggest_only",
							 "Do not create statistic but just record in WAL suggested create statistics statement.",
							 NULL,
							 &pg_statistics_advisor_add_statistics_suggest_only,
							 false,
							 PGC_SUSET,
							 0,
							 NULL,
							 NULL,
							 NULL);

	MarkGUCPrefixReserved("pg_statistics_advisor");

	/* Install hooks. */
	prev_ExecutorEnd = ExecutorEnd_hook;
	ExecutorEnd_hook = explain_ExecutorEnd;
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
			CreateStatsStmt* stats = makeNode(CreateStatsStmt);
			char *rel_namespace = get_namespace_name(get_rel_namespace(rte->relid));
			char *rel_name = get_rel_name(rte->relid);
			RangeVar* rel = makeRangeVar(rel_namespace, rel_name, 0);
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
				if (pg_statistics_advisor_add_statistics_suggest_only)
				{
					ereport(NOTICE, (errmsg("pg_statistics_advisor suggestion: CREATE STATISTICS %s %s FROM %s",
											stat_name, create_stat_stmt, rel_name),
									 errhidestmt(true)));
				}
				else
				{
					ereport(LOG, (errmsg("Add statistics %s", stat_name),
								  errhidestmt(true)));
					stats->defnames = list_make2(makeString(rel_namespace), makeString(stat_name));
					stats->if_not_exists = true;
					stats->relations = list_make1(rel);
					stats->exprs = cols;
					CreateStatistics(stats);
				}
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
		if (pg_statistics_advisor_add_statistics_threshold != 0
			&& planstate->instrument->ntuples / plan->plan_rows >= pg_statistics_advisor_add_statistics_threshold)
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
		// case T_ModifyTable:
		// 	AddMultiColumnStatisticsForMemberNodes(((ModifyTableState *) planstate)->appendplans,
		// 										   ((ModifyTableState *) planstate)->mt_nrels,
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
	if (queryDesc->totaltime)
	{
		MemoryContext oldcxt;

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

        ExplainState *es = NewExplainState();

        es->analyze = (queryDesc->instrument_options);
        es->buffers = (es->analyze);
        es->wal = (es->analyze);
        es->timing = (es->analyze);
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
        if (pg_statistics_advisor_add_statistics_threshold && !IsParallelWorker())
            AddMultiColumnStatisticsForNode(queryDesc->planstate, es);

        /* Remove last line break */
        if (es->str->len > 0 && es->str->data[es->str->len - 1] == '\n')
            es->str->data[--es->str->len] = '\0';

		MemoryContextSwitchTo(oldcxt);
	}
}