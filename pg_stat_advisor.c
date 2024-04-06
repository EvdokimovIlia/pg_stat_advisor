#include "postgres.h"

#include "access/hash.h"
#include "access/table.h"
#include "catalog/pg_statistic_ext.h"
#include "commands/explain.h"
#include "executor/spi.h"
#include "optimizer/optimizer.h"
#include "parser/parsetree.h"
#include "statistics/statistics.h"
#include "utils/guc.h"
#include "utils/syscache.h"
#include "utils/lsyscache.h"

PG_MODULE_MAGIC;

/* GUC variables */
static double pg_stat_advisor_suggest_statistics_threshold = 1.0;
static double pg_stat_advisor_analyze_scale_factor = 0.0;

/* Current nesting depth of ExecutorRun calls */
static int	nesting_level = 0;

#define pg_stat_advisor_enabled() (nesting_level == 0)

void		_PG_init(void);

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

static void SuggestMultiColumnStatisticsForNode(PlanState *planstate, ExplainState *es);
bool UpdateStatistics(char *rel_name);

/*
 * ExecutorStart hook: start up logging if needed
 */
static void
explain_ExecutorStart(QueryDesc *queryDesc, int eflags)
{
	if (prev_ExecutorStart)
		prev_ExecutorStart(queryDesc, eflags);
	else
		standard_ExecutorStart(queryDesc, eflags);

	if (pg_stat_advisor_enabled() && queryDesc->totaltime == NULL)
	{
		MemoryContext oldcxt;

		oldcxt = MemoryContextSwitchTo(queryDesc->estate->es_query_cxt);
		queryDesc->totaltime = InstrAlloc(1, INSTRUMENT_ALL, false);
		MemoryContextSwitchTo(oldcxt);
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

/*
 * ExecutorEnd hook: log results if needed
 */
static void
explain_ExecutorEnd(QueryDesc *queryDesc)
{
	if (queryDesc->totaltime && pg_stat_advisor_enabled())
	{
		MemoryContext oldcxt;
		ExplainState *es;

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

		/* Entry to check */
		if (!IsParallelWorker())
			SuggestMultiColumnStatisticsForNode(queryDesc->planstate, es);

		MemoryContextSwitchTo(oldcxt);
	}

	if (prev_ExecutorEnd)
		prev_ExecutorEnd(queryDesc);
	else
		standard_ExecutorEnd(queryDesc);
}

/**
 * Comparator used to sort Vars by name
 */
static int
vars_list_comparator(const ListCell *a, const ListCell *b)
{
	char	   *va = strVal(linitial(((ColumnRef *) lfirst(a))->fields));
	char	   *vb = strVal(linitial(((ColumnRef *) lfirst(b))->fields));

	return strcmp(va, vb);
}

/**
 * Suggest statistics for specified subplans.
 */
static void
SuggestMultiColumnStatisticsForSubPlans(List *plans, ExplainState *es)
{
	ListCell   *lst;

	foreach(lst, plans)
	{
		SubPlanState *sps = (SubPlanState *) lfirst(lst);

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
	int			j;

	for (j = 0; j < nsubnodes; j++)
		SuggestMultiColumnStatisticsForNode(planstates[j], es);
}

/*
 * UpdateStatistics --- check entries in pg_statistic of rel_name
 */
bool
UpdateStatistics(char *rel_name)
{
    int ret, ntuples;
    bool result = false;
    char sqlcmd[256];

    if ((ret = SPI_connect()) < 0)
    {
        elog(ERROR, "SPI_connect failed: error code %d", ret);
    }

    snprintf(sqlcmd, sizeof(sqlcmd), "SELECT n_live_tup, n_mod_since_analyze FROM pg_stat_all_tables WHERE relname = '%s';", rel_name);

    ret = SPI_execute(sqlcmd, true, 0);
    if (ret != SPI_OK_SELECT)
        elog(ERROR, "SPI_execute failed: error code %d", ret);

    ntuples = SPI_processed;

    if (ntuples > 0)
    {
        SPITupleTable *tuptable = SPI_tuptable;
        TupleDesc tupdesc = SPI_tuptable->tupdesc;
        HeapTuple tuple = tuptable->vals[0];
        bool is_null;

		double n_live_tup          = (double)DatumGetInt64(SPI_getbinval(tuple, tupdesc, 1, &is_null));
        double n_mod_since_analyze = (double)DatumGetInt64(SPI_getbinval(tuple, tupdesc, 2, &is_null));

		/* NOTE: Criteria of suggestion's message about ANALYZE */
        if (1 - (n_live_tup - n_mod_since_analyze) / n_live_tup > pg_stat_advisor_analyze_scale_factor)
            result = true;
    }

    SPI_finish();

    return result;
}

/**
 * Find extended statistics from 'relation_oid' relation on colmap
 */
static bool
FindExtendedStatisticsOnVars(Oid *relation_oid, Bitmapset *colmap)
{
	bool		isExistExtendedStatistics = false;
	ListCell   *oid;

	/* Receive all extended statistics for current relation */
	Relation	pg_stextdata = table_open(*relation_oid, RowExclusiveLock);
	List	   *statistics = RelationGetStatExtList(pg_stextdata);

	foreach(oid, statistics)
	{
		Oid			statOid = lfirst_oid(oid);
		Form_pg_statistic_ext staForm;
		HeapTuple	htup;
		Bitmapset  *keys = NULL;
		int			i;

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
			isExistExtendedStatistics = true;

			elog(DEBUG1, "pg_stat_advisor: found statistics for '%s' relation - '%s'",
				get_rel_name(*relation_oid),
				get_rel_name(statOid));

			ReleaseSysCache(htup);
			bms_free(keys);

			break;
		}

		ReleaseSysCache(htup);
		bms_free(keys);
	}

	list_free(statistics);
	table_close(pg_stextdata, RowExclusiveLock);

	return isExistExtendedStatistics;
}

/**
 * Suggest statistics for qual
 */
static void
SuggestMultiColumnStatisticsForQual(List *qual, ExplainState *es)
{
	List	   *vars = NULL;
	ListCell   *lc;

	/* Extract vars from all quals */
	foreach(lc, qual)
	{
		Node	   *node = (Node *) lfirst(lc);

		if (IsA(node, RestrictInfo))
			node = (Node *) ((RestrictInfo *) node)->clause;
		vars = list_concat(vars, pull_vars_of_level(node, 0));
	}

	/* Loop until we considered all vars */
	while (vars != NULL)
	{
		ListCell   *cell;
		List	   *cols = NULL;
		Index		relno = 0;
		Bitmapset  *colmap = NULL;
		RangeTblEntry *rte;
		char	   *rel_name;

		/* Contruct list of unique vars */
		foreach(cell, vars)
		{
			Node	   *node = (Node *) lfirst(cell);

			if (IsA(node, Var))
			{
				Var		   *var = (Var *) node;
				int			varno = IS_SPECIAL_VARNO(var->varno) ? var->varnosyn : var->varno;

				if (cols == NULL || varno == relno)
				{
					int			varattno = IS_SPECIAL_VARNO(var->varno) ? var->varattnosyn : var->varattno;

					relno = varno;
					if (var->varattno > 0 &&
						!bms_is_member(varattno, colmap) &&
						varno >= 1 &&	/* not synthetic var */
						varno <= list_length(es->rtable) &&
						list_length(cols) < STATS_MAX_DIMENSIONS)
					{
						rte = rt_fetch(varno, es->rtable);

						if (rte->rtekind == RTE_RELATION)
						{
							ColumnRef  *col = makeNode(ColumnRef);
							char	   *colname = get_rte_attribute_name(rte, varattno);

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

		rte = rt_fetch(relno, es->rtable);
		rel_name = get_rel_name(rte->relid);
		if (UpdateStatistics(rel_name))
		{
			ereport(NOTICE, (errmsg("pg_stat_advisor suggestion: 'ANALYZE %s'", rel_name),
								errhidestmt(true)));

			bms_free(colmap);
			return;
		}

		/* To suggest statitics we need to have at least 2 columns */
		if (list_length(cols) >= 2)
		{
			char	   *stat_name = rel_name;
			char	   *create_stat_stmt = (char *) "";
			char const *sep = "ON";
			size_t		name_len;

			/* Sort variables by name */
			list_sort(cols, vars_list_comparator);

			/*
			 * Construct name for statistic by concatenating relation name
			 * with all columns
			 */
			foreach(cell, cols)
			{
				char	   *col_name = strVal(linitial(((ColumnRef *) lfirst(cell))->fields));

				stat_name = psprintf("%s_%s", stat_name, col_name);
				create_stat_stmt = psprintf("%s%s %s", create_stat_stmt, sep, col_name);
				sep = ",";
			}

			name_len = strlen(stat_name);
			/* Truncate name if it doesn't fit in NameData */
			if (name_len >= NAMEDATALEN)
				stat_name = psprintf("%.*s_%08x", NAMEDATALEN - 10, stat_name, (unsigned) hash_any((uint8 *) stat_name, name_len));

			if (!FindExtendedStatisticsOnVars(&rte->relid, colmap))
			{
				ereport(NOTICE, (errmsg("pg_stat_advisor suggestion: CREATE STATISTICS %s %s FROM %s",
										stat_name, create_stat_stmt, rel_name),
								 errhidestmt(true)));
			}
		}

		bms_free(colmap);
	}
}

/**
 * Suggest statistics for node
 */
static void
SuggestMultiColumnStatisticsForNode(PlanState *planstate, ExplainState *es)
{
	Plan	   *plan = planstate->plan;

	/* NOTE: Criteria of suggestion's message about CREATE STATISTICS */
	if (planstate->instrument && plan->plan_rows != 0 &&
		plan->plan_rows / planstate->instrument->ntuples < pg_stat_advisor_suggest_statistics_threshold)
	{
		elog(DEBUG1, "pg_stat_advisor: suggest statistics due to following parameters \
			 estimated=%f, actual=%f, ratio=%f: plan=%s",
			 plan->plan_rows,
			 planstate->instrument->ntuples,
			 plan->plan_rows / planstate->instrument->ntuples,
			 nodeToString(plan));
		/* quals, sort keys, etc */
		switch (nodeTag(plan))
		{
			case T_IndexScan:
				SuggestMultiColumnStatisticsForQual(((IndexScan *) plan)->indexqualorig, es);
				break;
			case T_IndexOnlyScan:
				SuggestMultiColumnStatisticsForQual(((IndexOnlyScan *) plan)->indexqual, es);
				break;
			case T_BitmapIndexScan:
				SuggestMultiColumnStatisticsForQual(((BitmapIndexScan *) plan)->indexqualorig, es);
				break;
			case T_NestLoop:
				SuggestMultiColumnStatisticsForQual(((NestLoop *) plan)->join.joinqual, es);
				break;
			case T_MergeJoin:
				SuggestMultiColumnStatisticsForQual(((MergeJoin *) plan)->mergeclauses, es);
				SuggestMultiColumnStatisticsForQual(((MergeJoin *) plan)->join.joinqual, es);
				break;
			case T_HashJoin:
				SuggestMultiColumnStatisticsForQual(((HashJoin *) plan)->hashclauses, es);
				SuggestMultiColumnStatisticsForQual(((HashJoin *) plan)->join.joinqual, es);
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
			SuggestMultiColumnStatisticsForMemberNodes(((AppendState *) planstate)->appendplans,
													   ((AppendState *) planstate)->as_nplans,
													   es);
			break;
		case T_MergeAppend:
			SuggestMultiColumnStatisticsForMemberNodes(((MergeAppendState *) planstate)->mergeplans,
													   ((MergeAppendState *) planstate)->ms_nplans,
													   es);
			break;
		case T_BitmapAnd:
			SuggestMultiColumnStatisticsForMemberNodes(((BitmapAndState *) planstate)->bitmapplans,
													   ((BitmapAndState *) planstate)->nplans,
													   es);
			break;
		case T_BitmapOr:
			SuggestMultiColumnStatisticsForMemberNodes(((BitmapOrState *) planstate)->bitmapplans,
													   ((BitmapOrState *) planstate)->nplans,
													   es);
			break;
		case T_SubqueryScan:
			SuggestMultiColumnStatisticsForNode(((SubqueryScanState *) planstate)->subplan, es);
			break;
		default:
			break;
	}
}

/*
 * Module load callback
 */
void
_PG_init(void)
{
	/* Define custom GUC variables. */
	DefineCustomRealVariable("pg_stat_advisor.suggest_statistics_threshold",
							 "Set the threshold for estimated/actual rows",
							 "Zero disables suggestion of creating statistics",
							 &pg_stat_advisor_suggest_statistics_threshold,
							 1.0,
							 0.0,
							 1.0,
							 PGC_SUSET,
							 0,
							 NULL,
							 NULL,
							 NULL);

	DefineCustomRealVariable("pg_stat_advisor.analyze_scale_factor",
							 "Set the threshold for estimated/actual rows",
							 "Zero disables suggestion of updatinging statistics",
							 &pg_stat_advisor_analyze_scale_factor,
							 0.0,
							 0.0,
							 1.0,
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
