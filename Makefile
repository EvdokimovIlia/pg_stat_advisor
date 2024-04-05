# contrib/pg_stat_advisor/Makefile

MODULE_big = pg_stat_advisor
OBJS = \
	$(WIN32RES) \
	pg_stat_advisor.o
PGFILEDESC = "pg_stat_advisor - analyze query performance and recommend the creation of additional statistics"

REGRESS = pg_stat_advisor

ifdef USE_PGXS
PG_CONFIG = pg_config
PGXS := $(shell $(PG_CONFIG) --pgxs)
include $(PGXS)
else
subdir = contrib/pg_stat_advisor
top_builddir = ../..
include $(top_builddir)/src/Makefile.global
include $(top_srcdir)/contrib/contrib-global.mk
endif
