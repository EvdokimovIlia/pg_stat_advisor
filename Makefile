# contrib/pg_statistics_advisor/Makefile

MODULE_big = pg_statistics_advisor
OBJS = \
	$(WIN32RES) \
	pg_statistics_advisor.o
PGFILEDESC = "pg_statistics_advisor - notice for create statistics for fast execution of queries"

ifdef USE_PGXS
PG_CONFIG = /usr/local/pgsql/bin/pg_config
PGXS := $(shell $(PG_CONFIG) --pgxs)
include $(PGXS)
else
subdir = contrib/pg_statistics_advisor
top_builddir = ../..
include $(top_builddir)/src/Makefile.global
include $(top_srcdir)/contrib/contrib-global.mk
endif