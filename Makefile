MODULE_big = pg_wal_ext
OBJS = \
        $(WIN32RES) \
        pg_wal.o

EXTENSION = pg_wal_ext
DATA = pg_wal_ext--1.0.sql
PGFILEDESC = "pg_wal_ext - Decode WAL files data"
REGRESS = pg_wal_ext

ifdef USE_PGXS
PG_CONFIG = pg_config
PGXS := $(shell $(PG_CONFIG) --pgxs)
include $(PGXS)
else
subdir = contrib/pg_wal_ext
top_builddir = ../..
include $(top_builddir)/src/Makefile.global
include $(top_srcdir)/contrib/contrib-global.mk
endif
