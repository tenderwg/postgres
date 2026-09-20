MODULES = memtable
OBJS = memtable.o
EXTENSION = memtable
DATA = memtable--0.1.sql
PGFILEDESC = "memtable - in-memory table access method skeleton"

ifdef USE_PGXS
PG_CONFIG = pg_config
PGXS := $(shell $(PG_CONFIG) --pgxs)
include $(PGXS)
else
subdir = contrib/memtable
top_builddir = ../..
include $(top_builddir)/src/Makefile.global
include $(top_srcdir)/contrib/contrib-global.mk
endif