# contrib/walg_archive/Makefile

MODULES = walg_archive
PGFILEDESC = "walg_archive - walg archive module"

REGRESS = walg_archive
REGRESS_OPTS = --temp-config $(top_srcdir)/contrib/walg_archive/walg_archive.conf

NO_INSTALLCHECK = 1

ifdef USE_PGXS
PG_CONFIG = pg_config
PGXS := $(shell $(PG_CONFIG) --pgxs)
include $(PGXS)
else
subdir = contrib/walg_archive
top_builddir = ../..
include $(top_builddir)/src/Makefile.global
include $(top_srcdir)/contrib/contrib-global.mk
endif
