EXTENSION = my_nedo_cron
MODULE_big = my_nedo_cron
OBJS = src/my_nedo_cron.o src/job_metadata.o \
	src/cron_job.o src/cron_sql.o src/cron_task.o src/cron_time.o
DATA = sql/my_nedo_cron--0.1.sql

PG_CONFIG = /usr/local/pgsql/bin/pg_config
PG_CPPFLAGS = -I$(CURDIR)/include -I$(shell $(PG_CONFIG) --includedir)
SHLIB_LINK = -L$(shell $(PG_CONFIG) --libdir) -lpq


PGXS := $(shell $(PG_CONFIG) --pgxs)
include $(PGXS)