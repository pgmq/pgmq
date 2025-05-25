EXTENSION     = pgmq
EXTVERSION    = $(shell grep "^default_version" pgmq.control | sed -r "s/default_version[^']+'([^']+).*/\1/")
DATA          = $(wildcard sql/*--*.sql)
TESTS         = $(wildcard test/sql/*.sql)
REGRESS       = $(patsubst test/sql/%.sql,%,$(TESTS))
REGRESS_OPTS ?= --inputdir=test
EXTRA_CLEAN   = $(EXTENSION)-$(EXTVERSION).zip sql/$(EXTENSION)--$(EXTVERSION).sql META.json
PG_PARTMAN_VER = 5.1.0
PG_CONFIG   ?= pg_config

# pg_isolation_regress available in v14 and higher.
ifeq ($(shell test $$($(PG_CONFIG) --version | awk '{print $$2}' | awk 'BEGIN { FS = "." }; { print $$1 }' | sed -E 's/[^0-9]//g') -ge 14; echo $$?),0)
ISOLATION   = $(patsubst test/specs/%.spec,%,$(wildcard test/specs/*.spec))
ISOLATION_OPTS = $(REGRESS_OPTS)
endif

PGXS := $(shell $(PG_CONFIG) --pgxs)
include $(PGXS)

all: sql/$(EXTENSION)--$(EXTVERSION).sql

sql/$(EXTENSION)--$(EXTVERSION).sql: sql/$(EXTENSION).sql
	cp $< $@

dist: META.json
	git archive --format zip --prefix=$(EXTENSION)-$(EXTVERSION)/ -o $(EXTENSION)-$(EXTVERSION).zip --add-file META.json --add-file HEAD sql pgmq.control README.md UPDATING.md Makefile

run.postgres:
	docker run -d --name pgmq-pg -e POSTGRES_PASSWORD=postgres -p 5432:5432 ghcr.io/pgmq/pg17-pgmq:latest

pgxn-zip: dist

META.json:
	sed 's/@@VERSION@@/$(EXTVERSION)/g' META.json.in > META.json

install-pg-partman:
	git clone https://github.com/pgpartman/pg_partman.git && \
	cd pg_partman && \
	git checkout v${PG_PARTMAN_VER} && \
	make && \
	make install PG_CONFIG=$(PG_CONFIG) && \
	cd ../ && rm -rf pg_partman
