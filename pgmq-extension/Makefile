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
	cd .. && git archive --format zip --prefix=$(EXTENSION)-$(EXTVERSION)/ -o pgmq-extension/$(EXTENSION)-$(EXTVERSION).zip --add-file pgmq-extension/META.json HEAD:pgmq-extension

run.postgres:
	docker run -d --name pgmq-pg -e POSTGRES_PASSWORD=postgres -p 5432:5432 ghcr.io/pgmq/pg18-pgmq:latest

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

dump.extension-schema:
	echo "--- pgmq extension version ---" > $(FILE_PATH)
	psql $(DATABASE_URL) -A -t -c "SELECT extversion FROM pg_extension where extname = 'pgmq';" >> $(FILE_PATH)
	echo "--- pg_dump ---" >> $(FILE_PATH)
	pg_dump -n pgmq -e pgmq $(DATABASE_URL) >> $(FILE_PATH)
	# `pg_dump` does not dump all data, so we need to manually dump the other data we care about
	echo "--- pgmq functions ---" >> $(FILE_PATH)
	psql $(DATABASE_URL) -A -t -c "SELECT pg_get_functiondef(f.oid) FROM pg_catalog.pg_proc f INNER JOIN pg_catalog.pg_namespace n ON (f.pronamespace = n.oid) WHERE n.nspname = 'pgmq' ORDER BY pg_get_functiondef(f.oid);" >> $(FILE_PATH)
	echo "--- pgmq tables ---" >> $(FILE_PATH)
	psql $(DATABASE_URL) -A -c "SELECT table_name, column_name, ordinal_position, column_default, is_nullable, data_type FROM information_schema.columns WHERE table_schema = 'pgmq' ORDER BY table_name, ordinal_position;" >> $(FILE_PATH)
	echo "--- pgmq types ---" >> $(FILE_PATH)
	psql $(DATABASE_URL) -A -c "SELECT udt_name, attribute_name, ordinal_position, attribute_default, is_nullable, data_type FROM information_schema.attributes where udt_schema = 'pgmq' ORDER BY udt_name, ordinal_position;" >> $(FILE_PATH)
