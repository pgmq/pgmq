PGRX_POSTGRES ?= pg15
DISTVERSION  = $(shell grep -m 1 '^version' Cargo.toml | sed -e 's/[^"]*"\([^"]*\)",\{0,1\}/\1/')

test:
	cargo pgrx test $(PGRX_POSTGRES)
	cargo test --no-default-features --features ${PGRX_POSTGRES} -- --test-threads=1 --ignored

format:
	cargo +nightly fmt --all
	cargo +nightly clippy

run.postgres:
	docker run -d --name pgmq-pg -e POSTGRES_PASSWORD=postgres -p 5432:5432 quay.io/tembo/pgmq-pg:latest

META.json.bak: Cargo.toml META.json
	@sed -i.bak "s/@CARGO_VERSION@/$(DISTVERSION)/g" META.json

pgxn-zip: META.json.bak
	git archive --format zip --prefix=pgmq-$(DISTVERSION)/ -o pgmq-$(DISTVERSION).zip HEAD
	@mv META.json.bak META.json
