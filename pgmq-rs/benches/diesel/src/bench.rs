use criterion::{Criterion, criterion_group, criterion_main};
use diesel::RunQueryDsl;
use diesel::{PgConnection, sql_query};

static QUEUE: &str = "queue";

fn setup() -> PgConnection {
    use diesel::Connection;
    let mut conn =
        PgConnection::establish("postgres://postgres:postgres@localhost:5432/postgres").unwrap();

    sql_query("CREATE EXTENSION IF NOT EXISTS pgmq CASCADE")
        .execute(&mut conn)
        .unwrap();

    sql_query(format!("SELECT pgmq.create('{QUEUE}')"))
        .execute(&mut conn)
        .unwrap();

    conn
}

fn sql_function_benchmark(c: &mut Criterion) {
    let mut conn = setup();
    c.bench_function("sql-function-standard", move |b| {
        b.iter(|| diesel_bench::sql_function::execute(&mut conn, QUEUE))
    });
}

fn query_source_benchmark(c: &mut Criterion) {
    let mut conn = setup();
    c.bench_function("sql-function-query-source", move |b| {
        b.iter(|| diesel_bench::query_source::execute(&mut conn, QUEUE))
    });
}

fn query_fragment_standard_benchmark(c: &mut Criterion) {
    let mut conn = setup();
    c.bench_function("query-fragment-standard", move |b| {
        b.iter(|| diesel_bench::query_fragment::standard::execute(&mut conn, QUEUE))
    });
}

fn query_fragment_untyped_benchmark(c: &mut Criterion) {
    let mut conn = setup();
    c.bench_function("query-fragment-untyped", move |b| {
        b.iter(|| diesel_bench::query_fragment::untyped::execute(&mut conn, QUEUE))
    });
}

fn query_fragment_tuple_benchmark(c: &mut Criterion) {
    let mut conn = setup();
    c.bench_function("query-fragment-tuple", move |b| {
        b.iter(|| diesel_bench::query_fragment::tuple::execute(&mut conn, QUEUE))
    });
}

fn query_fragment_unnamed_params_benchmark(c: &mut Criterion) {
    let mut conn = setup();
    c.bench_function("query-fragment-unnamed-params", move |b| {
        b.iter(|| diesel_bench::query_fragment::unnamed_params::execute(&mut conn, QUEUE))
    });
}

fn sql_query_benchmark(c: &mut Criterion) {
    let mut conn = setup();
    c.bench_function("sql-query", move |b| {
        b.iter(|| diesel_bench::sql_query::execute(&mut conn, QUEUE))
    });
}

criterion_group!(
    benches,
    sql_function_benchmark,
    query_source_benchmark,
    query_fragment_untyped_benchmark,
    query_fragment_standard_benchmark,
    query_fragment_tuple_benchmark,
    query_fragment_unnamed_params_benchmark,
    sql_query_benchmark
);
criterion_main!(benches);
