use criterion::{Criterion, criterion_group, criterion_main};
use std::time::Duration;

static QUEUE: &str = "queue";

fn setup() -> diesel::PgConnection {
    use diesel::Connection;
    use diesel::RunQueryDsl;
    use diesel::sql_query;

    let mut conn =
        diesel::PgConnection::establish("postgres://postgres:postgres@localhost:5432/postgres")
            .unwrap();

    sql_query("CREATE EXTENSION IF NOT EXISTS pgmq CASCADE")
        .execute(&mut conn)
        .unwrap();

    sql_query(format!("SELECT pgmq.create('{QUEUE}')"))
        .execute(&mut conn)
        .unwrap();

    conn
}

fn setup_read() -> diesel::PgConnection {
    use diesel::{RunQueryDsl, select};
    use pgmq::queue::diesel::sql::pgmq_send_batch;

    let mut conn = setup();

    let _: Vec<i64> = select(pgmq_send_batch(
        QUEUE,
        vec![
            serde_json::Value::Null,
            serde_json::Value::Null,
            serde_json::Value::Null,
        ],
        vec![
            serde_json::Value::Null,
            serde_json::Value::Null,
            serde_json::Value::Null,
        ],
        0,
    ))
    .get_results(&mut conn)
    .unwrap();

    conn
}

fn teardown_read(conn: &mut diesel::PgConnection) {
    use diesel::{RunQueryDsl, select};
    use pgmq::queue::diesel::sql::pgmq_purge_queue;

    select(pgmq_purge_queue(QUEUE)).execute(conn).unwrap();
}

fn bench_metrics(c: &mut Criterion) {
    let mut conn = setup();

    let mut group = c.benchmark_group("metrics");
    group.measurement_time(Duration::from_secs(20));

    group.bench_function("sql-function-standard", |b| {
        b.iter(|| diesel_bench::metrics::sql_function::execute(&mut conn, QUEUE))
    });
    group.bench_function("sql-function-query-source", |b| {
        b.iter(|| diesel_bench::metrics::query_source::execute(&mut conn, QUEUE))
    });
    group.bench_function("query-fragment-standard", |b| {
        b.iter(|| diesel_bench::metrics::query_fragment::standard::execute(&mut conn, QUEUE))
    });
    group.bench_function("query-fragment-untyped", |b| {
        b.iter(|| diesel_bench::metrics::query_fragment::untyped::execute(&mut conn, QUEUE))
    });
    group.bench_function("query-fragment-tuple", |b| {
        b.iter(|| diesel_bench::metrics::query_fragment::tuple::execute(&mut conn, QUEUE))
    });
    group.bench_function("query-fragment-unnamed-params", |b| {
        b.iter(|| diesel_bench::metrics::query_fragment::unnamed_params::execute(&mut conn, QUEUE))
    });
    group.bench_function("sql-query", |b| {
        b.iter(|| diesel_bench::metrics::sql_query::execute(&mut conn, QUEUE))
    });

    group.finish();
}

fn bench_read(c: &mut Criterion) {
    let mut conn = setup_read();

    let mut group = c.benchmark_group("read");
    group.measurement_time(Duration::from_secs(20));

    group.bench_function("sql-function-standard", |b| {
        b.iter(|| diesel_bench::read::sql_function::execute(&mut conn, QUEUE, 1))
    });
    group.bench_function("sql-function-query-source", |b| {
        b.iter(|| diesel_bench::read::query_source::execute(&mut conn, QUEUE, 1))
    });
    group.bench_function("query-fragment-tuple", |b| {
        b.iter(|| diesel_bench::read::query_fragment::execute(&mut conn, QUEUE, 1))
    });

    group.finish();

    teardown_read(&mut conn);
}

fn bench_read_batch(c: &mut Criterion) {
    let mut conn = setup_read();

    let mut group = c.benchmark_group("read-batch");
    group.measurement_time(Duration::from_secs(60));

    group.bench_function("sql-function-standard", |b| {
        b.iter(|| diesel_bench::read::sql_function::execute(&mut conn, QUEUE, 3))
    });
    group.bench_function("sql-function-query-source", |b| {
        b.iter(|| diesel_bench::read::query_source::execute(&mut conn, QUEUE, 3))
    });
    group.bench_function("query-fragment-tuple", |b| {
        b.iter(|| diesel_bench::read::query_fragment::execute(&mut conn, QUEUE, 3))
    });

    group.finish();

    teardown_read(&mut conn);
}

criterion_group!(benches, bench_metrics, bench_read, bench_read_batch);

criterion_main!(benches);
