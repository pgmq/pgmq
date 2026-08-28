//! Private macro(s) used by the `pgmq` crate in tests.

use quote::quote;
use syn::{Item, ItemFn, parse_macro_input};

/// This attribute macro is only intended to be used to help generate tests for each implementation
/// of the `pgmq::Queue` trait.
///
/// The macro can be placed on a method that takes `conn_details: ConnDetails` and
/// `queue: impl Queue` parameters. Since this macro is only intended to be used in the `Queue`
/// trait integration tests, it makes some assumptions about utility types/functions that it needs
/// to be in scope. For example, it expects the `ConnDetails` struct to be in scope, as well as
/// functions to create connections and/or connection pools for each client that implements `Queue`,
/// and `before`/`after` functions to perform setup/teardown logic that's shared between all tests.
///
/// # Examples
///
/// ```ignore
/// #[pgmq_test_macro::queue_test]
/// async fn create(conn_details: ConnDetails, queue: impl Queue) {
///     queue.create("queue").await.unwrap();
/// }
/// ```
#[proc_macro_attribute]
pub fn queue_test(
    _attr: proc_macro::TokenStream,
    item: proc_macro::TokenStream,
) -> proc_macro::TokenStream {
    let item = match parse_macro_input!(item as Item) {
        Item::Fn(item) => item,
        _ => panic!("This macro only supports functions"),
    };

    // Create individual test functions using the test details for each client implementation.
    let test_functions = build_queue_tests(&item)
        .into_iter()
        // Include the tests that execute inside a transaction
        .chain(build_queue_transaction_tests(&item).into_iter());

    // Collect all the test functions into a single token stream.
    test_functions.collect::<proc_macro2::TokenStream>().into()
}

/// This attribute macro is only intended to be used to help generate tests for each implementation
/// of the `pgmq::QueueTransaction` trait.
///
/// The macro can be placed on a method that takes `conn_details: ConnDetails` and
/// `queue: impl QueueTransaction` parameters. Since this macro is only intended to be used in the
/// `QueueTransaction` trait integration tests, it makes some assumptions about utility
/// types/functions that it needs to be in scope. For example, it expects the `ConnDetails` struct
/// to be in scope, as well as functions to create connections and/or connection pools for each
/// client that implements `QueueTransaction`, and `before`/`after` functions to perform
/// setup/teardown logic that's shared between all tests.
///
/// # Examples
///
/// ```ignore
/// #[pgmq_test_macro::queue_transaction_test]
/// async fn acquire_queue_lock(conn_details: ConnDetails, queue: impl Queue) {
///     queue.acquire_queue_lock("queue").await.unwrap();
/// }
/// ```
#[proc_macro_attribute]
pub fn queue_transaction_test(
    _attr: proc_macro::TokenStream,
    item: proc_macro::TokenStream,
) -> proc_macro::TokenStream {
    let item = match parse_macro_input!(item as Item) {
        Item::Fn(item) => item,
        _ => panic!("This macro only supports functions"),
    };

    // Collect all the test functions into a single token stream.
    build_queue_transaction_tests(&item)
        .into_iter()
        .collect::<proc_macro2::TokenStream>()
        .into()
}

/// Builds the test functions for all types that only implement `Queue` and not `QueueTransaction`
fn build_queue_tests(original_fn: &ItemFn) -> impl IntoIterator<Item = proc_macro2::TokenStream> {
    /*
    Because the `Queue` trait's methods all take `self`, only a single method can be called in a
    method that takes `impl Queue`. Example:

    ```
    async fn test(queue: impl Queue) {
        queue.create(QUEUE).await.unwrap();
        // This second call is not allowed because the `queue.create` call moves ownership of the
        // `queue` variable.
        queue.send(QUEUE, serde_json::json!({}), serde_json::json!({}), 0).await.unwrap();
    }
    ```

    However, in practice, each type we're testing is a reference type, so we can get around this
    by simply copying the `block` (e.g., body) of the original test method into the individual test
    methods, and the block will compile with multiple method calls because the `queue` variable
    is a reference.
     */
    let original_block = &original_fn.block;

    let test_details = [
        (
            // The `pgmq` crate feature that needs to be enabled in order for this test to run.
            "sqlx",
            // String describing the type of the client implementation. Will be appended to the
            // original test name, so must be a valid rust function name.
            "ext_deref",
            // Details of how to create an instance of the client and how to invoke the original
            // test method with it.
            quote! {
                use std::ops::Deref;
                let queue_ext = initialization::pgmq_ext(&conn_details.test_db_url).await;
                let queue = queue_ext.deref();
                #original_block
            },
        ),
        (
            "sqlx",
            "ext_ref",
            quote! {
                let queue_ext = initialization::pgmq_ext(&conn_details.test_db_url).await;
                let queue = queue_ext.as_ref();
                #original_block
            },
        ),
        (
            "sqlx",
            "sqlx_cxn",
            quote! {
                let mut conn = initialization::sqlx_conn(&conn_details.test_db_url).await;
                let queue = &mut conn;
                #original_block
            },
        ),
        (
            "sqlx",
            "sqlx_pool",
            quote! {
                let pool = pgmq::util::connect(conn_details.test_db_url.as_str(), 2)
                    .await
                    .unwrap();
                let queue = &pool;
                #original_block
            },
        ),
        (
            "rust-postgres",
            "rp_client",
            quote! {
                {
                    /*
                    `postgres` is a wrapper around `tokio_postgres` -- it simply blocks on async
                    operations. So, we need to spawn a blocking task in order to be able to create
                    the client and invoke the original test method with it.
                     */
                    let conn_details = conn_details.clone();
                    tokio::task::spawn_blocking(move || {
                        tokio::runtime::Handle::current()
                            .block_on(async {
                                let mut client = initialization::rust_postgres(&conn_details.test_db_url);
                                let queue = &mut client;
                                #original_block
                            });
                    });
                }
            },
        ),
        (
            "tokio-postgres",
            "tp_client",
            quote! {
                let (client, conn) = initialization::tokio_postgres(&conn_details.test_db_url).await;
                tokio::spawn(async move {
                    conn.await.unwrap();
                });
                let queue = &client;
                #original_block
            },
        ),
        (
            "diesel-sync",
            "d_conn",
            quote! {
                let mut conn = initialization::diesel_conn(&conn_details.test_db_url);
                let queue = &mut conn;
                #original_block
            },
        ),
        (
            "diesel-sync-pool",
            "d_pool",
            quote! {
                let pool = initialization::diesel_pool(&conn_details.test_db_url);
                let queue = &pool;
                #original_block
            },
        ),
        (
            "diesel-sync-pool",
            "d_pool_conn",
            quote! {
                let pool = initialization::diesel_pool(&conn_details.test_db_url);
                let mut conn = pool.get().unwrap();
                let queue = &mut conn;
                #original_block
            },
        ),
        (
            "diesel-async",
            "da_conn",
            quote! {
                let mut conn = initialization::diesel_async_conn(&conn_details.test_db_url).await;
                let queue = &mut conn;
                #original_block
            },
        ),
        (
            "diesel-async-pool",
            "da_pool",
            quote! {
                let pool = initialization::diesel_async_pool(&conn_details.test_db_url).await;
                let queue = &pool;
                #original_block
            },
        ),
        (
            "diesel-async-pool",
            "da_pool_conn",
            quote! {
                let pool = initialization::diesel_async_pool(&conn_details.test_db_url).await;
                let mut conn = pool.get().await.unwrap();
                let queue = &mut conn;
                #original_block
            },
        ),
    ];

    // Create individual test functions using the test details for each client implementation.
    build_test_functions(&original_fn, test_details)
}

/// Builds the test functions for types that implement `QueueTransaction` and to need to run inside
/// a transaction.
fn build_queue_transaction_tests(
    original_fn: &ItemFn,
) -> impl IntoIterator<Item = proc_macro2::TokenStream> {
    let original_block = &original_fn.block;
    let test_details = [
        (
            "sqlx",
            "sqlx_txn",
            quote! {
                use sqlx::Connection;
                let mut conn = initialization::sqlx_conn(&conn_details.test_db_url).await;
                let mut transaction = conn.begin().await.unwrap();
                let queue = &mut transaction;

                #original_block

                transaction.commit().await.unwrap();
            },
        ),
        (
            "rust-postgres",
            "rp_txn",
            quote! {
                {
                    let conn_details = conn_details.clone();
                    tokio::task::spawn_blocking(move || {
                        tokio::runtime::Handle::current()
                            .block_on(async {
                                let mut client = initialization::rust_postgres(&conn_details.test_db_url);
                                let mut transaction = client.transaction().unwrap();
                                let queue = &mut transaction;
                                #original_block
                                transaction.commit().unwrap();
                            });
                    });
                }
            },
        ),
        (
            "tokio-postgres",
            "tp_txn",
            quote! {
                let (mut client, conn) = initialization::tokio_postgres(&conn_details.test_db_url).await;
                tokio::spawn(async move {
                    conn.await.unwrap();
                });
                let transaction = client.transaction().await.unwrap();
                let queue = &transaction;

                #original_block

                transaction.commit().await.unwrap();
            },
        ),
        (
            "diesel-sync",
            "d_txn",
            quote! {
                use diesel::Connection;
                let mut conn = initialization::diesel_conn(&conn_details.test_db_url);
                tokio::task::spawn_blocking(move || {
                    conn.transaction(|queue| -> Result<(), diesel::result::Error> {
                        tokio::runtime::Handle::current()
                            .block_on(async {
                                #original_block
                            });
                        Ok(())
                    }).unwrap();
                });
            },
        ),
        (
            "diesel-async",
            "da_txn",
            quote! {
                use diesel_async::AsyncConnection;
                let mut conn = initialization::diesel_async_conn(&conn_details.test_db_url).await;
                conn.transaction(async |queue| -> Result<(), diesel::result::Error> {
                    #original_block

                    Ok(())
                }).await.unwrap();
            },
        ),
    ];

    // Create individual test functions using the test details for each client implementation.
    build_test_functions(&original_fn, test_details)
}

fn build_test_functions<'a>(
    original_fn: &ItemFn,
    test_details: impl IntoIterator<Item = (&'a str, &'a str, proc_macro2::TokenStream)>,
) -> impl IntoIterator<Item = proc_macro2::TokenStream> {
    test_details
        .into_iter()
        .map(|(feature, impl_type, invoke)| {
            let test_name = syn::Ident::new(
                &format!("{}_{}", original_fn.sig.ident, impl_type),
                proc_macro2::Span::call_site(),
            );
            quote! {
                #[cfg(all(feature = "queue-experimental", feature = #feature))]
                #[tokio::test]
                async fn #test_name() {
                    let conn_details = ConnDetails::new();
                    // Clone the `conn_details` to use in the `after` hook because some test
                    // impls may perform a partial move of the original value.
                    let conn_details_cloned = conn_details.clone();
                    initialization::before(&conn_details).await;

                    #invoke

                    initialization::after(&conn_details_cloned).await;
                }
            }
        })
}
