# Diesel benchmarks

Benchmarks for different diesel implementation options.

# Running locally

```shell
# Start and setup the database
make run.postgres && sleep 2 && make setup.env
# Run the benchmarks
cargo bench
```
