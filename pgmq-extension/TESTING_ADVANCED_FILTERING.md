# Testing Advanced JSON Filtering

This document describes how to test the advanced JSON filtering functionality added in version 1.9.0.

## Overview

The advanced filtering feature extends the `conditional` parameter in `pgmq.read()` and `pgmq.read_with_poll()` functions to support:
- Comparison operators: `>`, `>=`, `<`, `<=`, `=`, `!=`, `<>`
- Field existence checks: `exists`
- Nested field access: `"user.age"`
- Backward compatibility with simple containment: `{"field": "value"}`

## Test Files

1. **test/sql/advanced_filtering.sql** - Comprehensive regression tests
2. **test_advanced_filtering.sql** - Manual test script for quick verification

## Running Tests

### Option 1: Using pg_regress (Recommended)

```bash
cd pgmq-extension
make installcheck
```

This will run all tests including the new advanced filtering tests.

### Option 2: Manual Testing

```bash
# Start PostgreSQL with pgmq extension
docker run -d --name pgmq-test -e POSTGRES_PASSWORD=postgres -p 5432:5432 ghcr.io/pgmq/pg18-pgmq:latest

# Run the manual test script
psql postgres://postgres:postgres@localhost:5432/postgres -f test_advanced_filtering.sql
```

## Test Cases

### 1. Backward Compatibility
Tests that simple containment still works:
```sql
SELECT * FROM pgmq.read('queue', 30, 1, '{"name": "Alice"}');
```

### 2. Comparison Operators
Tests all comparison operators:
- `>` - Greater than
- `>=` - Greater than or equal
- `<` - Less than
- `<=` - Less than or equal
- `=` - Equality
- `!=` or `<>` - Not equal

Example:
```sql
SELECT * FROM pgmq.read('queue', 30, 1, '{"field": "age", "operator": ">", "value": 25}');
```

### 3. Field Existence
Tests the `exists` operator:
```sql
SELECT * FROM pgmq.read('queue', 30, 1, '{"field": "age", "operator": "exists"}');
```

### 4. Nested Fields
Tests nested field access:
```sql
SELECT * FROM pgmq.read('queue', 30, 1, '{"field": "user.age", "operator": ">", "value": 30}');
```

### 5. String Comparisons
Tests string value comparisons:
```sql
SELECT * FROM pgmq.read('queue', 30, 1, '{"field": "status", "operator": "=", "value": "active"}');
```

### 6. Error Handling
Tests that invalid operators raise appropriate errors:
```sql
-- Should raise error
SELECT * FROM pgmq.read('queue', 30, 1, '{"field": "age", "operator": "invalid", "value": 25}');
```

### 7. read_with_poll
Tests that advanced filtering works with polling:
```sql
SELECT * FROM pgmq.read_with_poll('queue', 30, 10, 5, 100, '{"field": "age", "operator": ">", "value": 25}');
```

## Expected Results

All tests should:
1. Return correct messages based on filter criteria
2. Maintain backward compatibility with existing code
3. Handle edge cases gracefully (NULL values, missing fields, etc.)
4. Raise appropriate errors for invalid input

## Verification Checklist

- [ ] Simple containment works (backward compatibility)
- [ ] All comparison operators work correctly
- [ ] Field existence check works
- [ ] Nested fields work
- [ ] String comparisons work
- [ ] Numeric comparisons work
- [ ] NULL handling works
- [ ] Error cases raise appropriate exceptions
- [ ] read_with_poll works with filters
- [ ] Multiple messages matching filter are returned correctly
- [ ] Empty filter returns all visible messages

