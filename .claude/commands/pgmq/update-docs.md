# Sync Documentation and README with Source Code

This command guides you through the process of verifying and synchronizing all documentation (types.md, functions.md, README.md) with the actual implementation in pgmq-extension/sql/pgmq.sql.

## Overview

When code changes are made to pgmq.sql, documentation often falls out of sync. This process ensures complete accuracy across all documentation.

## ⚠️ Important

1. **Always review generated documentation changes by hand** - Verify every edit for accuracy before committing
2. **Run this command twice** - After making documentation updates, run `/sync-docs-and-readme` again to verify all changes are correct and no new issues were introduced

## 🤖 AI-Agnostic

This command is designed to be AI-agnostic and can be used with any AI assistant or coding agent. Simply copy-paste the contents to ChatGPT, Copilot, or any other AI tool when you need to sync documentation with source code.

## Step 1: Analyze the Source Code

Read the main SQL file to understand the current implementation:

```
Read pgmq-extension/sql/pgmq.sql
```

Identify and catalog:

- All public function signatures (name, parameters, defaults, return types)
- All type definitions (pgmq.message_record, pgmq.queue_record, pgmq.metrics_result)
- All function overloads (especially send/send_batch which have 6 variants each)
- Deprecated functions and their warnings
- Default values for all parameters

## Step 2: Read All Documentation Files

Read all documentation systematically:

```
Read docs/api/sql/types.md
Read docs/api/sql/functions.md
Read pgmq-extension/README.md
```

Note: The root README.md is a symlink to pgmq-extension/README.md, so only edit the latter.

## Step 3: Deep Analysis and Comparison

Systematically check every detail by comparing source against documentation:

- Compare each function in source vs docs
- Verify parameter names, types, defaults
- Check return types (RETURNS vs RETURNS SETOF)
- Verify all type fields match
- Check example outputs for missing columns
- Identify any deprecated functions

Key areas to ultra-verify:

- Function parameter defaults (check if DEFAULT exists in source or if handled via overloads)
- Type field data types (integer vs bigint, varchar vs text)
- Return type keywords (RETURNS SETOF vs RETURNS TABLE vs RETURNS)
- Missing fields in composite types (like headers in message_record)

## Step 4: Create Comprehensive Issue List

Document ALL issues found, organized by category:

### Types Issues (types.md)

- Missing fields in type definitions
- Wrong data types for fields
- Missing type documentation entirely

### Function Signature Issues (functions.md)

- Missing function overloads
- Wrong parameter names or types
- Missing parameters
- Wrong return types
- Incorrect default values
- Missing DEFAULT keyword vs actual overloads

### Missing Documentation

- Undocumented public functions
- Missing deprecation warnings

### Example Issues

- Missing columns in output examples
- Incorrect default values in examples

### Important User-Facing Details

- Missing constraints (queue name length limits)
- Missing dependency notes (pg_partman requirements)
- Missing behavioral explanations

## Step 5: Organize Work into Phases

Create a todo list to organize work into phases:

```
Phase 1: Fix types.md
Phase 2: Fix existing function signatures
Phase 3: Add missing function documentation
Phase 4: Update all examples
Phase 5: Add important user-facing notes
```

## Step 6: Execute Fixes Methodically

### For Types (types.md)

- Add missing fields to existing types
- Fix incorrect data types
- Add any missing type definitions
- Update example outputs

### For Functions (functions.md)

- For functions with many overloads (send, send_batch):
  - Use "Signatures:" section listing all variants
  - Document parameters once with clear descriptions
  - Add examples showing different combinations

- For parameter corrections:
  - Verify actual parameter names in source
  - Check if DEFAULT exists in signature or if handled via overloads
  - Update descriptions to match actual behavior

- For return types:
  - Use exact syntax from source (RETURNS SETOF vs RETURNS)
  - Change RETURNS TABLE(...) to RETURNS SETOF <type_name> for consistency

### For Missing Functions

Add complete documentation including:

- Function signature with all parameters and defaults
- Parameter descriptions
- Return value description
- At least one example
- Any special notes (requirements, deprecations)

### For README (pgmq-extension/README.md)

- Add missing columns to ALL example outputs
- Fix default values in text descriptions
- Ensure partition behavior descriptions match source

## Step 7: Verify Each Edit Ultra-Carefully

Before committing, verify:

1. **Column order in examples** matches actual PostgreSQL output:
   - message_record: msg_id, read_ct, enqueued_at, vt, message, headers
   - archive table: msg_id, read_ct, enqueued_at, archived_at, vt, message, headers

2. **Default values** match source exactly:
   - Check the actual function signature in pgmq.sql
   - Don't assume defaults - verify line by line

3. **Parameter names** match source exactly:
   - Wrong: vt_offset (docs had this wrong)
   - Right: vt (actual parameter name)

4. **Type consistency**:
   - integer vs bigint (read_ct is integer, not bigint)
   - varchar vs text
   - TIMESTAMPTZ vs TIMESTAMP WITH TIME ZONE

## Step 8: Create Detailed Commits

Create separate commits for logical groupings:

1. **Commit 1: SQL API documentation sync**
   - Include all fixes to types.md and functions.md
   - Comprehensive commit message documenting all 29+ issues fixed
   - Organized by category for easy review

2. **Commit 2: README examples and defaults**
   - Include fixes to pgmq-extension/README.md only
   - Document example output corrections
   - Document default value corrections

Use commit message format:

```
docs: [Short title]

[Comprehensive description organized by category]

## [Category 1]
- Bullet points of changes

## [Category 2]
- Bullet points of changes

## Impact
[Summary of improvements]

🤖 Generated with [Claude Code](https://claude.com/claude-code)

Co-Authored-By: Claude <noreply@anthropic.com>
```

## Common Pitfalls to Avoid

1. **Don't assume parameter defaults exist** - Check if handled via function overloads
2. **Don't forget headers column** - It's in every message_record and archive table
3. **Don't mix up partition defaults** - They're '10000' and '100000', not 'daily' and '5 days'
4. **Don't edit root README.md** - It's a symlink, edit pgmq-extension/README.md
5. **Don't batch todo completions** - Mark each as complete immediately after finishing

## Verification Checklist

Before considering the sync complete, verify:

- [ ] All public functions in pgmq.sql are documented
- [ ] All function overloads are documented (especially send/send_batch)
- [ ] All type definitions match source exactly
- [ ] All parameters have correct names, types, and defaults
- [ ] All return types use correct syntax
- [ ] All examples include headers column
- [ ] All deprecated functions are marked with warnings
- [ ] Queue name length limit (47 chars) is documented
- [ ] pg_partman requirement is documented for partitioned queues
- [ ] Partition interval behavior (numeric vs time) is explained
- [ ] Archive table archived_at field is mentioned

## Post-Sync Actions

After committing:

1. Review git log to ensure commits are clean
2. Check that commit messages are comprehensive
3. Verify root README.md shows changes (via symlink)
4. Consider creating a PR with detailed description

## Example Issue Categories from Last Sync

Types Issues: 4

- Missing headers field
- Wrong read_ct type
- Missing queue_record type
- Missing metrics_result type

Function Signature Issues: 13

- send() missing 5 overloads
- send_batch() missing 5 overloads
- pop() missing qty parameter
- set_vt() wrong parameter name
- read() incorrect default documentation
- metrics() missing field
- list_queues() inconsistent return type
- etc.

Missing Documentation: 4

- create_non_partitioned()
- convert_archive_partitioned()
- enable_notify_insert()
- disable_notify_insert()

Example Issues: 5

- All message outputs missing headers

User-Facing Details: 3

- Queue name limit
- pg_partman requirement
- Partition interval behavior

Total: 29 issues identified and fixed

## Success Criteria

Documentation sync is complete when:

1. Every public function in pgmq.sql has accurate documentation
2. Every type definition matches source exactly
3. Every example output includes all columns
4. Every default value matches source
5. No inconsistencies between types.md, functions.md, and README.md
6. All deprecated functions are clearly marked
7. All important constraints and requirements are documented
