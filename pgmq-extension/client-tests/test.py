"""
PGMQ Test Suite Runner

This is the main test aggregator that imports all PGMQ client tests.
When pytest runs this file, it will discover and execute all imported tests.

Test files included:
- basic_notify_test.py: Basic notification functionality tests
- extended_notify_test.py: Comprehensive notification mechanism tests

Usage: uv run pytest test.py -v
"""

# Import all tests from test modules

# noinspection PyUnusedImports
from basic_notify_test import *
# noinspection PyUnusedImports
from extended_notify_test import *
