## APSW Integration Patterns ([APSW Docs](https://rogerbinns.github.io/apsw/index.html))

### Connection Handling (Customizing Connections)
```python
# Pattern: APSW connection wrapper (APSW Best Practice)
class APSWConnection:
    def __init__(self, db_path: str):
        """Initialize with APSW's recommended settings"""
        self.conn = apsw.Connection(db_path)
        self.conn.setrowtrace(self._row_factory)  # Row processing
        self.conn.setexectrace(self._log_queries) # Query logging

    def _row_factory(self, cursor, row):
        """Convert rows to dicts matching pysqlite3 behavior"""
        return {col[0]: row[i] for i, col in enumerate(cursor.getdescription())}

    def _log_queries(self, cursor, sql, bindings):
        """Logging hook per APSW execution tracing docs"""
        logger.debug(f"EXEC: {sql} | {bindings}")
        return True
```

### Transaction Management (Explicit Control)
```python
# Pattern: Context manager matching APSW transaction docs
@contextlib.contextmanager
def transaction_handler(conn: APSWConnection):
    """Explicit transaction control (APSW Tips: Transactions)"""
    cursor = conn.conn.cursor()
    cursor.execute("BEGIN IMMEDIATE")  # Match current isolation level
    try:
        yield cursor
        cursor.execute("COMMIT")
    except Exception as e:
        cursor.execute("ROLLBACK")
        logger.error(f"Transaction rolled back: {str(e)}")
        raise
```

### Type Conversion (APSW Type Adapter Pattern)
```python
# Pattern: Type registration (APSW: Type conversion)
class APSWTypeAdapter:
    def __init__(self):
        self.adapters = {
            datetime: lambda dt: dt.isoformat(),  # DATETIME → TEXT
            Decimal: lambda d: str(d),            # DECIMAL → TEXT
            bytes: lambda b: b.hex()              # BLOB → HEX TEXT
        }

    def register(self, connection: APSWConnection):
        """APSW type adaptation (See: Type conversion docs)"""
        for pytype, adapter in self.adapters.items():
            connection.conn.createscalarfunction(
                f"adapt_{pytype.__name__}",
                adapter,
                1,
                deterministic=True
            )
```

### Migration Validation Steps
1. **Connection Testing**
   - Verify dict row format in test_database.py
   - Check query logging functionality

2. **Transaction Verification**
   - Add explicit rollback tests
   - Benchmark transaction throughput

3. **Type Compatibility**
   - Validate datetime/Decimal serialization
   - Add round-trip conversion tests

4. **Performance Metrics**
   - Compare INSERT/UPDATE rates
   - Profile memory usage (APSW Statistics module)

### Documentation References
- [APSW Transactions](https://rogerbinns.github.io/apsw/tips.html#transactions)
- [Type Conversion](https://rogerbinns.github.io/apsw/example.html#type-conversion-into-out-of-database)
- [Best Practices](https://rogerbinns.github.io/apsw/bestpractice.html)
