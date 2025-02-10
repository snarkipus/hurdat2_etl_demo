# HURDAT2 ETL Code Review Report

## 1. src/hurdat2_etl/exceptions.py
**Summary:** Foundation for error handling with clear hierarchy
**Key Findings:**
- Good base exception inheritance structure
- Missing contextual error information capture
- No error code standardization
- Limited documentation for exception usage contexts

**Architectural Issues:**
1. All exceptions empty `pass` implementations
2. No structured error context preservation
3. ProgressError mixes tracking failures with data errors
4. Missing serialization support for distributed tracing

**Recommendations:**
```python
class ETLError(Exception):
    """Base ETL error with context capture."""
    def __init__(self, msg: str, context: dict | None = None):
        self.context = context or {}
        super().__init__(f"{msg} | Context: {self.context}")
```

## 2. src/hurdat2_etl/transform/transform.py
**Summary:** Data transformation implementation
**Key Findings:**
- Atlantic basin bounds hardcoded (violates config principle)
- Iterator materialization defeats streaming purpose
- Nested error handling creates maintenance complexity
- Normalization stub limits data quality enforcement

**Performance Issues:**
1. Line 87: `storms = list(data)` - Forces full dataset into memory
2. Double progress tracking (storm + observations)
3. No batch processing for observation normalization

**Recommendations:**
```python
# Change to generator-based processing
def process_storm(storm: Storm) -> Storm:
    try:
        transformed = transform_data(storm)
        transformed.observations = (normalize(o) for o in storm.observations)
        return transformed
    except TransformError:
        self.log_failed_storm(storm)
        return None  # Filtered in pipeline

yield from filter(None, (process_storm(s) for s in data))
```

## 3. src/hurdat2_etl/core.py
**Summary:** Pipeline foundation implementation
**Key Issues:**
- Tight coupling to tqdm violates Dependency Inversion
- ETLPipeline only supports linear execution
- No resource lifecycle management
- Abstract process() method lacks error scaffolding

**Pattern Improvements:**
1. Introduce ProgressReporter protocol
```python
class ProgressReporter(Protocol):
    def init(self, total: int, desc: str) -> None: ...
    def update(self, n: int = 1) -> None: ...
    def close(self) -> None: ...
```

2. Add pipeline hooks for resource management:
```python
def setup(self) -> None: ...
def teardown(self) -> None: ...
```

## 4. src/hurdat2_etl/main.py
**Summary:** Pipeline orchestration
**Key Issues:**
- Monolithic pipeline construction
- Error handling anti-pattern (line 113-115)
- Tight coupling between CLI and pipeline config
- No validation of input/output paths

**Security Improvements:**
1. Add input validation:
```python
def validate_path(path: Path) -> Path:
    if not path.exists():
        raise ETLError(f"Path {path} does not exist")
    if path.stat().st_size == 0:
        raise ETLError(f"File {path} is empty")
    return path
```

2. Implement configuration sanitization:
```python
Settings.validate()  # Check all paths and connections
```

## 5. src/hurdat2_etl/load/connection.py
**Summary:** Database connection management
**Key Risks:**
- Queue-based pool limits concurrent access
- No connection health checks
- Missing spatialite extension validation
- Hardcoded pragma settings in connection

**Performance Recommendations:**
1. Implement connection recycling:
```python
def _validate_connection(conn: apsw.Connection) -> bool:
    try:
        conn.cursor().execute("SELECT 1")
        return True
    except apsw.Error:
        return False
```

2. Add adaptive pool sizing:
```python
def adjust_pool_size(current_utilization: float) -> None:
    if current_utilization > 0.8:
        self.pool_size += 2
    elif current_utilization < 0.2:
        self.pool_size = max(1, self.pool_size - 1)
```

## 6. src/hurdat2_etl/models.py
**Summary:** Data model definitions and validation
**Key Findings:**
- Strict basin validation limits multi-basin support
- Cyclone number range too permissive (0-99 vs 1-99)
- Missing coordinate reference system in spatial data
- Required fields may not match data availability
- Hardcoded validation ranges in multiple locations

**Data Model Issues:**
1. Basin restricted to "AL" prevents processing other basins
2. Cyclone number 00 validation mismatch with HURDAT2 spec
3. WKT output missing SRID (should be EPSG:4326)
4. StormStatus enum coverage not verified
5. Field requirements vs HURDAT2 data reality mismatch

**Recommendations:**
```python
# Expand basin validation
VALID_BASINS = {"AL", "EP", "CP", "WP", "IO", "SH"}

@field_validator("basin")
@classmethod
def validate_basin(cls, value: str) -> str:
    if value not in VALID_BASINS:
        raise ValueError(f"Invalid basin: {value}")
    return value

# Add SRID to WKT output
def to_wkt(self) -> str:
    return f"SRID=4326;POINT({self.longitude} {self.latitude})"

# Update cyclone number validation
@field_validator("cyclone_number")
@classmethod
def validate_cyclone_number(cls, value: int) -> int:
    if not 1 <= value <= MAX_CYCLONES:  # Changed from 0
        raise ValueError(f"Cyclone number must be 01-{MAX_CYCLONES:02}, got {value:02}")
    return value
```

## 7. src/hurdat2_etl/extract/extract.py
**Summary:** Data extraction implementation
**Key Findings:**
- File validation occurs after initialization
- Duplicate file reading for line counting
- Progress tracking conflates header/observation lines
- No validation of file format/contents
- Tight coupling to Settings class

**Extraction Issues:**
1. Line 63: Full file read just for line counting
2. Lines 67-72: Progress updates based on file lines not parsed entities
3. Missing format validation beyond file existence
4. No chunking/streaming for large files
5. Error handling too broad (line 78)

**Recommendations:**
```python
# Add format validation
def validate_file_format(self) -> None:
    with open(self.input_path) as f:
        header = f.readline()
        if not header.startswith("XXXXXX"):
            raise ExtractionError("Invalid file header")

# Use single file handle for counting and parsing
def process(self, _: None) -> Iterator[Storm]:
    with open(self.input_path) as f:
        total_lines = sum(1 for _ in f)
        f.seek(0)  # Reset file pointer

        self.init_progress(total_lines, "Extracting storms")
        for storm in parse_hurdat2(f):  # Pass file handle
            self.update_progress(storm.observation_count + 1)  # Header + obs
            yield storm

# Add chunked reading support
def chunked_read(self, chunk_size: int = 1000) -> Iterator[list[Storm]]:
    chunk = []
    for storm in self.process():
        chunk.append(storm)
        if len(chunk) >= chunk_size:
            yield chunk
            chunk = []
```

## 8. src/hurdat2_etl/extract/types.py
**Summary:** Type definitions for raw data
**Key Findings:**
- Missing validation for HURDAT2-specific codes
- No versioning for data format changes
- Hardcoded field positions
- Limited documentation of format assumptions
- No null handling for optional fields
- Duplicate enum values (WV)
- Missing modern status codes

**Data Type Issues:**
1. Field positions not validated against spec
2. Missing coordinate system documentation
3. No handling of historical format variations
4. Magic numbers for field positions
5. No unit validation for measurement fields
6. StormStatus enum missing PTC/IN/RL codes
7. Duplicate WV entry for wave
8. No deprecation handling for historical codes

**Recommendations:**
```python
# Update StormStatus enum with latest codes
class StormStatus(str, Enum):
    TROPICAL_DEPRESSION = "TD"
    TROPICAL_STORM = "TS"
    HURRICANE = "HU"
    EXTRATROPICAL = "EX"
    SUBTROPICAL_STORM = "SS"
    SUBTROPICAL_DEPRESSION = "SD"
    REMNANT_LOW = "RL"
    INVEST = "IN"
    POST_TROPICAL = "PT"
    POTENTIAL_TROPICAL_CYCLONE = "PTC"
    UNKNOWN = "XX"

# Add unit validation and version mapping
class Measurement(NamedTuple):
    value: float
    unit: Literal["kts", "mb", "nm"]

HURDAT2_VERSIONS = {
    "v3.0": {"header_len": 50, "obs_len": 80},
    "v2.0": {"header_len": 45, "obs_len": 75}
}

# Add deprecated code handling
DEPRECATED_CODES = {"WV": "Use DB for tropical waves"}
```

## 9. src/hurdat2_etl/extract/parser.py
**Summary:** HURDAT2 format parsing implementation
**Key Findings:**
- Monolithic parsing function
- Hardcoded field positions
- Minimal error validation
- No line format version handling
- Limited documentation of format assumptions

**Parsing Issues:**
1. Magic numbers for field positions
2. No validation of header/observation line lengths
3. Mixed parsing logic for headers/observations
4. No recovery from malformed lines
5. Missing coordinate precision handling

**Recommendations:**
```python
# Add line format validation
def validate_line_length(line: str, expected: int) -> None:
    if len(line) < expected:
        raise ParseError(f"Line too short ({len(line)} < {expected}): {line}")

# Separate header/observation parsing
def parse_header(line: str) -> StormHeader:
    validate_line_length(line, HEADER_LENGTH)
    return StormHeader(
        basin=line[0:2].strip(),
        cyclone_number=int(line[2:4]),
        year=int(line[4:8])
    )

# Add error context to exceptions
class ParseError(Exception):
    def __init__(self, msg: str, line: str, lineno: int):
        super().__init__(f"{msg} at line {lineno}: {line[:40]}...")

# Use dataclass for field positions
@dataclass
class FieldPositions:
    header_storm_id: slice = slice(0, 8)
    header_year: slice = slice(4, 8)
    obs_latitude: slice = slice(34, 40)
```

## 10. src/hurdat2_etl/config/settings.py
**Summary:** Configuration management implementation
**Key Findings:**
- Hardcoded environment-specific paths
- No validation for numeric ranges
- Magic numbers for missing values
- Limited environment variable support
- No secret management for DB credentials

**Configuration Issues:**
1. Line 25: OS-specific spatialite path
2. Line 30: PRAGMA settings not validated
3. Line 42: Magic number missing values
4. No hierarchy for different environments
5. Database credentials exposed in code

**Recommendations:**
```python
# Add Pydantic validation
from pydantic import BaseSettings, PositiveInt, constr

class DBSettings(BaseSettings):
    host: str = "localhost"
    port: PositiveInt = 5432
    user: str
    password: constr(min_length=8)
    pool_size: PositiveInt = 5

    class Config:
        env_prefix = "DB_"
        secrets_dir = "/run/secrets"

# Add environment-specific configs
class Config(BaseSettings):
    environment: Literal["dev", "staging", "prod"] = "dev"
    db: DBSettings = DBSettings()
    logging: LoggingConfig = LoggingConfig()

# Convert magic numbers to enums
class MissingValue(int, Enum):
    WIND = -999
    PRESSURE = -99
```

```python
# Add dynamic spatialite detection
def find_spatialite() -> Path:
    for path in [
        "/usr/lib/x86_64-linux-gnu/mod_spatialite.so",
        "/usr/local/lib/mod_spatialite.dylib",
        "C:/OSGeo4W/bin/mod_spatialite.dll"
    ]:
        if Path(path).exists():
            return Path(path)
    raise ImportError("Spatialite library not found")

# Add field position constants
class FieldPositions:
    HEADER_STORM_ID = slice(0, 8)
    HEADER_VERSION = slice(9, 13)
    OBS_LATITUDE = slice(34, 40)

# Add measurement units validation
class MeasurementWithUnits(NamedTuple):
    value: float
    units: Literal["kts", "mb", "nm"]
```



## 11. src/hurdat2_etl/load/load.py
**Summary:** Database loading implementation
**Key Findings:**
- Monolithic SQL statements reduce maintainability (lines 75-126)
- Manual connection pooling management (lines 208-312)
- No retry logic for failed inserts (lines 282-287)
- Hardcoded spatial reference system (line 133)
- Validation triggers block historical data (lines 136-152)

**Loading Issues:**
1. Schema definition lacks version control
2. Batch processing implemented manually vs using DB utilities
3. Complex validation done in application layer vs database constraints
4. No connection retry mechanism
5. Spatial index creation not optimized (line 178)

**Recommendations:**
```python
# Use SQLAlchemy Core for schema management
from sqlalchemy import MetaData, Table, Column, Integer, Text, Float, ForeignKey

metadata = MetaData()

storms = Table(
    'storms', metadata,
    Column('id', Integer, primary_key=True),
    Column('basin', Text, nullable=False),
    Column('cyclone_number', Integer, nullable=False),
    Column('year', Integer, nullable=False),
    Column('name', Text, nullable=False)
)

# Add bulk insert with retry logic
def safe_bulk_insert(conn, table, data, retries=3):
    for attempt in range(retries):
        try:
            conn.execute(table.insert(), data)
            return
        except apsw.SQLError as e:
            if attempt == retries - 1:
                raise
            logging.warning(f"Insert failed, retrying ({attempt+1}/{retries}): {e}")

# Add spatial reference configuration
DEFAULT_SRID = 4326
ALTERNATE_SRIDS = {
    'historical': 4055,
    'scientific': 7030
}

# Add validation mode handling
class ValidationMode(Enum):
    STRICT = auto()
    HISTORICAL = auto()
    SCIENTIFIC = auto()
```

## Overall Recommendations
1. **Architectural Patterns:**
- Implement Pipeline as Directed Acyclic Graph (DAG)
- Adopt Circuit Breaker pattern for database operations
- Introduce Data Quality Gate pattern between stages

2. **Performance:**
- Implement streaming data flow
- Add batch processing for observations
- Introduce connection pooling metrics

3. **Observability:**
- Add OpenTelemetry instrumentation
- Implement structured logging
- Create pipeline health check endpoint

4. **Security:**
- Encrypt sensitive settings
- Implement input validation pipeline
- Add audit logging for database operations

## Next Steps
1. Prioritize streaming implementation
2. Develop configuration validation suite
3. Create architectural decision records (ADRs)
4. Implement observability framework
