# Location Data Debug Plan

## Issue Description
Tropical storm location data shows coordinates well outside the Atlantic basin (136.9°W to 63.0°E longitude, 7.0°N to 83.0°N latitude). This indicates potential issues in coordinate handling and validation.

## Current Data Flow Analysis

### 1. Raw Data Format (HURDAT2)
- Coordinates are provided in degrees with hemisphere indicators (N/S, E/W)
- Example: `29.1N, 90.2W`
- Source validation: None (raw text file)

### 2. Data Model Parsing (models.py)
```python
class Point:
    # Validation settings from config
    MAX_LATITUDE = 90.0
    MAX_LONGITUDE = 360.0  # Issue: Allows coordinates outside WGS84 range

    # Coordinate parsing with potential issues
    def parse_hurdat2(cls, coord: str, is_latitude: bool) -> float:
        # Converts Western longitudes > 180° using: 360° - value
        if direction == "W":
            if degrees > 180:
                return degrees - 360  # Potential source of invalid coordinates
            return -degrees
```

### 3. Database Storage (schema.py)
```sql
-- Spatial validation trigger
CREATE TRIGGER observations_geom_validate
BEFORE INSERT ON observations
FOR EACH ROW
BEGIN
    SELECT CASE
        WHEN ST_X(NEW.geom) < -180 OR ST_X(NEW.geom) > 180 THEN
            RAISE(ROLLBACK, 'Longitude out of range (-180 to 180)')
        WHEN ST_Y(NEW.geom) < -90 OR ST_Y(NEW.geom) > 90 THEN
            RAISE(ROLLBACK, 'Latitude out of range (-90 to 90)')
    END;
END;
```

## Identified Issues

1. **Inconsistent Coordinate Systems**
   - Models use 0-360° longitude range
   - Database uses WGS84 (-180° to 180° longitude)
   - No explicit Atlantic basin boundary validation

2. **Coordinate Transformation Issues**
   - Western longitude conversion may produce invalid results
   - No validation against Atlantic basin boundaries
   - Potential loss of precision or sign errors

3. **Missing Validation Layers**
   - No Atlantic basin specific validation
   - No intermediate validation between parsing and database storage
   - No warning system for suspicious coordinates

## Debug Plan

### 1. Add Logging Points
```python
# Add to Point.parse_hurdat2():
logging.debug(f"Raw coordinate: {coord}")
logging.debug(f"Parsed degrees: {degrees}, direction: {direction}")
logging.debug(f"Converted coordinate: {result}")

# Add to database insertion:
logging.debug(f"Pre-insert coordinates: lat={lat}, lon={lon}")
```

### 2. Add Coordinate Boundary Assertions
```python
# Add to Point class:
ATLANTIC_BASIN_BOUNDS = {
    'lat_min': 7.0,
    'lat_max': 83.0,
    'lon_min': -136.9,
    'lon_max': 63.0
}

def validate_atlantic_basin(self) -> bool:
    """Validate coordinates are within Atlantic basin."""
    return (self.ATLANTIC_BASIN_BOUNDS['lat_min'] <= self.latitude <= self.ATLANTIC_BASIN_BOUNDS['lat_max'] and
            self.ATLANTIC_BASIN_BOUNDS['lon_min'] <= self.longitude <= self.ATLANTIC_BASIN_BOUNDS['lon_max'])
```

### 3. Database Validation
```sql
-- Add Atlantic basin check to spatial trigger
CREATE TRIGGER observations_atlantic_validate
BEFORE INSERT ON observations
FOR EACH ROW
BEGIN
    SELECT CASE
        WHEN ST_Y(NEW.geom) < 7.0 OR ST_Y(NEW.geom) > 83.0 THEN
            RAISE(ROLLBACK, 'Latitude outside Atlantic basin')
        WHEN ST_X(NEW.geom) < -136.9 OR ST_X(NEW.geom) > 63.0 THEN
            RAISE(ROLLBACK, 'Longitude outside Atlantic basin')
    END;
END;
```

### 4. Systematic Testing

1. **Raw Data Validation**
   - Parse sample HURDAT2 file
   - Log all coordinate transformations
   - Identify patterns in out-of-bounds coordinates

2. **Transformation Testing**
   - Test coordinate conversion edge cases
   - Verify WGS84 compliance
   - Check precision loss in conversions

3. **Database Validation**
   - Test spatial trigger effectiveness
   - Verify coordinate storage accuracy
   - Check spatial index functionality

## Implementation Steps

1. Update settings.py:
   - Add Atlantic basin boundary constants
   - Modify MAX_LONGITUDE to use WGS84 standard (-180 to 180)

2. Modify models.py:
   - Add Atlantic basin validation
   - Update longitude conversion logic
   - Add detailed logging

3. Update schema.py:
   - Add Atlantic basin validation trigger
   - Enhance spatial validation

4. Create test cases:
   - Edge case coordinates
   - Atlantic basin boundaries
   - Invalid coordinate formats

## Expected Outcomes

1. All coordinates properly validated against Atlantic basin boundaries
2. Consistent coordinate system usage throughout pipeline
3. Clear error messages for invalid coordinates
4. Comprehensive logging for debugging
5. Improved data quality and reliability

## Success Criteria

1. No coordinates outside Atlantic basin boundaries
2. All coordinates properly converted to WGS84
3. Clear audit trail for coordinate transformations
4. Validated database constraints
5. Comprehensive test coverage
