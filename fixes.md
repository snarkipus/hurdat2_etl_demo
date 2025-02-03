# Test Failure Analysis and Fixes

## Test Failure: `test_invalid_header_format`
### Description
The test `test_invalid_header_format` in `tests/extract/test_extraction.py` failed because the `ExtractionError` was not raised when parsing invalid headers. This indicates that the parser is not correctly identifying and handling invalid header formats.

### Root Cause Analysis
The `parse_header` function in `src/hurdat2_etl/extract/parser.py` is not properly validating the header format, allowing invalid headers to pass without raising an `ExtractionError`.

### Recommended Fixes
1. **Add Validation for Header Format:**
   - Ensure that the header string contains exactly four comma-separated fields.
   - Validate that the basin is exactly two characters long.
   - Validate that the cyclone number is numeric and between 1-99.
   - Validate that the year is a four-digit number.
   - Ensure that the name field is not empty.
   - Ensure that the count is a valid integer.

2. **Update `parse_header` Function:**
   - Add checks for the number of fields and their validity before parsing.
   - Raise `ExtractionError` if any validation fails.

3. **Test Cases:**
   - Ensure that all invalid header formats are covered in the test cases.
   - Verify that the `ExtractionError` is raised for each invalid case.

### Steps to Verify the Fix
1. Implement the validation checks in the `parse_header` function.
2. Run the tests to ensure that all invalid headers now correctly raise `ExtractionError`.
3. Verify that valid headers are still parsed correctly without errors.
