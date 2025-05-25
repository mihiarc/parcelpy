# Parcel Cleaning Tool

A tool for standardizing and cleaning parcel data from various sources. It maps county-specific field names to a standardized schema, processes PIDs (Parcel Identification Numbers), and generates reports on the standardization process.

## Overview

The Parcel Cleaning Tool addresses the challenge of working with disparate parcel datasets that use different field naming conventions and structures. By providing a configuration-driven approach to field standardization, it enables users to:

1. Map county-specific field names to standardized names using regex patterns
2. Conditionally process and standardize PIDs based on county requirements
3. Combine fields when data is split across multiple columns
4. Generate comprehensive reports on the standardization process
5. Convert data types according to field definitions

## Key Features

- **Configuration-Driven Design**: Adapt to any state or county's parcel data without code changes
- **Modular Architecture**: Follows SOLID principles for maintainability and extensibility
- **Field Standardization**: Maps source fields to standard names based on patterns
- **Conditional PID Processing**: Configurable PID validation and standardization
- **Field Combination**: Combine multiple source fields into a single standardized field
- **Data Type Conversion**: Apply proper data types based on field definitions
- **Reporting**: Generate detailed reports on the standardization process

## Installation

1. Clone the repository:
   ```
   git clone https://github.com/yourusername/parcel-cleaning-tool.git
   cd parcel-cleaning-tool
   ```

2. Install dependencies:
   ```
   pip install -r requirements.txt
   ```

3. Install the package in development mode:
   ```
   pip install -e .
   ```

## Usage

### Command Line Interface

The tool provides a command-line interface for processing parcel data files:

```bash
python -m src.cli.standardize_parcel_data --state NC --county CLAY --input path/to/data.csv --output-dir output
```

Options:
- `--state`, `-s`: Two-letter state code (e.g., NC)
- `--county`, `-c`: County code (e.g., CLAY)
- `--input`, `-i`: Input file path (formats: parquet, csv, shp)
- `--output-dir`, `-o`: Output directory path (default: ./output)
- `--config-dir`: Configuration directory path (default: ./config)
- `--verbose`, `-v`: Enable verbose logging

### Python API

You can also use the tool programmatically:

```python
from src.main import standardize_parcel_file

result = standardize_parcel_file(
    file_path="path/to/data.csv",
    state_code="NC",
    county_code="CLAY",
    output_dir="path/to/output",
    config_dir="path/to/config"
)

# Access results
print(f"Standardized {result['report']['rows']} rows")
print(f"Mapped {len(result['report']['field_mapping'])} fields")
```

## Configuration

The tool uses YAML configuration files for field definitions and county-specific settings:

### Directory Structure

```
config/
├── states/
│   ├── NC/
│   │   ├── fields.yaml       # General field definitions for North Carolina
│   │   ├── clay.yaml         # Clay County specific configuration
│   │   └── ... other counties
│   └── ... other states
```

### Field Definitions (fields.yaml)

Defines standardized field names, descriptions, data types, and patterns for matching:

```yaml
fields:
  parcel_id:
    description: "Primary parcel identification number"
    data_type: "string"
    patterns:
      - "(?i)^(parcel|pin|pid|parc|parcelid)$"
      - "(?i)^(tax_?id|property_?id|prop_?id)$"
    required: true
  
  owner_name:
    description: "Primary owner name"
    data_type: "string"
    patterns:
      - "(?i)^(owner|ownername|owner_name)$"
      - "(?i)^(taxpayer|taxpayer_name|name)$"
```

### County Configuration (e.g., clay.yaml)

County-specific settings, including PID processing and field overrides:

```yaml
county_code: "CLAY"
state: "NC"
fips_code: "37043"

# PID processing settings
process_pids: true
pid_field: "PIN_NUM"
standardize_pid: true
standardized_pid_length: 12
standardized_pid_prefix: "37043"

# County-specific excluded fields
excluded_fields:
  - "OBJECTID"
  - "Shape"

# County-specific field overrides
field_overrides:
  parcel_id:
    patterns:
      - "(?i)^(PIN_NUM|PINNUM)$"

# Field combinations
combine_fields:
  property_address:
    fields:
      - "ADDR_NUM"
      - "STREET_NAME"
    separator: " "
    null_handling: "skip"
```

## Output

The tool generates several output files:

1. `{input_file_stem}_standardized.parquet`: Standardized parcel data
2. `{input_file_stem}_report.json`: Detailed report on the standardization process
3. `{input_file_stem}_field_mapping.csv`: Mapping from source fields to standardized fields
4. `{input_file_stem}_unmapped_fields.csv`: List of fields that couldn't be mapped

## Development

### Project Structure

```
parcel-cleaning-tool/
├── config/               # Configuration files
├── src/                  # Source code
│   ├── cli/              # Command-line interface
│   ├── data_loading/     # Loading data from files
│   ├── field_mapping/    # Field standardization
│   ├── pid_processing/   # PID validation and standardization
│   ├── reporting/        # Report generation
│   ├── schema_registry/  # Schema registry management
│   └── orchestration/    # Process coordination
├── tests/                # Unit and integration tests
└── output/               # Output files
```

### Running Tests

```bash
pytest tests/
```

## Architecture

The tool follows a modular architecture with the following components:

1. **ParcelOrchestrator**: Coordinates the overall process flow
2. **RegistryManager**: Manages field definitions and patterns
3. **ParcelLoader**: Loads data from various file formats
4. **FieldStandardizer**: Maps fields to standardized names
5. **PIDProcessor**: Processes PIDs according to configuration
6. **ReportGenerator**: Generates standardization reports

## Contributing

Contributions are welcome! Please feel free to submit a Pull Request.

## License

This project is licensed under the MIT License - see the LICENSE file for details.
