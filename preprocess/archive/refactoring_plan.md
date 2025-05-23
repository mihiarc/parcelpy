# Refactoring Plan for Better SRP

## Current Issues

The `TaxParcelCleaner` class currently handles multiple responsibilities:
1. File I/O operations
2. Field name standardization
3. PID formatting
4. Report generation
5. Data validation

## Proposed Structure

### 1. ParcelDataLoader
```python
class ParcelDataLoader:
    """Handles all parquet file loading operations."""
    
    def __init__(self, config: ParcelConfig):
        self.config = config
    
    def load_parquet(self, file_path: Path) -> pd.DataFrame:
        """Load and perform initial cleaning of parquet file."""
        try:
            df = pd.read_parquet(file_path)
            return self._remove_excluded_columns(df)
        except Exception as e:
            logger.error(f"Error loading {file_path}: {str(e)}")
            raise
            
    def _remove_excluded_columns(self, df: pd.DataFrame) -> pd.DataFrame:
        """Remove excluded columns based on configuration."""
        columns_to_drop = ['geometry'] + [
            col for col in df.columns 
            if col in self.config.excluded_fields
        ]
        return df.drop(columns=columns_to_drop)
```

### 2. FieldStandardizer
```python
class FieldStandardizer:
    """Handles field name standardization."""
    
    def __init__(self):
        self.mappers = [
            get_tax_field_group,
            get_land_field_group,
            get_owner_field_group,
            get_property_field_group,
            get_valuation_field_group
        ]
    
    def standardize_field(self, field_name: str) -> Tuple[str, Optional[str]]:
        """Map a county-specific field name to standard name."""
        for mapper in self.mappers:
            group, subgroup, sub_subgroup = mapper(field_name)
            if group:
                return self._create_standard_name(group, subgroup, sub_subgroup), group
        return field_name.lower(), None
        
    def _create_standard_name(self, group: str, subgroup: Optional[str], 
                            sub_subgroup: Optional[str]) -> str:
        """Create standardized field name from components."""
        if not subgroup:
            return group
        std_field = f"{group}_{subgroup}"
        if sub_subgroup:
            std_field = f"{std_field}_{sub_subgroup}"
        return std_field.lower()
```

### 3. PIDProcessor
```python
class PIDProcessor:
    """Handles all PID-related operations."""
    
    def __init__(self):
        self.formatter = PIDFormatter()
    
    def process_pids(self, df: pd.DataFrame, county_abbr: str) -> pd.DataFrame:
        """Format PIDs for a county."""
        pid_field = self.formatter.get_pid_field_name(county_abbr)
        
        if pid_field not in df.columns:
            raise ValueError(
                f"Required PID field '{pid_field}' not found in {county_abbr} county data.\n"
                f"Available columns: {sorted(df.columns.tolist())}"
            )
        
        df['mn_parcel_id'] = df[pid_field].apply(
            lambda x: self.formatter.format_pid(x, county_abbr)
        )
        return df
```

### 4. FieldReportGenerator
```python
class FieldReportGenerator:
    """Handles report generation for field mapping."""
    
    def generate_report(self, df: pd.DataFrame, county_abbr: str,
                       unmapped_columns: List[str],
                       excluded_fields: Set[str]) -> None:
        """Generate field mapping report."""
        report = {
            'county': county_abbr,
            'unmapped_fields': self._get_field_info(df, unmapped_columns),
            'excluded_fields': self._get_field_info(df, excluded_fields)
        }
        
        output_dir = Path('output') / county_abbr
        output_dir.mkdir(parents=True, exist_ok=True)
        
        with open(output_dir / f"{county_abbr}_field_report.json", 'w') as f:
            json.dump(report, f, indent=2)
            
    def _get_field_info(self, df: pd.DataFrame, 
                       columns: Union[List[str], Set[str]]) -> List[Dict]:
        """Get sample values for fields."""
        return [
            {
                'field_name': col,
                'sample_value': str(df[col].iloc[0]) if not df[col].empty else None
            }
            for col in columns
        ]
```

### 5. ParcelDataOrchestrator
```python
class ParcelDataOrchestrator:
    """Orchestrates the parcel data processing workflow."""
    
    def __init__(self, config: Optional[ParcelConfig] = None):
        self.config = config or ParcelConfig.default()
        self.loader = ParcelDataLoader(self.config)
        self.standardizer = FieldStandardizer()
        self.pid_processor = PIDProcessor()
        self.report_generator = FieldReportGenerator()
    
    def process_parcels(self, input_path: Path) -> pd.DataFrame:
        """Process parcel data through the entire workflow."""
        try:
            # 1. Load data
            df = self.loader.load_parquet(input_path)
            county_abbr = input_path.stem[:4].upper()
            
            # 2. Process PIDs
            df = self.pid_processor.process_pids(df, county_abbr)
            
            # 3. Standardize fields
            mapped_columns = {}
            unmapped_columns = []
            
            for col in df.columns:
                std_field, group = self.standardizer.standardize_field(col)
                if group:
                    mapped_columns[col] = std_field
                else:
                    unmapped_columns.append(col)
            
            # 4. Generate report
            self.report_generator.generate_report(
                df, county_abbr, unmapped_columns, self.config.excluded_fields
            )
            
            # 5. Clean up dataframe
            df = df.rename(columns=mapped_columns)
            df = df.drop(columns=unmapped_columns + list(mapped_columns.keys()))
            
            return df
            
        except Exception as e:
            logger.error(f"Error processing {input_path}: {str(e)}")
            raise
```

## Benefits of This Structure

1. **Clear Responsibilities**
   - Each class has a single, well-defined purpose
   - Easy to understand what each component does
   - Simple to modify individual components

2. **Better Testing**
   - Can test each component in isolation
   - Easier to mock dependencies
   - More focused test cases

3. **Improved Maintainability**
   - Changes are localized to relevant components
   - Easier to add new features
   - Better error isolation

4. **Enhanced Reusability**
   - Components can be used independently
   - Easy to combine in different ways
   - Clear interfaces between components

## Implementation Steps

1. Create new files for each component:
   ```
   src/
   ├── data_loading/
   │   └── parcel_loader.py
   ├── field_mapping/
   │   └── standardizer.py
   ├── pid_processing/
   │   └── processor.py
   ├── reporting/
   │   └── report_generator.py
   └── orchestration/
       └── orchestrator.py
   ```

2. Move existing code to new components

3. Update imports and dependencies

4. Add proper error handling and logging

5. Update tests to match new structure

## Testing Strategy

1. **Unit Tests**
   - Test each component in isolation
   - Mock dependencies
   - Focus on edge cases

2. **Integration Tests**
   - Test component interactions
   - Verify workflow
   - Test with real data

3. **End-to-End Tests**
   - Test complete workflow
   - Verify output correctness
   - Check error handling 