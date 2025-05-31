#!/usr/bin/env python3
"""
Verify database schema types and structure for ParcelPy.
This script checks that the database schema matches expected types and constraints.
"""

import json
from pathlib import Path
from sqlalchemy import text, inspect

from parcelpy.database.core.database_manager import DatabaseManager

def load_schema():
    """Load the normalized schema definition."""
    schema_path = Path(__file__).parent.parent / "schema.json"
    with open(schema_path) as f:
        return json.load(f)

def get_county_tables(db):
    """Get list of county-based tables."""
    with db.get_connection() as conn:
        result = conn.execute(text("""
            SELECT table_name 
            FROM information_schema.tables 
            WHERE table_schema = 'public' 
            AND (table_name LIKE '%county%' OR table_name LIKE '%parcels%')
            AND table_type = 'BASE TABLE';
        """))
        return [row[0] for row in result]

def analyze_column_types(db, table_name):
    """Analyze column types and constraints for a table."""
    with db.get_connection() as conn:
        # Get column information
        result = conn.execute(text("""
            SELECT 
                column_name,
                data_type,
                character_maximum_length,
                numeric_precision,
                numeric_scale,
                is_nullable,
                column_default
            FROM information_schema.columns 
            WHERE table_name = :table_name
            ORDER BY ordinal_position;
        """), {"table_name": table_name})
        
        columns = {}
        for row in result:
            col_type = row[1]
            if row[2]:  # has character_maximum_length
                col_type = f"{col_type}({row[2]})"
            elif row[3]:  # has numeric precision
                if row[4]:  # has numeric scale
                    col_type = f"{col_type}({row[3]},{row[4]})"
                else:
                    col_type = f"{col_type}({row[3]})"
            
            columns[row[0]] = {
                "type": col_type,
                "nullable": row[5] == "YES",
                "default": row[6]
            }
        
        return columns

def analyze_value_ranges(db, table_name):
    """Analyze actual value ranges for numeric and string columns."""
    with db.get_connection() as conn:
        # Get column names and types
        result = conn.execute(text("""
            SELECT column_name, data_type
            FROM information_schema.columns 
            WHERE table_name = :table_name;
        """), {"table_name": table_name})
        
        columns = {}
        for col_name, data_type in result:
            if data_type in ('character varying', 'varchar', 'text'):
                # For string columns, get max length
                query = text(f"""
                    SELECT 
                        MAX(length({col_name})) as max_len,
                        COUNT(DISTINCT {col_name}) as distinct_count
                    FROM {table_name}
                    WHERE {col_name} IS NOT NULL;
                """)
                try:
                    result = conn.execute(query).fetchone()
                    if result:
                        columns[col_name] = {
                            "max_length": result[0],
                            "distinct_values": result[1]
                        }
                except Exception as e:
                    print(f"Error analyzing {col_name}: {e}")
                    
            elif data_type in ('integer', 'bigint', 'numeric', 'double precision'):
                # For numeric columns, get min/max
                query = text(f"""
                    SELECT 
                        MIN({col_name}),
                        MAX({col_name}),
                        COUNT(DISTINCT {col_name}) as distinct_count
                    FROM {table_name}
                    WHERE {col_name} IS NOT NULL;
                """)
                try:
                    result = conn.execute(query).fetchone()
                    if result:
                        columns[col_name] = {
                            "min_value": result[0],
                            "max_value": result[1],
                            "distinct_values": result[2]
                        }
                except Exception as e:
                    print(f"Error analyzing {col_name}: {e}")
        
        return columns

def main():
    """Main verification function."""
    print("🔍 Analyzing schema compatibility...")
    print("=" * 60)
    
    # Load schema definition
    schema = load_schema()
    normalized_tables = schema["tables"]
    
    db = DatabaseManager()
    county_tables = get_county_tables(db)
    
    if not county_tables:
        print("❌ No county tables found to analyze!")
        return
    
    print(f"Found {len(county_tables)} tables to analyze:")
    for table in county_tables:
        print(f"  - {table}")
    
    # Collect all unique columns and their types across county tables
    all_columns = defaultdict(lambda: defaultdict(int))
    column_types = defaultdict(set)
    value_ranges = defaultdict(list)
    
    for table in county_tables:
        print(f"\nAnalyzing {table}...")
        
        # Get column types
        columns = analyze_column_types(db, table)
        for col_name, info in columns.items():
            all_columns[col_name]["count"] += 1
            column_types[col_name].add(info["type"])
            all_columns[col_name]["nullable"] |= info["nullable"]
        
        # Get value ranges
        ranges = analyze_value_ranges(db, table)
        for col_name, info in ranges.items():
            value_ranges[col_name].append(info)
    
    # Compare with normalized schema
    print("\n📊 Schema Analysis Results:")
    print("=" * 60)
    
    # Track columns for each normalized table
    for table_name, table_info in normalized_tables.items():
        print(f"\n{table_name.upper()} Table Analysis:")
        print("-" * 40)
        
        for col_name, col_info in table_info["columns"].items():
            schema_type = col_info["type"]
            
            if col_name in all_columns:
                actual_types = column_types[col_name]
                occurrence = all_columns[col_name]["count"]
                nullable = all_columns[col_name]["nullable"]
                
                print(f"\n{col_name}:")
                print(f"  Schema type: {schema_type}")
                print(f"  Found in: {occurrence}/{len(county_tables)} tables")
                print(f"  Actual types: {', '.join(actual_types)}")
                print(f"  Nullable: {nullable}")
                
                if col_name in value_ranges:
                    ranges = value_ranges[col_name]
                    if "max_length" in ranges[0]:  # string column
                        max_len = max(r["max_length"] for r in ranges if r["max_length"])
                        print(f"  Max length found: {max_len}")
                    elif "min_value" in ranges[0]:  # numeric column
                        min_val = min(r["min_value"] for r in ranges if r["min_value"] is not None)
                        max_val = max(r["max_value"] for r in ranges if r["max_value"] is not None)
                        print(f"  Value range: {min_val} to {max_val}")
            else:
                print(f"\n{col_name}:")
                print("  ⚠️ Not found in any existing tables")
    
    # Find columns in county tables not in schema
    all_schema_columns = set()
    for table_info in normalized_tables.values():
        all_schema_columns.update(table_info["columns"].keys())
    
    extra_columns = set(all_columns.keys()) - all_schema_columns
    if extra_columns:
        print("\n⚠️ Columns found in county tables but not in schema:")
        for col in sorted(extra_columns):
            types = column_types[col]
            occurrence = all_columns[col]["count"]
            print(f"  - {col}")
            print(f"    Types: {', '.join(types)}")
            print(f"    Found in: {occurrence}/{len(county_tables)} tables")
            if col in value_ranges:
                ranges = value_ranges[col]
                if "max_length" in ranges[0]:
                    max_len = max(r["max_length"] for r in ranges if r["max_length"])
                    print(f"    Max length: {max_len}")
                elif "min_value" in ranges[0]:
                    min_val = min(r["min_value"] for r in ranges if r["min_value"] is not None)
                    max_val = max(r["max_value"] for r in ranges if r["max_value"] is not None)
                    print(f"    Value range: {min_val} to {max_val}")

if __name__ == "__main__":
    main() 