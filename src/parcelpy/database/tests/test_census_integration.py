#!/usr/bin/env python3
"""
Tests for Census Integration module.

These tests verify the census integration functionality works correctly.
Note: Some tests require internet access and a Census API key.
"""

import pytest
import pandas as pd
import geopandas as gpd
from unittest.mock import Mock, patch
import tempfile
from pathlib import Path

# Try to import the modules
try:
    from parcelpy.database import DatabaseManager, CensusIntegration
    PARCELPY_AVAILABLE = True
except ImportError:
    PARCELPY_AVAILABLE = False

try:
    import socialmapper
    SOCIALMAPPER_AVAILABLE = True
except ImportError:
    SOCIALMAPPER_AVAILABLE = False


@pytest.mark.skipif(not PARCELPY_AVAILABLE, reason="ParcelPy not available")
class TestCensusIntegrationBasic:
    """Basic tests that don't require SocialMapper."""
    
    def test_import_without_socialmapper(self):
        """Test that import works gracefully without SocialMapper."""
        with patch.dict('sys.modules', {'socialmapper.census': None}):
            # Should not raise ImportError, but should work in mock mode
            from parcelpy.database.core.census_integration import CensusIntegration
            assert CensusIntegration is not None
    
    def test_database_manager_initialization(self):
        """Test that DatabaseManager can be initialized."""
        with tempfile.NamedTemporaryFile(suffix='.duckdb', delete=True) as tmp:
            db_path = tmp.name
        
        # Remove the file so DuckDB can create a fresh database
        Path(db_path).unlink(missing_ok=True)
        
        try:
            db_manager = DatabaseManager(db_path=db_path)
            assert db_manager.db_path == Path(db_path)
        finally:
            Path(db_path).unlink(missing_ok=True)


@pytest.mark.skipif(not SOCIALMAPPER_AVAILABLE, reason="SocialMapper not available")
@pytest.mark.skipif(not PARCELPY_AVAILABLE, reason="ParcelPy not available")
class TestCensusIntegrationWithSocialMapper:
    """Tests that require SocialMapper to be installed."""
    
    @pytest.fixture
    def db_manager(self):
        """Create a test database manager."""
        with tempfile.NamedTemporaryFile(suffix='.duckdb', delete=True) as tmp:
            db_path = tmp.name
        
        # Remove the file so DuckDB can create a fresh database
        Path(db_path).unlink(missing_ok=True)
        
        db_manager = DatabaseManager(db_path=db_path)
        
        # Create sample parcel data
        with db_manager.get_connection() as conn:
            conn.execute("""
                CREATE TABLE parcels (
                    parno VARCHAR PRIMARY KEY,
                    geometry GEOMETRY,
                    assessed_value DOUBLE
                )
            """)
            
            # Insert sample parcels (Raleigh, NC area)
            sample_parcels = [
                ('12345', 'POINT(-78.8 35.8)', 250000),
                ('12346', 'POINT(-78.9 35.9)', 300000),
                ('12347', 'POINT(-80.8 35.2)', 200000)  # Charlotte area
            ]
            
            for parno, geom_wkt, value in sample_parcels:
                conn.execute(
                    "INSERT INTO parcels (parno, geometry, assessed_value) VALUES (?, ST_GeomFromText(?), ?)",
                    [parno, geom_wkt, value]
                )
        
        yield db_manager
        
        # Cleanup
        Path(db_path).unlink(missing_ok=True)
    
    def test_census_integration_initialization(self, db_manager):
        """Test that CensusIntegration can be initialized."""
        census_integration = CensusIntegration(
            parcel_db_manager=db_manager,
            cache_boundaries=False
        )
        
        assert census_integration.parcel_db == db_manager
        assert census_integration.census_db is not None
        assert census_integration.census_data_manager is not None
    
    def test_census_schema_creation(self, db_manager):
        """Test that census integration schema is created correctly."""
        census_integration = CensusIntegration(
            parcel_db_manager=db_manager,
            cache_boundaries=False
        )
        
        # Check that required tables exist
        tables = db_manager.list_tables()
        assert 'parcel_census_geography' in tables
        assert 'parcel_census_data' in tables
        
        # Check table structure
        geo_info = db_manager.get_table_info('parcel_census_geography')
        geo_columns = geo_info['column_name'].tolist()
        
        expected_geo_columns = [
            'parcel_id', 'state_fips', 'county_fips', 
            'tract_geoid', 'block_group_geoid',
            'centroid_lat', 'centroid_lon'
        ]
        
        for col in expected_geo_columns:
            assert col in geo_columns
    
    def test_get_census_integration_status_empty(self, db_manager):
        """Test status check on empty database."""
        census_integration = CensusIntegration(
            parcel_db_manager=db_manager,
            cache_boundaries=False
        )
        
        status = census_integration.get_census_integration_status()
        
        assert 'geography_mappings' in status
        assert 'census_data' in status
        assert 'available_variables' in status
        
        # Should be empty initially
        assert status['geography_mappings']['total_mappings'] == 0
        assert status['census_data']['total_records'] == 0
    
    @patch('parcelpy.database.core.census_integration.get_geography_from_point')
    def test_link_parcels_to_census_geographies_mock(self, mock_get_geography, db_manager):
        """Test linking parcels to census geographies with mocked API calls."""
        # Mock the geography lookup
        mock_get_geography.return_value = {
            'state_fips': '37',
            'county_fips': '183',
            'tract_geoid': '37183001001',
            'block_group_geoid': '371830010011'
        }
        
        census_integration = CensusIntegration(
            parcel_db_manager=db_manager,
            cache_boundaries=False
        )
        
        # Link parcels to geographies
        summary = census_integration.link_parcels_to_census_geographies(
            parcel_table="parcels",
            batch_size=10
        )
        
        # Check summary
        assert summary['total_parcels'] == 3
        assert summary['processed'] == 3
        assert summary['errors'] == 0
        assert summary['success_rate'] == 100.0
        
        # Check that geography mappings were created
        status = census_integration.get_census_integration_status()
        assert status['geography_mappings']['total_mappings'] == 3
        assert status['geography_mappings']['states'] == 1
        assert status['geography_mappings']['counties'] == 1
    
    def test_create_enriched_view_without_data(self, db_manager):
        """Test creating enriched view without census data."""
        census_integration = CensusIntegration(
            parcel_db_manager=db_manager,
            cache_boundaries=False
        )
        
        # Should raise error when no census variables are available
        with pytest.raises(ValueError, match="No census variables available"):
            census_integration.create_enriched_parcel_view()
    
    def test_analyze_parcel_demographics_without_data(self, db_manager):
        """Test demographic analysis without census data."""
        census_integration = CensusIntegration(
            parcel_db_manager=db_manager,
            cache_boundaries=False
        )
        
        # Should raise error when no census data is available
        with pytest.raises(ValueError, match="No census data available"):
            census_integration.analyze_parcel_demographics()


@pytest.mark.integration
@pytest.mark.skipif(not SOCIALMAPPER_AVAILABLE, reason="SocialMapper not available")
@pytest.mark.skipif(not PARCELPY_AVAILABLE, reason="ParcelPy not available")
class TestCensusIntegrationIntegration:
    """Integration tests that require internet access and Census API key."""
    
    @pytest.fixture
    def db_manager_with_real_data(self):
        """Create database with real parcel data for integration testing."""
        with tempfile.NamedTemporaryFile(suffix='.duckdb', delete=True) as tmp:
            db_path = tmp.name
        
        # Remove the file so DuckDB can create a fresh database
        Path(db_path).unlink(missing_ok=True)
        
        db_manager = DatabaseManager(db_path=db_path)
        
        # Create sample parcel data with real coordinates
        with db_manager.get_connection() as conn:
            conn.execute("""
                CREATE TABLE parcels (
                    parno VARCHAR PRIMARY KEY,
                    geometry GEOMETRY,
                    assessed_value DOUBLE
                )
            """)
            
            # Insert parcels in Raleigh, NC (known good coordinates)
            sample_parcels = [
                ('REAL001', 'POINT(-78.6382 35.7796)', 350000),  # Downtown Raleigh
                ('REAL002', 'POINT(-78.7811 35.8302)', 275000),  # North Raleigh
            ]
            
            for parno, geom_wkt, value in sample_parcels:
                conn.execute(
                    "INSERT INTO parcels (parno, geometry, assessed_value) VALUES (?, ST_GeomFromText(?), ?)",
                    [parno, geom_wkt, value]
                )
        
        yield db_manager
        
        # Cleanup
        Path(db_path).unlink(missing_ok=True)
    
    @pytest.mark.slow
    def test_full_census_integration_workflow(self, db_manager_with_real_data):
        """Test the complete census integration workflow with real API calls."""
        import os
        
        # Skip if no API key is available
        if not os.environ.get('CENSUS_API_KEY'):
            pytest.skip("Census API key not available")
        
        census_integration = CensusIntegration(
            parcel_db_manager=db_manager_with_real_data,
            cache_boundaries=False
        )
        
        # Step 1: Link parcels to census geographies
        geography_summary = census_integration.link_parcels_to_census_geographies(
            parcel_table="parcels",
            batch_size=10
        )
        
        assert geography_summary['total_parcels'] == 2
        assert geography_summary['processed'] >= 1  # At least one should succeed
        
        # Step 2: Enrich with census data (if geography linking succeeded)
        if geography_summary['processed'] > 0:
            enrichment_summary = census_integration.enrich_parcels_with_census_data(
                variables=['total_population'],
                year=2021
            )
            
            assert enrichment_summary['block_groups'] >= 1
            assert enrichment_summary['variables'] == 1
            
            # Step 3: Create enriched view
            view_name = census_integration.create_enriched_parcel_view(
                view_name="test_enriched_parcels"
            )
            
            assert view_name == "test_enriched_parcels"
            
            # Step 4: Query enriched data
            enriched_parcels = census_integration.get_parcels_with_demographics(
                parcel_table="parcels",
                limit=10
            )
            
            assert len(enriched_parcels) >= 1
            assert 'state_fips' in enriched_parcels.columns
            assert 'county_fips' in enriched_parcels.columns


def test_cli_import():
    """Test that CLI module can be imported."""
    try:
        from parcelpy.database import cli_census
        assert hasattr(cli_census, 'main')
    except ImportError:
        pytest.skip("CLI module not available")


if __name__ == "__main__":
    # Run basic tests
    pytest.main([__file__, "-v"]) 