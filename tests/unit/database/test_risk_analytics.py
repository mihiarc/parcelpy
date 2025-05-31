#!/usr/bin/env python3
"""
Tests for Risk Analytics module.

These tests verify the risk analytics functionality works correctly
with comprehensive mocking to avoid database dependencies.
"""

import pytest
import pandas as pd
import numpy as np
from unittest.mock import Mock, patch, MagicMock
from datetime import datetime, timedelta
import sys
from pathlib import Path

# Add the parent directory to the path

from parcelpy.database.core.risk_analytics import RiskAnalytics


@pytest.fixture
def mock_db_manager():
    """Create a mock database manager for testing."""
    mock_db = Mock()
    mock_db.execute_query = Mock()
    return mock_db


@pytest.fixture
def sample_flood_risk_data():
    """Create sample flood risk data."""
    return pd.DataFrame({
        'parno': ['FR001', 'FR002', 'FR003', 'FR004', 'FR005'],
        'total_value': [250000, 400000, 180000, 600000, 320000],
        'acres': [0.5, 2.0, 0.3, 15.0, 1.2],
        'property_type': ['Residential', 'Commercial', 'Residential', 'Agricultural', 'Residential'],
        'longitude': [-78.8, -78.9, -78.7, -78.85, -78.95],
        'latitude': [34.8, 35.5, 36.2, 34.5, 35.8],  # Different elevations
        'elevation_category': ['Low_Elevation', 'Medium_Elevation', 'High_Elevation', 'Low_Elevation', 'Medium_Elevation'],
        'flood_risk_level': ['High', 'Medium', 'Low', 'High', 'Medium'],
        'flood_risk_score': [85, 45, 15, 85, 45]
    })


@pytest.fixture
def sample_market_volatility_data():
    """Create sample market volatility data."""
    return pd.DataFrame({
        'county_fips': ['37183', '37183', '37183', '37135', '37135'],
        'property_type': ['Residential', 'Commercial', 'Industrial', 'Residential', 'Commercial'],
        'land_use_code': ['R1', 'C1', 'I1', 'R1', 'C1'],
        'mean_value': [300000, 800000, 500000, 250000, 600000],
        'value_volatility': [45000, 160000, 75000, 15000, 48000],
        'quarters_analyzed': [8, 8, 8, 8, 8],
        'total_properties': [150, 25, 10, 200, 30],
        'volatility_risk_level': ['High', 'High', 'High', 'Low', 'Low'],
        'volatility_percentage': [15.0, 20.0, 15.0, 6.0, 8.0]
    })


@pytest.fixture
def sample_tax_risk_data():
    """Create sample tax assessment risk data."""
    return pd.DataFrame({
        'parno': ['TR001', 'TR002', 'TR003', 'TR004', 'TR005'],
        'county_fips': ['37183', '37183', '37183', '37183', '37183'],
        'total_value': [300000, 450000, 200000, 750000, 180000],
        'assessed_value': [240000, 360000, 180000, 600000, 162000],
        'land_value': [90000, 135000, 60000, 225000, 54000],
        'improvement_value': [210000, 315000, 140000, 525000, 126000],
        'assessment_date': [datetime(2020, 1, 1), datetime(2019, 1, 1), datetime(2021, 1, 1), 
                           datetime(2018, 1, 1), datetime(2022, 1, 1)],
        'property_type': ['Residential', 'Residential', 'Residential', 'Commercial', 'Residential'],
        'acres': [0.8, 1.2, 0.5, 2.5, 0.6],
        'assessment_ratio': [0.8, 0.8, 0.9, 0.8, 0.9],
        'years_since_assessment': [5, 6, 4, 7, 3],
        'avg_assessment_ratio': [0.85, 0.85, 0.85, 0.82, 0.85],
        'assessment_ratio_std': [0.05, 0.05, 0.05, 0.04, 0.05],
        'tax_increase_risk': ['High', 'High', 'Low', 'Medium', 'Low'],
        'reassessment_risk': ['High', 'High', 'Medium', 'High', 'Low'],
        'potential_assessment_increase': [15000, 22500, 0, 15000, 0]
    })


@pytest.fixture
def sample_comprehensive_risk_data():
    """Create sample data for comprehensive risk assessment."""
    flood_data = pd.DataFrame({
        'parno': ['CR001', 'CR002', 'CR003'],
        'total_value': [300000, 450000, 200000],
        'acres': [0.8, 1.2, 0.5],
        'property_type': ['Residential', 'Commercial', 'Residential'],
        'flood_risk_score': [45, 85, 15],
        'flood_risk_level': ['Medium', 'High', 'Low'],
        'potential_loss_estimate': [40500, 114750, 9000]
    })
    
    tax_data = pd.DataFrame({
        'parno': ['CR001', 'CR002', 'CR003'],
        'tax_risk_score': [60, 40, 20],
        'tax_increase_risk': ['High', 'Medium', 'Low'],
        'potential_assessment_increase': [15000, 10000, 0]
    })
    
    return flood_data, tax_data


class TestRiskAnalytics:
    """Test RiskAnalytics functionality."""
    
    def test_initialization(self, mock_db_manager):
        """Test RiskAnalytics initialization."""
        risk_analytics = RiskAnalytics(mock_db_manager)
        
        assert risk_analytics.db_manager == mock_db_manager
        assert risk_analytics.risk_cache == {}
    
    def test_assess_flood_risk_success(self, mock_db_manager, sample_flood_risk_data):
        """Test successful flood risk assessment."""
        mock_db_manager.execute_query.return_value = sample_flood_risk_data
        
        risk_analytics = RiskAnalytics(mock_db_manager)
        result = risk_analytics.assess_flood_risk(
            parcel_ids=['FR001', 'FR002', 'FR003'],
            county_fips='37183',
            buffer_meters=200
        )
        
        # Verify structure
        assert isinstance(result, pd.DataFrame)
        assert 'parno' in result.columns
        assert 'flood_risk_level' in result.columns
        assert 'flood_risk_score' in result.columns
        assert 'potential_loss_estimate' in result.columns
        assert 'risk_factors' in result.columns
        
        # Verify content
        assert len(result) == 5
        assert result['flood_risk_score'].notna().all()
        assert (result['flood_risk_score'] >= 0).all()
        assert (result['flood_risk_score'] <= 100).all()
        
        # Verify database query was called
        mock_db_manager.execute_query.assert_called_once()
        query_call = mock_db_manager.execute_query.call_args[0][0]
        assert 'FR001' in query_call
        assert '37183' in query_call
    
    def test_assess_flood_risk_no_data(self, mock_db_manager):
        """Test flood risk assessment with no data."""
        mock_db_manager.execute_query.return_value = pd.DataFrame()
        
        risk_analytics = RiskAnalytics(mock_db_manager)
        result = risk_analytics.assess_flood_risk(county_fips='99999')
        
        assert isinstance(result, pd.DataFrame)
        assert len(result) == 0
    
    def test_assess_flood_risk_no_filters(self, mock_db_manager, sample_flood_risk_data):
        """Test flood risk assessment without filters."""
        mock_db_manager.execute_query.return_value = sample_flood_risk_data
        
        risk_analytics = RiskAnalytics(mock_db_manager)
        result = risk_analytics.assess_flood_risk()
        
        # Should work without filters
        assert len(result) > 0
        
        # Verify query doesn't include specific filters
        query_call = mock_db_manager.execute_query.call_args[0][0]
        assert '1=1' in query_call
    
    def test_get_flood_risk_factors(self, mock_db_manager):
        """Test flood risk factors generation."""
        risk_analytics = RiskAnalytics(mock_db_manager)
        
        # Test high risk property
        high_risk_row = pd.Series({
            'elevation_category': 'Low_Elevation',
            'acres': 15.0,
            'property_type': 'Residential'
        })
        factors = risk_analytics._get_flood_risk_factors(high_risk_row)
        
        assert 'Low elevation' in factors
        assert 'Large property area' in factors
        assert 'Developed property' in factors
        
        # Test low risk property
        low_risk_row = pd.Series({
            'elevation_category': 'High_Elevation',
            'acres': 0.5,
            'property_type': 'Agricultural'
        })
        factors = risk_analytics._get_flood_risk_factors(low_risk_row)
        
        assert len(factors) == 0
    
    def test_assess_market_volatility_risk_success(self, mock_db_manager, sample_market_volatility_data):
        """Test successful market volatility risk assessment."""
        mock_db_manager.execute_query.return_value = sample_market_volatility_data
        
        risk_analytics = RiskAnalytics(mock_db_manager)
        result = risk_analytics.assess_market_volatility_risk(
            county_fips='37183',
            analysis_period_months=36
        )
        
        # Verify structure
        assert isinstance(result, pd.DataFrame)
        assert 'county_fips' in result.columns
        assert 'property_type' in result.columns
        assert 'volatility_risk_level' in result.columns
        assert 'volatility_percentage' in result.columns
        assert 'market_risk_score' in result.columns
        
        # Verify content
        assert len(result) == 5
        assert result['market_risk_score'].notna().all()
        assert (result['market_risk_score'] >= 0).all()
        assert (result['market_risk_score'] <= 100).all()
        
        # Verify database query was called
        mock_db_manager.execute_query.assert_called_once()
        query_call = mock_db_manager.execute_query.call_args[0][0]
        assert '37183' in query_call
        assert '36 months' in query_call
    
    def test_assess_market_volatility_risk_no_data(self, mock_db_manager):
        """Test market volatility assessment with no data."""
        mock_db_manager.execute_query.return_value = pd.DataFrame()
        
        risk_analytics = RiskAnalytics(mock_db_manager)
        result = risk_analytics.assess_market_volatility_risk(county_fips='99999')
        
        assert isinstance(result, pd.DataFrame)
        assert len(result) == 0
    
    def test_assess_tax_assessment_risk_success(self, mock_db_manager, sample_tax_risk_data):
        """Test successful tax assessment risk analysis."""
        mock_db_manager.execute_query.return_value = sample_tax_risk_data
        
        risk_analytics = RiskAnalytics(mock_db_manager)
        result = risk_analytics.assess_tax_assessment_risk(
            parcel_ids=['TR001', 'TR002'],
            county_fips='37183'
        )
        
        # Verify structure
        assert isinstance(result, pd.DataFrame)
        assert 'parno' in result.columns
        assert 'tax_increase_risk' in result.columns
        assert 'reassessment_risk' in result.columns
        assert 'tax_risk_score' in result.columns
        assert 'potential_assessment_increase' in result.columns
        
        # Verify content
        assert len(result) == 5
        assert result['tax_risk_score'].notna().all()
        assert (result['tax_risk_score'] >= 0).all()
        assert (result['tax_risk_score'] <= 100).all()
        
        # Verify database query was called
        mock_db_manager.execute_query.assert_called_once()
        query_call = mock_db_manager.execute_query.call_args[0][0]
        assert 'TR001' in query_call
        assert '37183' in query_call
    
    def test_assess_tax_assessment_risk_no_data(self, mock_db_manager):
        """Test tax assessment risk with no data."""
        mock_db_manager.execute_query.return_value = pd.DataFrame()
        
        risk_analytics = RiskAnalytics(mock_db_manager)
        result = risk_analytics.assess_tax_assessment_risk(county_fips='99999')
        
        assert isinstance(result, pd.DataFrame)
        assert len(result) == 0
    
    def test_calculate_tax_risk_score(self, mock_db_manager):
        """Test tax risk score calculation."""
        risk_analytics = RiskAnalytics(mock_db_manager)
        
        # Test high risk scenario
        high_risk_row = pd.Series({
            'tax_increase_risk': 'High',
            'reassessment_risk': 'High',
            'total_value': 600000
        })
        score = risk_analytics._calculate_tax_risk_score(high_risk_row)
        assert score == 90  # 40 + 30 + 20
        
        # Test medium risk scenario
        medium_risk_row = pd.Series({
            'tax_increase_risk': 'Medium',
            'reassessment_risk': 'Medium',
            'total_value': 300000
        })
        score = risk_analytics._calculate_tax_risk_score(medium_risk_row)
        assert score == 45  # 20 + 15 + 10
        
        # Test low risk scenario
        low_risk_row = pd.Series({
            'tax_increase_risk': 'Low',
            'reassessment_risk': 'Low',
            'total_value': 150000
        })
        score = risk_analytics._calculate_tax_risk_score(low_risk_row)
        assert score == 0  # 0 + 0 + 0
        
        # Test maximum possible score (no capping needed in this case)
        extreme_risk_row = pd.Series({
            'tax_increase_risk': 'High',
            'reassessment_risk': 'High',
            'total_value': 2000000
        })
        score = risk_analytics._calculate_tax_risk_score(extreme_risk_row)
        assert score == 90  # 40 + 30 + 20 (max possible with current logic)
    
    def test_comprehensive_risk_assessment_success(self, mock_db_manager, sample_comprehensive_risk_data):
        """Test successful comprehensive risk assessment."""
        flood_data, tax_data = sample_comprehensive_risk_data
        
        risk_analytics = RiskAnalytics(mock_db_manager)
        
        # Mock the individual assessment methods
        risk_analytics.assess_flood_risk = Mock(return_value=flood_data)
        risk_analytics.assess_tax_assessment_risk = Mock(return_value=tax_data)
        
        result = risk_analytics.comprehensive_risk_assessment(
            parcel_ids=['CR001', 'CR002', 'CR003'],
            county_fips='37183'
        )
        
        # Verify structure
        assert isinstance(result, pd.DataFrame)
        assert 'parno' in result.columns
        assert 'composite_risk_score' in result.columns
        assert 'overall_risk_level' in result.columns
        assert 'total_potential_impact' in result.columns
        assert 'risk_recommendations' in result.columns
        assert 'market_risk_score' in result.columns
        
        # Verify content
        assert len(result) == 3
        assert result['composite_risk_score'].notna().all()
        assert (result['composite_risk_score'] >= 0).all()
        assert (result['composite_risk_score'] <= 100).all()
        
        # Verify individual methods were called
        risk_analytics.assess_flood_risk.assert_called_once_with(['CR001', 'CR002', 'CR003'], '37183')
        risk_analytics.assess_tax_assessment_risk.assert_called_once_with(['CR001', 'CR002', 'CR003'], '37183')
    
    def test_comprehensive_risk_assessment_flood_only(self, mock_db_manager, sample_comprehensive_risk_data):
        """Test comprehensive risk assessment with only flood data."""
        flood_data, _ = sample_comprehensive_risk_data
        
        risk_analytics = RiskAnalytics(mock_db_manager)
        
        # Mock methods - only flood data available
        risk_analytics.assess_flood_risk = Mock(return_value=flood_data)
        risk_analytics.assess_tax_assessment_risk = Mock(return_value=pd.DataFrame())
        
        result = risk_analytics.comprehensive_risk_assessment(county_fips='37183')
        
        # Should still work with only flood data
        assert len(result) == 3
        assert 'tax_risk_score' in result.columns
        assert (result['tax_risk_score'] == 0).all()
        assert (result['tax_increase_risk'] == 'Unknown').all()
    
    def test_comprehensive_risk_assessment_tax_only(self, mock_db_manager, sample_comprehensive_risk_data):
        """Test comprehensive risk assessment with only tax data."""
        _, tax_data = sample_comprehensive_risk_data
        
        risk_analytics = RiskAnalytics(mock_db_manager)
        
        # Mock methods - only tax data available
        risk_analytics.assess_flood_risk = Mock(return_value=pd.DataFrame())
        risk_analytics.assess_tax_assessment_risk = Mock(return_value=tax_data)
        
        result = risk_analytics.comprehensive_risk_assessment(county_fips='37183')
        
        # Should still work with only tax data
        assert len(result) == 3
        assert 'flood_risk_score' in result.columns
        assert (result['flood_risk_score'] == 0).all()
        assert (result['flood_risk_level'] == 'Unknown').all()
    
    def test_comprehensive_risk_assessment_no_data(self, mock_db_manager):
        """Test comprehensive risk assessment with no data."""
        risk_analytics = RiskAnalytics(mock_db_manager)
        
        # Mock methods to return empty DataFrames
        risk_analytics.assess_flood_risk = Mock(return_value=pd.DataFrame())
        risk_analytics.assess_tax_assessment_risk = Mock(return_value=pd.DataFrame())
        
        result = risk_analytics.comprehensive_risk_assessment(county_fips='99999')
        
        assert isinstance(result, pd.DataFrame)
        assert len(result) == 0
    
    def test_estimate_market_risk(self, mock_db_manager):
        """Test market risk estimation."""
        risk_analytics = RiskAnalytics(mock_db_manager)
        
        # Test high risk commercial property
        high_risk_row = pd.Series({
            'property_type': 'Commercial',
            'total_value': 1500000,
            'acres': 60
        })
        risk = risk_analytics._estimate_market_risk(high_risk_row)
        assert risk == 80  # 20 + 20 + 25 + 15
        
        # Test medium risk residential property
        medium_risk_row = pd.Series({
            'property_type': 'Residential',
            'total_value': 600000,
            'acres': 1.0
        })
        risk = risk_analytics._estimate_market_risk(medium_risk_row)
        assert risk == 35  # 20 + 15
        
        # Test low value high risk
        low_value_row = pd.Series({
            'property_type': 'Residential',
            'total_value': 30000,
            'acres': 0.5
        })
        risk = risk_analytics._estimate_market_risk(low_value_row)
        assert risk == 40  # 20 + 20 (low value penalty)
        
        # Test industrial property
        industrial_row = pd.Series({
            'property_type': 'Industrial',
            'total_value': 800000,
            'acres': 5.0
        })
        risk = risk_analytics._estimate_market_risk(industrial_row)
        assert risk == 65  # 20 + 30 + 15
        
        # Test risk capping
        extreme_row = pd.Series({
            'property_type': 'Industrial',
            'total_value': 2000000,
            'acres': 100
        })
        risk = risk_analytics._estimate_market_risk(extreme_row)
        assert risk == 90  # 20 + 30 + 25 + 15 (max possible with current logic)
    
    def test_generate_risk_recommendations(self, mock_db_manager):
        """Test risk recommendations generation."""
        risk_analytics = RiskAnalytics(mock_db_manager)
        
        # Test high risk property
        high_risk_row = pd.Series({
            'flood_risk_level': 'High',
            'tax_increase_risk': 'High',
            'market_risk_score': 70,
            'composite_risk_score': 80
        })
        recommendations = risk_analytics._generate_risk_recommendations(high_risk_row)
        
        assert 'Consider flood insurance' in recommendations
        assert 'Budget for potential tax increases' in recommendations
        assert 'Diversify property portfolio' in recommendations
        assert 'Consider professional risk assessment' in recommendations
        
        # Test low risk property
        low_risk_row = pd.Series({
            'flood_risk_level': 'Low',
            'tax_increase_risk': 'Low',
            'market_risk_score': 30,
            'composite_risk_score': 25
        })
        recommendations = risk_analytics._generate_risk_recommendations(low_risk_row)
        
        assert len(recommendations) == 0
        
        # Test medium risk property
        medium_risk_row = pd.Series({
            'flood_risk_level': 'Medium',
            'tax_increase_risk': 'Medium',
            'market_risk_score': 50,
            'composite_risk_score': 50
        })
        recommendations = risk_analytics._generate_risk_recommendations(medium_risk_row)
        
        # Should have no recommendations for medium risk levels
        assert len(recommendations) == 0


class TestRiskAnalyticsEdgeCases:
    """Test edge cases and error handling."""
    
    def test_assess_flood_risk_database_error(self, mock_db_manager):
        """Test handling of database errors in flood risk assessment."""
        mock_db_manager.execute_query.side_effect = Exception("Database connection failed")
        
        risk_analytics = RiskAnalytics(mock_db_manager)
        
        with pytest.raises(Exception, match="Database connection failed"):
            risk_analytics.assess_flood_risk()
    
    def test_assess_market_volatility_risk_database_error(self, mock_db_manager):
        """Test handling of database errors in market volatility assessment."""
        mock_db_manager.execute_query.side_effect = Exception("Query execution failed")
        
        risk_analytics = RiskAnalytics(mock_db_manager)
        
        with pytest.raises(Exception, match="Query execution failed"):
            risk_analytics.assess_market_volatility_risk()
    
    def test_assess_tax_assessment_risk_database_error(self, mock_db_manager):
        """Test handling of database errors in tax assessment risk."""
        mock_db_manager.execute_query.side_effect = Exception("Tax query failed")
        
        risk_analytics = RiskAnalytics(mock_db_manager)
        
        with pytest.raises(Exception, match="Tax query failed"):
            risk_analytics.assess_tax_assessment_risk()
    
    def test_comprehensive_risk_assessment_database_error(self, mock_db_manager):
        """Test handling of database errors in comprehensive assessment."""
        risk_analytics = RiskAnalytics(mock_db_manager)
        
        # Mock methods to raise exceptions
        risk_analytics.assess_flood_risk = Mock(side_effect=Exception("Flood assessment failed"))
        risk_analytics.assess_tax_assessment_risk = Mock(return_value=pd.DataFrame())
        
        with pytest.raises(Exception, match="Flood assessment failed"):
            risk_analytics.comprehensive_risk_assessment()
    
    def test_flood_risk_with_missing_data(self, mock_db_manager):
        """Test flood risk assessment with missing data fields."""
        # Create data with missing values
        incomplete_data = pd.DataFrame({
            'parno': ['FR001', 'FR002'],
            'total_value': [250000, None],
            'acres': [0.5, 2.0],
            'property_type': ['Residential', None],
            'longitude': [-78.8, -78.9],
            'latitude': [34.8, 35.5],
            'elevation_category': ['Low_Elevation', 'Medium_Elevation'],
            'flood_risk_level': ['High', 'Medium'],
            'flood_risk_score': [85, 45]
        })
        
        mock_db_manager.execute_query.return_value = incomplete_data
        
        risk_analytics = RiskAnalytics(mock_db_manager)
        result = risk_analytics.assess_flood_risk()
        
        # Should handle missing data gracefully
        assert len(result) == 2
        assert 'risk_factors' in result.columns
        assert 'potential_loss_estimate' in result.columns
    
    def test_tax_risk_score_edge_cases(self, mock_db_manager):
        """Test tax risk score calculation with edge cases."""
        risk_analytics = RiskAnalytics(mock_db_manager)
        
        # Test with missing values - should raise TypeError due to None comparison
        missing_data_row = pd.Series({
            'tax_increase_risk': None,
            'reassessment_risk': None,
            'total_value': None
        })
        with pytest.raises(TypeError):
            risk_analytics._calculate_tax_risk_score(missing_data_row)
        
        # Test with unexpected values but valid total_value
        unexpected_row = pd.Series({
            'tax_increase_risk': 'Unknown',
            'reassessment_risk': 'Unknown',
            'total_value': -100000  # Negative value
        })
        score = risk_analytics._calculate_tax_risk_score(unexpected_row)
        assert score == 0  # No matching conditions, so score should be 0
        
        # Test with valid total_value but None risk levels
        partial_none_row = pd.Series({
            'tax_increase_risk': None,
            'reassessment_risk': None,
            'total_value': 300000  # Valid value
        })
        score = risk_analytics._calculate_tax_risk_score(partial_none_row)
        assert score == 10  # Only gets points for total_value > 200000


if __name__ == "__main__":
    pytest.main([__file__]) 