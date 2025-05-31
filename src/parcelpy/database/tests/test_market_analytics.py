#!/usr/bin/env python3
"""
Tests for Market Analytics module.

These tests verify the market analytics functionality works correctly
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
sys.path.append(str(Path(__file__).parent.parent.parent))

from database.core.market_analytics import MarketAnalytics


@pytest.fixture
def mock_db_manager():
    """Create a mock database manager for testing."""
    mock_db = Mock()
    mock_db.execute_query = Mock()
    mock_db.execute_spatial_query = Mock()
    return mock_db


@pytest.fixture
def sample_market_data():
    """Create sample market trend data."""
    dates = pd.date_range('2023-01-01', '2024-12-01', freq='MS')  # Monthly start
    data = []
    
    for i, date in enumerate(dates):
        # Create realistic market data with some trends
        base_value = 250000 + (i * 2000)  # Gradual increase
        noise = np.random.normal(0, 10000)  # Add some noise
        
        data.append({
            'month': date,
            'property_count': 50 + np.random.randint(-10, 10),
            'avg_total_value': base_value + noise,
            'median_total_value': base_value * 0.95 + noise,
            'avg_land_value': base_value * 0.3,
            'avg_improvement_value': base_value * 0.7,
            'value_std': 50000,
            'property_type': 'Residential',
            'county_fips': '37183'
        })
    
    return pd.DataFrame(data)


@pytest.fixture
def sample_property_data():
    """Create sample property data for testing."""
    return pd.DataFrame({
        'parno': ['P001', 'P002', 'P003', 'P004', 'P005'],
        'land_value': [50000, 75000, 60000, 80000, 45000],
        'improvement_value': [150000, 225000, 180000, 240000, 135000],
        'total_value': [200000, 300000, 240000, 320000, 180000],
        'acres': [0.5, 1.0, 0.75, 1.2, 0.4],
        'square_feet': [1500, 2500, 2000, 2800, 1200],
        'property_type': ['Residential', 'Residential', 'Commercial', 'Residential', 'Residential'],
        'land_use_code': ['R1', 'R1', 'C1', 'R1', 'R1'],
        'assessment_year': [2023, 2023, 2023, 2023, 2023],
        'longitude': [-78.8, -78.9, -78.7, -78.85, -78.95],
        'latitude': [35.8, 35.9, 35.7, 35.85, 35.95],
        'actual_value': [200000, 300000, 240000, 320000, 180000]
    })


@pytest.fixture
def sample_cma_data():
    """Create sample data for comparative market analysis."""
    target_data = pd.DataFrame({
        'parno': ['TARGET001'],
        'geometry': ['POINT(-78.8 35.8)'],
        'total_value': [250000],
        'land_value': [75000],
        'improvement_value': [175000],
        'acres': [0.8],
        'square_feet': [2000],
        'property_type': ['Residential'],
        'assessment_date': [datetime.now()]
    })
    
    # Mock geometry object
    mock_geometry = Mock()
    mock_geometry.wkt = 'POINT(-78.8 35.8)'
    target_data.loc[0, 'geometry'] = mock_geometry
    
    comparables_data = pd.DataFrame({
        'parno': ['COMP001', 'COMP002', 'COMP003'],
        'total_value': [240000, 260000, 245000],
        'land_value': [70000, 80000, 72000],
        'improvement_value': [170000, 180000, 173000],
        'acres': [0.7, 0.9, 0.75],
        'square_feet': [1900, 2100, 1950],
        'property_type': ['Residential', 'Residential', 'Residential'],
        'assessment_date': [datetime.now(), datetime.now(), datetime.now()],
        'distance_meters': [150.0, 300.0, 200.0]
    })
    
    return target_data, comparables_data


class TestMarketAnalytics:
    """Test MarketAnalytics functionality."""
    
    def test_initialization(self, mock_db_manager):
        """Test MarketAnalytics initialization."""
        analytics = MarketAnalytics(mock_db_manager)
        
        assert analytics.db_manager == mock_db_manager
        assert analytics.models == {}
        assert analytics.scalers == {}
    
    def test_analyze_market_trends_success(self, mock_db_manager, sample_market_data):
        """Test successful market trend analysis."""
        mock_db_manager.execute_query.return_value = sample_market_data
        
        analytics = MarketAnalytics(mock_db_manager)
        result = analytics.analyze_market_trends(
            county_fips='37183',
            time_period_months=12,
            property_types=['Residential']
        )
        
        # Verify structure
        assert 'period_analyzed' in result
        assert 'total_properties' in result
        assert 'date_range' in result
        assert 'trends_by_type' in result
        assert 'overall_trends' in result
        
        # Verify content
        assert result['period_analyzed'] == '12 months'
        assert result['total_properties'] > 0
        assert 'Residential' in result['trends_by_type']
        
        # Verify overall trends calculations
        overall = result['overall_trends']
        assert 'avg_monthly_value_growth' in overall
        assert 'current_avg_value' in overall
        assert 'value_volatility' in overall
        
        # Verify database query was called
        mock_db_manager.execute_query.assert_called_once()
        query_call = mock_db_manager.execute_query.call_args[0][0]
        assert 'county_fips' in query_call
        assert '37183' in query_call
        assert 'Residential' in query_call
    
    def test_analyze_market_trends_no_data(self, mock_db_manager):
        """Test market trend analysis with no data."""
        mock_db_manager.execute_query.return_value = pd.DataFrame()
        
        analytics = MarketAnalytics(mock_db_manager)
        result = analytics.analyze_market_trends(county_fips='99999')
        
        assert 'error' in result
        assert result['error'] == "No data found for specified criteria"
    
    def test_analyze_market_trends_no_filters(self, mock_db_manager, sample_market_data):
        """Test market trend analysis without filters."""
        mock_db_manager.execute_query.return_value = sample_market_data
        
        analytics = MarketAnalytics(mock_db_manager)
        result = analytics.analyze_market_trends()
        
        # Should work without filters
        assert 'overall_trends' in result
        
        # Verify query was called (county_fips appears in SELECT but not in WHERE filters)
        query_call = mock_db_manager.execute_query.call_args[0][0]
        # The query should have a date filter but no county or property type filters
        assert 'assessment_date >=' in query_call
        # Should not have county_fips filter in WHERE clause
        assert "county_fips = '" not in query_call
    
    @patch('database.core.market_analytics.SKLEARN_AVAILABLE', True)
    def test_build_valuation_model_sklearn_available(self, mock_db_manager, sample_property_data):
        """Test building valuation model when sklearn is available."""
        # Create larger dataset to meet minimum requirements
        large_sample_data = pd.concat([sample_property_data] * 25, ignore_index=True)  # 125 records
        # Make parno unique
        large_sample_data['parno'] = [f'P{i:03d}' for i in range(len(large_sample_data))]
        
        mock_db_manager.execute_query.return_value = large_sample_data
        
        with patch('database.core.market_analytics.RandomForestRegressor') as mock_rf:
            with patch('database.core.market_analytics.train_test_split') as mock_split:
                with patch('database.core.market_analytics.mean_absolute_error') as mock_mae:
                    with patch('database.core.market_analytics.mean_squared_error') as mock_mse:
                        with patch('database.core.market_analytics.r2_score') as mock_r2:
                            
                            # Setup mocks
                            mock_model = Mock()
                            mock_rf.return_value = mock_model
                            
                            # Create proper train/test split data
                            features = large_sample_data[['land_value', 'improvement_value', 'acres', 'square_feet']]
                            target = large_sample_data['total_value']
                            
                            X_train = features.iloc[:100]
                            X_test = features.iloc[100:]
                            y_train = target.iloc[:100]
                            y_test = target.iloc[100:]
                            
                            mock_split.return_value = (X_train, X_test, y_train, y_test)
                            
                            # Mock model predictions to return actual arrays
                            y_pred = np.array([250000] * len(y_test))  # Realistic predictions
                            mock_model.predict.return_value = y_pred
                            
                            # Mock feature importances for tree-based models
                            mock_model.feature_importances_ = np.array([0.4, 0.3, 0.2, 0.1])  # 4 features
                            
                            # Mock metric functions
                            mock_mae.return_value = 15000.0
                            mock_mse.return_value = 500000000.0  # MSE value
                            mock_r2.return_value = 0.85
                            
                            analytics = MarketAnalytics(mock_db_manager)
                            result = analytics.build_valuation_model(
                                target_column='total_value',
                                model_type='random_forest',
                                county_fips='37183'
                            )
                            
                            # Verify results
                            assert 'model_key' in result
                            assert 'performance' in result
                            assert 'training_samples' in result
                            assert 'test_samples' in result
                            
                            # Verify model was stored
                            model_key = result['model_key']
                            assert model_key in analytics.models
                            
                            # Verify performance metrics
                            perf = result['performance']
                            assert 'mae' in perf
                            assert 'r2_score' in perf
                            assert 'rmse' in perf
                            assert 'mse' in perf
    
    @patch('database.core.market_analytics.SKLEARN_AVAILABLE', False)
    def test_build_valuation_model_sklearn_not_available(self, mock_db_manager):
        """Test building valuation model when sklearn is not available."""
        analytics = MarketAnalytics(mock_db_manager)
        
        with pytest.raises(ImportError, match="scikit-learn is required"):
            analytics.build_valuation_model()
    
    def test_predict_property_values_success(self, mock_db_manager, sample_property_data):
        """Test successful property value prediction."""
        # Filter sample data to only include the parcels we're predicting
        filtered_data = sample_property_data[sample_property_data['parno'].isin(['P001', 'P002', 'P003'])].copy()
        mock_db_manager.execute_query.return_value = filtered_data
        
        # Setup analytics with a mock model
        analytics = MarketAnalytics(mock_db_manager)
        mock_model = Mock()
        mock_model.predict.return_value = [210000, 290000, 250000]  # 3 predictions for 3 parcels
        analytics.models['test_model'] = mock_model
        
        result = analytics.predict_property_values(
            parcel_ids=['P001', 'P002', 'P003'],
            model_key='test_model'
        )
        
        # Verify results structure
        assert isinstance(result, pd.DataFrame)
        assert 'parno' in result.columns
        assert 'predicted_value' in result.columns
        assert 'prediction_error' in result.columns
        assert 'percentage_error' in result.columns
        
        # Verify predictions were made
        assert len(result) == 3
        assert result['predicted_value'].notna().all()
    
    def test_predict_property_values_model_not_found(self, mock_db_manager):
        """Test prediction with non-existent model."""
        analytics = MarketAnalytics(mock_db_manager)
        
        with pytest.raises(ValueError, match="Model nonexistent_model not found"):
            analytics.predict_property_values(
                parcel_ids=['P001'],
                model_key='nonexistent_model'
            )
    
    def test_predict_property_values_no_data(self, mock_db_manager):
        """Test prediction with no parcel data found."""
        mock_db_manager.execute_query.return_value = pd.DataFrame()
        
        analytics = MarketAnalytics(mock_db_manager)
        analytics.models['test_model'] = Mock()
        
        with pytest.raises(ValueError, match="No data found for specified parcels"):
            analytics.predict_property_values(
                parcel_ids=['NONEXISTENT'],
                model_key='test_model'
            )
    
    def test_comparative_market_analysis_success(self, mock_db_manager, sample_cma_data):
        """Test successful comparative market analysis."""
        target_data, comparables_data = sample_cma_data
        
        # Setup mock returns
        mock_db_manager.execute_spatial_query.return_value = target_data
        mock_db_manager.execute_query.return_value = comparables_data
        
        analytics = MarketAnalytics(mock_db_manager)
        result = analytics.comparative_market_analysis(
            target_parcel_id='TARGET001',
            radius_meters=500,
            max_comparables=5
        )
        
        # Verify structure
        assert 'target_parcel' in result
        assert 'comparables' in result
        assert 'analysis' in result
        assert 'comparable_properties' in result
        
        # Verify target parcel info
        target = result['target_parcel']
        assert target['parno'] == 'TARGET001'
        assert target['current_value'] == 250000
        
        # Verify comparables statistics
        comps = result['comparables']
        assert comps['count'] == 3
        assert 'value_statistics' in comps
        
        # Verify analysis metrics
        analysis = result['analysis']
        assert 'target_vs_mean_diff' in analysis
        assert 'target_vs_mean_pct' in analysis
        assert 'target_percentile' in analysis
        assert 'suggested_value_range' in analysis
    
    def test_comparative_market_analysis_target_not_found(self, mock_db_manager):
        """Test CMA with target parcel not found."""
        mock_db_manager.execute_spatial_query.return_value = pd.DataFrame()
        
        analytics = MarketAnalytics(mock_db_manager)
        
        with pytest.raises(ValueError, match="Target parcel NONEXISTENT not found"):
            analytics.comparative_market_analysis(target_parcel_id='NONEXISTENT')
    
    def test_comparative_market_analysis_no_comparables(self, mock_db_manager, sample_cma_data):
        """Test CMA with no comparable properties found."""
        target_data, _ = sample_cma_data
        
        mock_db_manager.execute_spatial_query.return_value = target_data
        mock_db_manager.execute_query.return_value = pd.DataFrame()  # No comparables
        
        analytics = MarketAnalytics(mock_db_manager)
        result = analytics.comparative_market_analysis(target_parcel_id='TARGET001')
        
        assert 'error' in result
        assert 'No comparable properties found' in result['error']
    
    def test_investment_opportunity_scoring_success(self, mock_db_manager):
        """Test successful investment opportunity scoring."""
        # Create sample investment data
        investment_data = pd.DataFrame({
            'parno': ['INV001', 'INV002', 'INV003', 'INV004'],
            'county_fips': ['37183', '37183', '37183', '37183'],
            'total_value': [200000, 300000, 150000, 400000],
            'land_value': [80000, 90000, 70000, 120000],
            'improvement_value': [120000, 210000, 80000, 280000],
            'acres': [1.0, 0.5, 2.0, 0.8],
            'property_type': ['Residential', 'Residential', 'Commercial', 'Residential'],
            'assessment_date': [datetime.now()] * 4,
            'longitude': [-78.8, -78.9, -78.7, -78.85],
            'latitude': [35.8, 35.9, 35.7, 35.85]
        })
        
        mock_db_manager.execute_query.return_value = investment_data
        
        analytics = MarketAnalytics(mock_db_manager)
        result = analytics.investment_opportunity_scoring(
            county_fips='37183',
            min_value=100000,
            max_value=500000,
            limit=3
        )
        
        # Verify results structure
        assert isinstance(result, pd.DataFrame)
        assert 'parno' in result.columns
        assert 'investment_score' in result.columns
        assert 'value_per_acre' in result.columns
        assert 'land_ratio' in result.columns
        
        # Verify scoring calculations
        assert len(result) <= 3  # Respects limit
        assert result['investment_score'].notna().all()
        assert (result['investment_score'] >= 0).all()
        assert (result['investment_score'] <= 100).all()
        
        # Verify results are sorted by investment score (descending)
        assert result['investment_score'].is_monotonic_decreasing
    
    def test_investment_opportunity_scoring_no_data(self, mock_db_manager):
        """Test investment scoring with no data."""
        mock_db_manager.execute_query.return_value = pd.DataFrame()
        
        analytics = MarketAnalytics(mock_db_manager)
        
        with pytest.raises(ValueError, match="No properties found matching criteria"):
            analytics.investment_opportunity_scoring(county_fips='99999')
    
    def test_investment_opportunity_scoring_with_filters(self, mock_db_manager):
        """Test investment scoring with various filters."""
        # Create sample data
        investment_data = pd.DataFrame({
            'parno': ['INV001', 'INV002'],
            'county_fips': ['37183', '37183'],
            'total_value': [250000, 350000],
            'land_value': [75000, 105000],
            'improvement_value': [175000, 245000],
            'acres': [1.0, 1.5],
            'property_type': ['Residential', 'Residential'],
            'assessment_date': [datetime.now()] * 2,
            'longitude': [-78.8, -78.9],
            'latitude': [35.8, 35.9]
        })
        
        mock_db_manager.execute_query.return_value = investment_data
        
        analytics = MarketAnalytics(mock_db_manager)
        result = analytics.investment_opportunity_scoring(
            county_fips='37183',
            min_value=200000,
            max_value=400000,
            limit=10
        )
        
        # Verify query was called with filters
        query_call = mock_db_manager.execute_query.call_args[0][0]
        assert '37183' in query_call
        assert '200000' in query_call
        assert '400000' in query_call
        
        # Verify results
        assert len(result) == 2


class TestMarketAnalyticsEdgeCases:
    """Test edge cases and error handling."""
    
    def test_analyze_market_trends_database_error(self, mock_db_manager):
        """Test handling of database errors in trend analysis."""
        mock_db_manager.execute_query.side_effect = Exception("Database connection failed")
        
        analytics = MarketAnalytics(mock_db_manager)
        
        with pytest.raises(Exception, match="Database connection failed"):
            analytics.analyze_market_trends()
    
    def test_comparative_market_analysis_database_error(self, mock_db_manager):
        """Test handling of database errors in CMA."""
        mock_db_manager.execute_spatial_query.side_effect = Exception("Spatial query failed")
        
        analytics = MarketAnalytics(mock_db_manager)
        
        with pytest.raises(Exception, match="Spatial query failed"):
            analytics.comparative_market_analysis(target_parcel_id='TEST001')
    
    def test_investment_scoring_database_error(self, mock_db_manager):
        """Test handling of database errors in investment scoring."""
        mock_db_manager.execute_query.side_effect = Exception("Query execution failed")
        
        analytics = MarketAnalytics(mock_db_manager)
        
        with pytest.raises(Exception, match="Query execution failed"):
            analytics.investment_opportunity_scoring()
    
    def test_market_trends_with_missing_property_types(self, mock_db_manager):
        """Test market trends with missing property type data."""
        # Create data with some NaN property types
        sample_data = pd.DataFrame({
            'month': [datetime.now()],
            'property_count': [10],
            'avg_total_value': [250000],
            'median_total_value': [240000],
            'avg_land_value': [75000],
            'avg_improvement_value': [175000],
            'value_std': [50000],
            'property_type': [None],  # Missing property type
            'county_fips': ['37183']
        })
        
        mock_db_manager.execute_query.return_value = sample_data
        
        analytics = MarketAnalytics(mock_db_manager)
        result = analytics.analyze_market_trends()
        
        # Should handle missing property types gracefully
        assert 'overall_trends' in result
        # NaN property types should be skipped in trends_by_type
        assert len(result['trends_by_type']) == 0


if __name__ == "__main__":
    pytest.main([__file__]) 