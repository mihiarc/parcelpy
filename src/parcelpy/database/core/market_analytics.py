"""
Market Analytics Module for ParcelPy

Provides advanced market analysis, property valuation trends, and predictive analytics
for real estate market intelligence.
"""

import logging
import pandas as pd
import numpy as np
from typing import Dict, List, Optional, Tuple, Any, Union
from datetime import datetime, timedelta
import geopandas as gpd
from pathlib import Path

try:
    from sklearn.ensemble import RandomForestRegressor, GradientBoostingRegressor
    from sklearn.linear_model import LinearRegression
    from sklearn.model_selection import train_test_split, cross_val_score
    from sklearn.metrics import mean_absolute_error, mean_squared_error, r2_score
    from sklearn.preprocessing import StandardScaler, LabelEncoder
    SKLEARN_AVAILABLE = True
except ImportError:
    SKLEARN_AVAILABLE = False

try:
    import plotly.express as px
    import plotly.graph_objects as go
    from plotly.subplots import make_subplots
    PLOTLY_AVAILABLE = True
except ImportError:
    PLOTLY_AVAILABLE = False

from .database_manager import DatabaseManager

logger = logging.getLogger(__name__)


class MarketAnalytics:
    """
    Advanced market analytics for property valuation and market intelligence.
    
    Provides:
    - Property value trend analysis
    - Market segmentation and clustering
    - Predictive valuation models
    - Comparative market analysis (CMA)
    - Investment opportunity scoring
    """
    
    def __init__(self, db_manager: DatabaseManager):
        """
        Initialize market analytics.
        
        Args:
            db_manager: Database manager instance
        """
        self.db_manager = db_manager
        self.models = {}
        self.scalers = {}
        
    def analyze_market_trends(self, 
                            county_fips: Optional[str] = None,
                            time_period_months: int = 24,
                            property_types: Optional[List[str]] = None) -> Dict[str, Any]:
        """
        Analyze property value trends over time.
        
        Args:
            county_fips: Optional county filter
            time_period_months: Analysis period in months
            property_types: Optional property type filter
            
        Returns:
            Dictionary with trend analysis results
        """
        try:
            # Build query filters
            where_conditions = []
            if county_fips:
                where_conditions.append(f"p.county_fips = '{county_fips}'")
            
            if property_types:
                type_list = "', '".join(property_types)
                where_conditions.append(f"pi.property_type IN ('{type_list}')")
            
            # Date filter for recent data
            cutoff_date = datetime.now() - timedelta(days=time_period_months * 30)
            where_conditions.append(f"pv.assessment_date >= '{cutoff_date.strftime('%Y-%m-%d')}'")
            
            where_clause = " AND ".join(where_conditions) if where_conditions else "1=1"
            
            query = f"""
            SELECT 
                DATE_TRUNC('month', pv.assessment_date) as month,
                COUNT(*) as property_count,
                AVG(pv.total_value) as avg_total_value,
                MEDIAN(pv.total_value) as median_total_value,
                AVG(pv.land_value) as avg_land_value,
                AVG(pv.improvement_value) as avg_improvement_value,
                STDDEV(pv.total_value) as value_std,
                pi.property_type,
                p.county_fips
            FROM parcel p
            JOIN property_values pv ON p.parno = pv.parno
            JOIN property_info pi ON p.parno = pi.parno
            WHERE {where_clause}
              AND pv.total_value > 0
            GROUP BY DATE_TRUNC('month', pv.assessment_date), pi.property_type, p.county_fips
            ORDER BY month, pi.property_type
            """
            
            trends_df = self.db_manager.execute_query(query)
            
            if trends_df.empty:
                return {"error": "No data found for specified criteria"}
            
            # Calculate trend metrics
            analysis_results = {
                "period_analyzed": f"{time_period_months} months",
                "total_properties": int(trends_df['property_count'].sum()),
                "date_range": {
                    "start": trends_df['month'].min().strftime('%Y-%m-%d'),
                    "end": trends_df['month'].max().strftime('%Y-%m-%d')
                },
                "trends_by_type": {},
                "overall_trends": {}
            }
            
            # Overall market trends
            overall_trends = trends_df.groupby('month').agg({
                'property_count': 'sum',
                'avg_total_value': 'mean',
                'median_total_value': 'mean'
            }).reset_index()
            
            # Calculate month-over-month growth
            overall_trends['value_growth_rate'] = overall_trends['avg_total_value'].pct_change()
            overall_trends['volume_growth_rate'] = overall_trends['property_count'].pct_change()
            
            analysis_results["overall_trends"] = {
                "avg_monthly_value_growth": float(overall_trends['value_growth_rate'].mean()),
                "avg_monthly_volume_growth": float(overall_trends['volume_growth_rate'].mean()),
                "current_avg_value": float(overall_trends['avg_total_value'].iloc[-1]),
                "value_volatility": float(overall_trends['value_growth_rate'].std())
            }
            
            # Trends by property type
            for prop_type in trends_df['property_type'].unique():
                if pd.isna(prop_type):
                    continue
                    
                type_data = trends_df[trends_df['property_type'] == prop_type].copy()
                type_data = type_data.sort_values('month')
                
                type_data['value_growth_rate'] = type_data['avg_total_value'].pct_change()
                
                analysis_results["trends_by_type"][prop_type] = {
                    "property_count": int(type_data['property_count'].sum()),
                    "avg_value": float(type_data['avg_total_value'].mean()),
                    "avg_growth_rate": float(type_data['value_growth_rate'].mean()),
                    "value_range": {
                        "min": float(type_data['avg_total_value'].min()),
                        "max": float(type_data['avg_total_value'].max())
                    }
                }
            
            logger.info(f"Market trend analysis completed for {len(trends_df)} data points")
            return analysis_results
            
        except Exception as e:
            logger.error(f"Failed to analyze market trends: {e}")
            raise
    
    def build_valuation_model(self,
                            target_column: str = "total_value",
                            feature_columns: Optional[List[str]] = None,
                            model_type: str = "random_forest",
                            county_fips: Optional[str] = None,
                            test_size: float = 0.2) -> Dict[str, Any]:
        """
        Build a predictive model for property valuation.
        
        Args:
            target_column: Column to predict
            feature_columns: Features to use for prediction
            model_type: Type of model ('random_forest', 'gradient_boost', 'linear')
            county_fips: Optional county filter
            test_size: Fraction of data for testing
            
        Returns:
            Dictionary with model performance metrics
        """
        if not SKLEARN_AVAILABLE:
            raise ImportError("scikit-learn is required for predictive modeling")
        
        try:
            # Default feature columns if not specified
            if feature_columns is None:
                feature_columns = [
                    'land_value', 'improvement_value', 'acres', 'square_feet'
                ]
            
            # Build query to get training data
            where_clause = "pv.total_value > 0"
            if county_fips:
                where_clause += f" AND p.county_fips = '{county_fips}'"
            
            query = f"""
            SELECT 
                pv.{target_column},
                pv.land_value,
                pv.improvement_value,
                pi.acres,
                pi.square_feet,
                pi.property_type,
                pi.land_use_code,
                EXTRACT(YEAR FROM pv.assessment_date) as assessment_year,
                ST_X(ST_Centroid(p.geometry)) as longitude,
                ST_Y(ST_Centroid(p.geometry)) as latitude
            FROM parcel p
            JOIN property_values pv ON p.parno = pv.parno
            JOIN property_info pi ON p.parno = pi.parno
            WHERE {where_clause}
              AND pv.land_value IS NOT NULL
              AND pv.improvement_value IS NOT NULL
            """
            
            data = self.db_manager.execute_query(query)
            
            if data.empty or len(data) < 100:
                raise ValueError("Insufficient data for model training (need at least 100 records)")
            
            # Prepare features
            X = data[feature_columns].copy()
            y = data[target_column]
            
            # Handle categorical variables
            categorical_cols = X.select_dtypes(include=['object']).columns
            label_encoders = {}
            
            for col in categorical_cols:
                if col in X.columns:
                    le = LabelEncoder()
                    X[col] = le.fit_transform(X[col].fillna('Unknown'))
                    label_encoders[col] = le
            
            # Fill missing values
            X = X.fillna(X.median())
            
            # Split data
            X_train, X_test, y_train, y_test = train_test_split(
                X, y, test_size=test_size, random_state=42
            )
            
            # Scale features
            scaler = StandardScaler()
            X_train_scaled = scaler.fit_transform(X_train)
            X_test_scaled = scaler.transform(X_test)
            
            # Select and train model
            if model_type == "random_forest":
                model = RandomForestRegressor(n_estimators=100, random_state=42)
            elif model_type == "gradient_boost":
                model = GradientBoostingRegressor(n_estimators=100, random_state=42)
            elif model_type == "linear":
                model = LinearRegression()
            else:
                raise ValueError(f"Unknown model type: {model_type}")
            
            # Train model
            if model_type == "linear":
                model.fit(X_train_scaled, y_train)
                y_pred = model.predict(X_test_scaled)
            else:
                model.fit(X_train, y_train)
                y_pred = model.predict(X_test)
            
            # Calculate metrics
            mae = mean_absolute_error(y_test, y_pred)
            mse = mean_squared_error(y_test, y_pred)
            rmse = np.sqrt(mse)
            r2 = r2_score(y_test, y_pred)
            
            # Feature importance (for tree-based models)
            feature_importance = None
            if hasattr(model, 'feature_importances_'):
                feature_importance = dict(zip(feature_columns, model.feature_importances_))
                feature_importance = dict(sorted(feature_importance.items(), 
                                               key=lambda x: x[1], reverse=True))
            
            # Store model and scaler
            model_key = f"{model_type}_{target_column}_{county_fips or 'all'}"
            self.models[model_key] = model
            self.scalers[model_key] = scaler
            
            results = {
                "model_type": model_type,
                "target_column": target_column,
                "training_samples": len(X_train),
                "test_samples": len(X_test),
                "performance": {
                    "mae": float(mae),
                    "mse": float(mse),
                    "rmse": float(rmse),
                    "r2_score": float(r2),
                    "mean_absolute_percentage_error": float(np.mean(np.abs((y_test - y_pred) / y_test)) * 100)
                },
                "feature_importance": feature_importance,
                "model_key": model_key
            }
            
            logger.info(f"Valuation model trained with R² = {r2:.3f}, RMSE = ${rmse:,.0f}")
            return results
            
        except Exception as e:
            logger.error(f"Failed to build valuation model: {e}")
            raise
    
    def predict_property_values(self,
                              parcel_ids: List[str],
                              model_key: str) -> pd.DataFrame:
        """
        Predict property values for specified parcels.
        
        Args:
            parcel_ids: List of parcel IDs to predict
            model_key: Key of the trained model to use
            
        Returns:
            DataFrame with predictions
        """
        if model_key not in self.models:
            raise ValueError(f"Model {model_key} not found. Train a model first.")
        
        try:
            # Get parcel data
            parcel_list = "', '".join(parcel_ids)
            query = f"""
            SELECT 
                p.parno,
                pv.land_value,
                pv.improvement_value,
                pv.total_value as actual_value,
                pi.acres,
                pi.square_feet,
                pi.property_type,
                pi.land_use_code,
                EXTRACT(YEAR FROM pv.assessment_date) as assessment_year,
                ST_X(ST_Centroid(p.geometry)) as longitude,
                ST_Y(ST_Centroid(p.geometry)) as latitude
            FROM parcel p
            JOIN property_values pv ON p.parno = pv.parno
            JOIN property_info pi ON p.parno = pi.parno
            WHERE p.parno IN ('{parcel_list}')
            """
            
            data = self.db_manager.execute_query(query)
            
            if data.empty:
                raise ValueError("No data found for specified parcels")
            
            # Prepare features (same as training)
            feature_columns = ['land_value', 'improvement_value', 'acres', 'square_feet']
            X = data[feature_columns].copy()
            
            # Handle missing values
            X = X.fillna(X.median())
            
            # Get model and make predictions
            model = self.models[model_key]
            scaler = self.scalers.get(model_key)
            
            if scaler and hasattr(model, 'coef_'):  # Linear model
                X_scaled = scaler.transform(X)
                predictions = model.predict(X_scaled)
            else:  # Tree-based model
                predictions = model.predict(X)
            
            # Create results DataFrame
            results = data[['parno', 'actual_value']].copy()
            results['predicted_value'] = predictions
            results['prediction_error'] = results['actual_value'] - results['predicted_value']
            results['percentage_error'] = (results['prediction_error'] / results['actual_value']) * 100
            
            logger.info(f"Generated predictions for {len(results)} parcels")
            return results
            
        except Exception as e:
            logger.error(f"Failed to predict property values: {e}")
            raise
    
    def comparative_market_analysis(self,
                                  target_parcel_id: str,
                                  radius_meters: float = 1000,
                                  max_comparables: int = 10,
                                  property_type_match: bool = True) -> Dict[str, Any]:
        """
        Perform comparative market analysis (CMA) for a target parcel.
        
        Args:
            target_parcel_id: ID of the target parcel
            radius_meters: Search radius for comparables
            max_comparables: Maximum number of comparable properties
            property_type_match: Whether to match property type
            
        Returns:
            Dictionary with CMA results
        """
        try:
            # Get target parcel information
            target_query = f"""
            SELECT 
                p.parno,
                p.geometry,
                pv.total_value,
                pv.land_value,
                pv.improvement_value,
                pi.acres,
                pi.square_feet,
                pi.property_type,
                pv.assessment_date
            FROM parcel p
            JOIN property_values pv ON p.parno = pv.parno
            JOIN property_info pi ON p.parno = pi.parno
            WHERE p.parno = '{target_parcel_id}'
            """
            
            target_data = self.db_manager.execute_spatial_query(target_query)
            
            if target_data.empty:
                raise ValueError(f"Target parcel {target_parcel_id} not found")
            
            target = target_data.iloc[0]
            
            # Build comparables query
            property_type_filter = ""
            if property_type_match and pd.notna(target['property_type']):
                property_type_filter = f"AND pi.property_type = '{target['property_type']}'"
            
            comparables_query = f"""
            SELECT 
                p.parno,
                pv.total_value,
                pv.land_value,
                pv.improvement_value,
                pi.acres,
                pi.square_feet,
                pi.property_type,
                pv.assessment_date,
                ST_Distance(p.geometry, ST_GeomFromText('{target.geometry.wkt}', 4326)) as distance_meters
            FROM parcel p
            JOIN property_values pv ON p.parno = pv.parno
            JOIN property_info pi ON p.parno = pi.parno
            WHERE p.parno != '{target_parcel_id}'
              AND ST_DWithin(p.geometry, ST_GeomFromText('{target.geometry.wkt}', 4326), {radius_meters})
              AND pv.total_value > 0
              {property_type_filter}
            ORDER BY distance_meters
            LIMIT {max_comparables}
            """
            
            comparables = self.db_manager.execute_query(comparables_query)
            
            if comparables.empty:
                return {
                    "target_parcel": target_parcel_id,
                    "error": "No comparable properties found within specified radius"
                }
            
            # Calculate CMA statistics
            comp_values = comparables['total_value']
            
            cma_results = {
                "target_parcel": {
                    "parno": target_parcel_id,
                    "current_value": float(target['total_value']),
                    "acres": float(target['acres']) if pd.notna(target['acres']) else None,
                    "property_type": target['property_type']
                },
                "comparables": {
                    "count": len(comparables),
                    "avg_distance_meters": float(comparables['distance_meters'].mean()),
                    "value_statistics": {
                        "mean": float(comp_values.mean()),
                        "median": float(comp_values.median()),
                        "min": float(comp_values.min()),
                        "max": float(comp_values.max()),
                        "std": float(comp_values.std())
                    }
                },
                "analysis": {
                    "target_vs_mean_diff": float(target['total_value'] - comp_values.mean()),
                    "target_vs_mean_pct": float(((target['total_value'] - comp_values.mean()) / comp_values.mean()) * 100),
                    "target_percentile": float((comp_values < target['total_value']).mean() * 100),
                    "suggested_value_range": {
                        "low": float(comp_values.quantile(0.25)),
                        "high": float(comp_values.quantile(0.75))
                    }
                },
                "comparable_properties": comparables.to_dict('records')
            }
            
            logger.info(f"CMA completed for {target_parcel_id} with {len(comparables)} comparables")
            return cma_results
            
        except Exception as e:
            logger.error(f"Failed to perform CMA: {e}")
            raise
    
    def investment_opportunity_scoring(self,
                                     county_fips: Optional[str] = None,
                                     min_value: Optional[float] = None,
                                     max_value: Optional[float] = None,
                                     limit: int = 100) -> pd.DataFrame:
        """
        Score properties for investment opportunities.
        
        Args:
            county_fips: Optional county filter
            min_value: Minimum property value
            max_value: Maximum property value
            limit: Maximum number of results
            
        Returns:
            DataFrame with investment scores
        """
        try:
            # Build query filters
            where_conditions = ["pv.total_value > 0"]
            
            if county_fips:
                where_conditions.append(f"p.county_fips = '{county_fips}'")
            if min_value:
                where_conditions.append(f"pv.total_value >= {min_value}")
            if max_value:
                where_conditions.append(f"pv.total_value <= {max_value}")
            
            where_clause = " AND ".join(where_conditions)
            
            query = f"""
            SELECT 
                p.parno,
                p.county_fips,
                pv.total_value,
                pv.land_value,
                pv.improvement_value,
                pi.acres,
                pi.property_type,
                pv.assessment_date,
                ST_X(ST_Centroid(p.geometry)) as longitude,
                ST_Y(ST_Centroid(p.geometry)) as latitude
            FROM parcel p
            JOIN property_values pv ON p.parno = pv.parno
            JOIN property_info pi ON p.parno = pi.parno
            WHERE {where_clause}
              AND pi.acres IS NOT NULL
              AND pi.acres > 0
            ORDER BY pv.total_value
            LIMIT {limit * 2}  -- Get more data for scoring
            """
            
            data = self.db_manager.execute_query(query)
            
            if data.empty:
                raise ValueError("No properties found matching criteria")
            
            # Calculate investment scoring metrics
            data = data.copy()
            
            # 1. Value per acre (lower is better for land investment)
            data['value_per_acre'] = data['total_value'] / data['acres']
            data['value_per_acre_score'] = 100 - ((data['value_per_acre'] - data['value_per_acre'].min()) / 
                                                 (data['value_per_acre'].max() - data['value_per_acre'].min()) * 100)
            
            # 2. Land to improvement ratio (higher land ratio may indicate development potential)
            data['land_ratio'] = data['land_value'] / data['total_value']
            data['land_ratio_score'] = (data['land_ratio'] - data['land_ratio'].min()) / \
                                     (data['land_ratio'].max() - data['land_ratio'].min()) * 100
            
            # 3. Size score (larger parcels may have more potential)
            data['size_score'] = (data['acres'] - data['acres'].min()) / \
                               (data['acres'].max() - data['acres'].min()) * 100
            
            # 4. Market position score (properties below median may have upside)
            median_value = data['total_value'].median()
            data['market_position_score'] = np.where(
                data['total_value'] < median_value,
                100 - ((data['total_value'] / median_value) * 100),
                0
            )
            
            # Calculate composite investment score
            weights = {
                'value_per_acre_score': 0.3,
                'land_ratio_score': 0.25,
                'size_score': 0.25,
                'market_position_score': 0.2
            }
            
            data['investment_score'] = sum(data[col] * weight for col, weight in weights.items())
            
            # Rank and return top opportunities
            result = data.nlargest(limit, 'investment_score')[
                ['parno', 'county_fips', 'total_value', 'acres', 'property_type',
                 'value_per_acre', 'land_ratio', 'investment_score']
            ].round(2)
            
            logger.info(f"Investment scoring completed for {len(result)} properties")
            return result
            
        except Exception as e:
            logger.error(f"Failed to score investment opportunities: {e}")
            raise 