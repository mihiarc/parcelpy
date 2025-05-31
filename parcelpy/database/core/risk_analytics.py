"""
Risk Analytics Module for ParcelPy

Provides comprehensive risk assessment for properties including environmental,
market, and regulatory risk factors.
"""

import logging
import pandas as pd
import numpy as np
from typing import Dict, List, Optional, Tuple, Any, Union
from datetime import datetime, timedelta
import geopandas as gpd
from pathlib import Path

try:
    import requests
    REQUESTS_AVAILABLE = True
except ImportError:
    REQUESTS_AVAILABLE = False

from .database_manager import DatabaseManager

logger = logging.getLogger(__name__)


class RiskAnalytics:
    """
    Comprehensive risk assessment for property analysis.
    
    Provides:
    - Environmental risk assessment (flood zones, natural disasters)
    - Market risk analysis (volatility, liquidity)
    - Regulatory risk evaluation (zoning changes, tax assessments)
    - Composite risk scoring
    """
    
    def __init__(self, db_manager: DatabaseManager):
        """
        Initialize risk analytics.
        
        Args:
            db_manager: Database manager instance
        """
        self.db_manager = db_manager
        self.risk_cache = {}
        
    def assess_flood_risk(self,
                         parcel_ids: Optional[List[str]] = None,
                         county_fips: Optional[str] = None,
                         buffer_meters: float = 100) -> pd.DataFrame:
        """
        Assess flood risk for properties based on proximity to water bodies.
        
        Args:
            parcel_ids: Optional list of specific parcel IDs
            county_fips: Optional county filter
            buffer_meters: Buffer distance for water proximity analysis
            
        Returns:
            DataFrame with flood risk assessments
        """
        try:
            # Build query filters
            where_conditions = []
            
            if parcel_ids:
                parcel_list = "', '".join(parcel_ids)
                where_conditions.append(f"p.parno IN ('{parcel_list}')")
            
            if county_fips:
                where_conditions.append(f"p.county_fips = '{county_fips}'")
            
            where_clause = " AND ".join(where_conditions) if where_conditions else "1=1"
            
            # Query to assess flood risk based on elevation and water proximity
            # Note: This is a simplified model - in production you'd integrate with FEMA flood maps
            query = f"""
            WITH parcel_elevations AS (
                SELECT 
                    p.parno,
                    p.geometry,
                    pv.total_value,
                    pi.acres,
                    pi.property_type,
                    ST_X(ST_Centroid(p.geometry)) as longitude,
                    ST_Y(ST_Centroid(p.geometry)) as latitude,
                    -- Simplified elevation estimation based on latitude (lower = higher risk)
                    CASE 
                        WHEN ST_Y(ST_Centroid(p.geometry)) < 35.0 THEN 'Low_Elevation'
                        WHEN ST_Y(ST_Centroid(p.geometry)) < 36.0 THEN 'Medium_Elevation'
                        ELSE 'High_Elevation'
                    END as elevation_category
                FROM parcel p
                JOIN property_values pv ON p.parno = pv.parno
                JOIN property_info pi ON p.parno = pi.parno
                WHERE {where_clause}
            )
            SELECT 
                parno,
                total_value,
                acres,
                property_type,
                longitude,
                latitude,
                elevation_category,
                CASE elevation_category
                    WHEN 'Low_Elevation' THEN 'High'
                    WHEN 'Medium_Elevation' THEN 'Medium'
                    ELSE 'Low'
                END as flood_risk_level,
                CASE elevation_category
                    WHEN 'Low_Elevation' THEN 85
                    WHEN 'Medium_Elevation' THEN 45
                    ELSE 15
                END as flood_risk_score
            FROM parcel_elevations
            ORDER BY flood_risk_score DESC
            """
            
            flood_risk_df = self.db_manager.execute_query(query)
            
            if flood_risk_df.empty:
                return pd.DataFrame()
            
            # Add additional risk factors
            flood_risk_df['risk_factors'] = flood_risk_df.apply(
                lambda row: self._get_flood_risk_factors(row), axis=1
            )
            
            # Calculate financial impact
            flood_risk_df['potential_loss_estimate'] = (
                flood_risk_df['total_value'] * 
                flood_risk_df['flood_risk_score'] / 100 * 0.3  # Assume 30% max loss
            )
            
            logger.info(f"Flood risk assessment completed for {len(flood_risk_df)} properties")
            return flood_risk_df
            
        except Exception as e:
            logger.error(f"Failed to assess flood risk: {e}")
            raise
    
    def _get_flood_risk_factors(self, row: pd.Series) -> List[str]:
        """Get specific flood risk factors for a property."""
        factors = []
        
        if row['elevation_category'] == 'Low_Elevation':
            factors.extend(['Low elevation', 'Potential storm surge'])
        
        if row['acres'] > 10:
            factors.append('Large property area')
        
        if row['property_type'] in ['Residential', 'Commercial']:
            factors.append('Developed property')
        
        return factors
    
    def assess_market_volatility_risk(self,
                                    county_fips: Optional[str] = None,
                                    analysis_period_months: int = 24) -> pd.DataFrame:
        """
        Assess market volatility risk based on historical value changes.
        
        Args:
            county_fips: Optional county filter
            analysis_period_months: Period for volatility analysis
            
        Returns:
            DataFrame with market risk assessments
        """
        try:
            where_clause = "pv.total_value > 0"
            if county_fips:
                where_clause += f" AND p.county_fips = '{county_fips}'"
            
            # Calculate value volatility by property type and area
            query = f"""
            WITH value_changes AS (
                SELECT 
                    p.county_fips,
                    pi.property_type,
                    pi.land_use_code,
                    DATE_TRUNC('quarter', pv.assessment_date) as quarter,
                    AVG(pv.total_value) as avg_value,
                    COUNT(*) as property_count,
                    STDDEV(pv.total_value) as value_std
                FROM parcel p
                JOIN property_values pv ON p.parno = pv.parno
                JOIN property_info pi ON p.parno = pi.parno
                WHERE {where_clause}
                  AND pv.assessment_date >= CURRENT_DATE - INTERVAL '{analysis_period_months} months'
                GROUP BY p.county_fips, pi.property_type, pi.land_use_code, 
                         DATE_TRUNC('quarter', pv.assessment_date)
                HAVING COUNT(*) >= 5  -- Minimum sample size
            ),
            volatility_metrics AS (
                SELECT 
                    county_fips,
                    property_type,
                    land_use_code,
                    AVG(avg_value) as mean_value,
                    STDDEV(avg_value) as value_volatility,
                    COUNT(*) as quarters_analyzed,
                    SUM(property_count) as total_properties
                FROM value_changes
                GROUP BY county_fips, property_type, land_use_code
            )
            SELECT 
                *,
                CASE 
                    WHEN value_volatility / NULLIF(mean_value, 0) > 0.15 THEN 'High'
                    WHEN value_volatility / NULLIF(mean_value, 0) > 0.08 THEN 'Medium'
                    ELSE 'Low'
                END as volatility_risk_level,
                ROUND((value_volatility / NULLIF(mean_value, 0)) * 100, 2) as volatility_percentage
            FROM volatility_metrics
            ORDER BY volatility_percentage DESC
            """
            
            volatility_df = self.db_manager.execute_query(query)
            
            if volatility_df.empty:
                return pd.DataFrame()
            
            # Add risk scoring
            volatility_df['market_risk_score'] = np.where(
                volatility_df['volatility_percentage'] > 15, 80,
                np.where(volatility_df['volatility_percentage'] > 8, 50, 20)
            )
            
            logger.info(f"Market volatility assessment completed for {len(volatility_df)} segments")
            return volatility_df
            
        except Exception as e:
            logger.error(f"Failed to assess market volatility risk: {e}")
            raise
    
    def assess_tax_assessment_risk(self,
                                 parcel_ids: Optional[List[str]] = None,
                                 county_fips: Optional[str] = None) -> pd.DataFrame:
        """
        Assess risk of tax assessment increases.
        
        Args:
            parcel_ids: Optional list of specific parcel IDs
            county_fips: Optional county filter
            
        Returns:
            DataFrame with tax risk assessments
        """
        try:
            # Build query filters
            where_conditions = []
            
            if parcel_ids:
                parcel_list = "', '".join(parcel_ids)
                where_conditions.append(f"p.parno IN ('{parcel_list}')")
            
            if county_fips:
                where_conditions.append(f"p.county_fips = '{county_fips}'")
            
            where_clause = " AND ".join(where_conditions) if where_conditions else "1=1"
            
            # Analyze assessment patterns and predict risk
            query = f"""
            WITH assessment_history AS (
                SELECT 
                    p.parno,
                    p.county_fips,
                    pv.total_value,
                    pv.assessed_value,
                    pv.land_value,
                    pv.improvement_value,
                    pv.assessment_date,
                    pi.property_type,
                    pi.acres,
                    -- Calculate assessment to market value ratio
                    CASE 
                        WHEN pv.total_value > 0 THEN pv.assessed_value / pv.total_value
                        ELSE NULL
                    END as assessment_ratio,
                    -- Years since last assessment
                    EXTRACT(YEAR FROM CURRENT_DATE) - EXTRACT(YEAR FROM pv.assessment_date) as years_since_assessment
                FROM parcel p
                JOIN property_values pv ON p.parno = pv.parno
                JOIN property_info pi ON p.parno = pi.parno
                WHERE {where_clause}
                  AND pv.assessed_value > 0
                  AND pv.total_value > 0
            ),
            county_averages AS (
                SELECT 
                    county_fips,
                    property_type,
                    AVG(assessment_ratio) as avg_assessment_ratio,
                    STDDEV(assessment_ratio) as assessment_ratio_std
                FROM assessment_history
                WHERE assessment_ratio IS NOT NULL
                GROUP BY county_fips, property_type
            )
            SELECT 
                ah.*,
                ca.avg_assessment_ratio,
                ca.assessment_ratio_std,
                CASE 
                    WHEN ah.assessment_ratio < (ca.avg_assessment_ratio - ca.assessment_ratio_std) THEN 'High'
                    WHEN ah.assessment_ratio < ca.avg_assessment_ratio THEN 'Medium'
                    ELSE 'Low'
                END as tax_increase_risk,
                CASE 
                    WHEN ah.years_since_assessment > 5 THEN 'High'
                    WHEN ah.years_since_assessment > 3 THEN 'Medium'
                    ELSE 'Low'
                END as reassessment_risk,
                -- Estimate potential tax increase
                CASE 
                    WHEN ah.assessment_ratio < (ca.avg_assessment_ratio - ca.assessment_ratio_std) 
                    THEN (ca.avg_assessment_ratio - ah.assessment_ratio) * ah.total_value
                    ELSE 0
                END as potential_assessment_increase
            FROM assessment_history ah
            JOIN county_averages ca ON ah.county_fips = ca.county_fips 
                                   AND ah.property_type = ca.property_type
            ORDER BY potential_assessment_increase DESC
            """
            
            tax_risk_df = self.db_manager.execute_query(query)
            
            if tax_risk_df.empty:
                return pd.DataFrame()
            
            # Calculate composite tax risk score
            tax_risk_df['tax_risk_score'] = tax_risk_df.apply(
                lambda row: self._calculate_tax_risk_score(row), axis=1
            )
            
            logger.info(f"Tax assessment risk analysis completed for {len(tax_risk_df)} properties")
            return tax_risk_df
            
        except Exception as e:
            logger.error(f"Failed to assess tax risk: {e}")
            raise
    
    def _calculate_tax_risk_score(self, row: pd.Series) -> int:
        """Calculate composite tax risk score."""
        score = 0
        
        # Assessment ratio risk
        if row['tax_increase_risk'] == 'High':
            score += 40
        elif row['tax_increase_risk'] == 'Medium':
            score += 20
        
        # Reassessment timing risk
        if row['reassessment_risk'] == 'High':
            score += 30
        elif row['reassessment_risk'] == 'Medium':
            score += 15
        
        # Property value risk (higher values = higher risk)
        if row['total_value'] > 500000:
            score += 20
        elif row['total_value'] > 200000:
            score += 10
        
        return min(score, 100)  # Cap at 100
    
    def comprehensive_risk_assessment(self,
                                    parcel_ids: Optional[List[str]] = None,
                                    county_fips: Optional[str] = None) -> pd.DataFrame:
        """
        Perform comprehensive risk assessment combining multiple risk factors.
        
        Args:
            parcel_ids: Optional list of specific parcel IDs
            county_fips: Optional county filter
            
        Returns:
            DataFrame with comprehensive risk scores
        """
        try:
            # Get individual risk assessments
            flood_risk = self.assess_flood_risk(parcel_ids, county_fips)
            tax_risk = self.assess_tax_assessment_risk(parcel_ids, county_fips)
            
            if flood_risk.empty and tax_risk.empty:
                return pd.DataFrame()
            
            # Merge risk assessments
            if not flood_risk.empty and not tax_risk.empty:
                comprehensive_risk = flood_risk.merge(
                    tax_risk[['parno', 'tax_risk_score', 'tax_increase_risk', 'potential_assessment_increase']],
                    on='parno',
                    how='outer'
                )
            elif not flood_risk.empty:
                comprehensive_risk = flood_risk.copy()
                comprehensive_risk['tax_risk_score'] = 0
                comprehensive_risk['tax_increase_risk'] = 'Unknown'
                comprehensive_risk['potential_assessment_increase'] = 0
            else:
                comprehensive_risk = tax_risk.copy()
                comprehensive_risk['flood_risk_score'] = 0
                comprehensive_risk['flood_risk_level'] = 'Unknown'
                comprehensive_risk['potential_loss_estimate'] = 0
            
            # Fill missing values
            comprehensive_risk = comprehensive_risk.fillna(0)
            
            # Calculate composite risk score
            weights = {
                'flood_risk_score': 0.4,
                'tax_risk_score': 0.3,
                'market_risk': 0.3  # Will be added based on property characteristics
            }
            
            # Simple market risk based on property value and type
            comprehensive_risk['market_risk_score'] = comprehensive_risk.apply(
                lambda row: self._estimate_market_risk(row), axis=1
            )
            
            # Calculate weighted composite score
            comprehensive_risk['composite_risk_score'] = (
                comprehensive_risk['flood_risk_score'] * weights['flood_risk_score'] +
                comprehensive_risk['tax_risk_score'] * weights['tax_risk_score'] +
                comprehensive_risk['market_risk_score'] * weights['market_risk']
            ).round(1)
            
            # Assign risk categories
            comprehensive_risk['overall_risk_level'] = pd.cut(
                comprehensive_risk['composite_risk_score'],
                bins=[0, 25, 50, 75, 100],
                labels=['Low', 'Medium', 'High', 'Very High'],
                include_lowest=True
            )
            
            # Calculate total potential financial impact
            comprehensive_risk['total_potential_impact'] = (
                comprehensive_risk['potential_loss_estimate'] +
                comprehensive_risk['potential_assessment_increase']
            )
            
            # Add risk recommendations
            comprehensive_risk['risk_recommendations'] = comprehensive_risk.apply(
                lambda row: self._generate_risk_recommendations(row), axis=1
            )
            
            # Sort by composite risk score
            comprehensive_risk = comprehensive_risk.sort_values('composite_risk_score', ascending=False)
            
            logger.info(f"Comprehensive risk assessment completed for {len(comprehensive_risk)} properties")
            return comprehensive_risk
            
        except Exception as e:
            logger.error(f"Failed to perform comprehensive risk assessment: {e}")
            raise
    
    def _estimate_market_risk(self, row: pd.Series) -> float:
        """Estimate market risk based on property characteristics."""
        risk_score = 20  # Base risk
        
        # Property type risk
        if row.get('property_type') == 'Commercial':
            risk_score += 20
        elif row.get('property_type') == 'Industrial':
            risk_score += 30
        
        # Value-based risk
        total_value = row.get('total_value', 0)
        if total_value > 1000000:
            risk_score += 25
        elif total_value > 500000:
            risk_score += 15
        elif total_value < 50000:
            risk_score += 20  # Very low value properties can be risky
        
        # Size-based risk
        acres = row.get('acres', 0)
        if acres > 50:
            risk_score += 15  # Large properties harder to sell
        
        return min(risk_score, 100)
    
    def _generate_risk_recommendations(self, row: pd.Series) -> List[str]:
        """Generate risk mitigation recommendations."""
        recommendations = []
        
        # Flood risk recommendations
        if row.get('flood_risk_level') == 'High':
            recommendations.extend([
                'Consider flood insurance',
                'Evaluate drainage improvements',
                'Review elevation certificates'
            ])
        
        # Tax risk recommendations
        if row.get('tax_increase_risk') == 'High':
            recommendations.extend([
                'Budget for potential tax increases',
                'Consider tax assessment appeal if overvalued',
                'Monitor local assessment practices'
            ])
        
        # Market risk recommendations
        market_risk = row.get('market_risk_score', 0)
        if market_risk > 60:
            recommendations.extend([
                'Diversify property portfolio',
                'Consider shorter holding periods',
                'Monitor local market conditions closely'
            ])
        
        # Overall high risk
        if row.get('composite_risk_score', 0) > 75:
            recommendations.append('Consider professional risk assessment')
        
        return recommendations 