#!/usr/bin/env python3
"""
Analytics Demo for ParcelPy Database Module.

This script demonstrates the analytics capabilities of the ParcelPy database module,
including market analytics, risk analytics, and spatial queries.
"""

import logging
import pandas as pd
import numpy as np
from pathlib import Path
import sys
import os
from typing import Dict, Any

from ..core.database_manager import DatabaseManager
from ..core.market_analytics import MarketAnalytics
from ..core.risk_analytics import RiskAnalytics
from ..core.spatial_queries import SpatialQueries

# Setup logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


def create_sample_analytics_data(db_manager: DatabaseManager):
    """
    Create sample data for analytics demonstration.
    
    Args:
        db_manager: Database manager instance
    """
    print("📊 Creating sample analytics data...")
    
    try:
        # Create sample parcel data with realistic values
        sample_data = []
        
        # Generate 1000 sample parcels
        np.random.seed(42)  # For reproducible results
        
        for i in range(1000):
            parno = f"DEMO{i:06d}"
            
            # Random location in North Carolina
            longitude = np.random.uniform(-84.0, -75.0)
            latitude = np.random.uniform(33.8, 36.6)
            
            # Property characteristics
            acres = np.random.lognormal(1.0, 1.0)  # Log-normal distribution for acres
            property_types = ['Residential', 'Commercial', 'Industrial', 'Agricultural', 'Vacant']
            property_type = np.random.choice(property_types, p=[0.6, 0.15, 0.05, 0.15, 0.05])
            
            # Value calculations based on property type and size
            base_value_per_acre = {
                'Residential': 50000,
                'Commercial': 100000,
                'Industrial': 30000,
                'Agricultural': 5000,
                'Vacant': 8000
            }
            
            land_value = acres * base_value_per_acre[property_type] * np.random.uniform(0.7, 1.3)
            
            # Improvement value (higher for developed properties)
            if property_type in ['Residential', 'Commercial', 'Industrial']:
                improvement_value = land_value * np.random.uniform(0.5, 2.0)
            else:
                improvement_value = land_value * np.random.uniform(0.0, 0.3)
            
            total_value = land_value + improvement_value
            assessed_value = total_value * np.random.uniform(0.8, 1.0)
            
            # Assessment date (within last 5 years)
            assessment_year = np.random.choice([2019, 2020, 2021, 2022, 2023], p=[0.1, 0.15, 0.25, 0.3, 0.2])
            assessment_date = f"{assessment_year}-01-01"
            
            sample_data.append({
                'parno': parno,
                'county_fips': '37183',  # Wake County
                'state_fips': '37',
                'longitude': longitude,
                'latitude': latitude,
                'acres': acres,
                'property_type': property_type,
                'land_value': land_value,
                'improvement_value': improvement_value,
                'total_value': total_value,
                'assessed_value': assessed_value,
                'assessment_date': assessment_date
            })
        
        # Create DataFrame
        df = pd.DataFrame(sample_data)
        
        # Create tables and insert data
        print("Creating parcel table...")
        db_manager.execute_query("""
            CREATE TABLE IF NOT EXISTS parcel (
                id SERIAL PRIMARY KEY,
                parno VARCHAR(20) UNIQUE,
                county_fips VARCHAR(3),
                state_fips VARCHAR(2),
                geometry GEOMETRY(Point, 4326),
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            )
        """)
        
        print("Creating property_info table...")
        db_manager.execute_query("""
            CREATE TABLE IF NOT EXISTS property_info (
                id SERIAL PRIMARY KEY,
                parno VARCHAR(20) UNIQUE,
                acres DOUBLE PRECISION,
                property_type VARCHAR(50),
                square_feet DOUBLE PRECISION,
                land_use_code VARCHAR(10),
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                FOREIGN KEY (parno) REFERENCES parcel(parno)
            )
        """)
        
        print("Creating property_values table...")
        db_manager.execute_query("""
            CREATE TABLE IF NOT EXISTS property_values (
                id SERIAL PRIMARY KEY,
                parno VARCHAR(20) UNIQUE,
                land_value DOUBLE PRECISION,
                improvement_value DOUBLE PRECISION,
                total_value DOUBLE PRECISION,
                assessed_value DOUBLE PRECISION,
                assessment_date DATE,
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                FOREIGN KEY (parno) REFERENCES parcel(parno)
            )
        """)
        
        # Insert sample data
        print("Inserting sample data...")
        
        for _, row in df.iterrows():
            # Insert parcel
            db_manager.execute_query("""
                INSERT INTO parcel (parno, county_fips, state_fips, geometry)
                VALUES (%s, %s, %s, ST_SetSRID(ST_MakePoint(%s, %s), 4326))
                ON CONFLICT (parno) DO NOTHING
            """, (row['parno'], row['county_fips'], row['state_fips'], row['longitude'], row['latitude']))
            
            # Insert property info (add some default values)
            square_feet = row['acres'] * 43560 * np.random.uniform(0.1, 0.3) if row['property_type'] != 'Vacant' else None
            db_manager.execute_query("""
                INSERT INTO property_info (parno, acres, property_type, square_feet, land_use_code)
                VALUES (%s, %s, %s, %s, %s)
                ON CONFLICT (parno) DO NOTHING
            """, (row['parno'], row['acres'], row['property_type'], square_feet, 'RES'))
            
            # Insert property values
            db_manager.execute_query("""
                INSERT INTO property_values (parno, land_value, improvement_value, total_value, assessed_value, assessment_date)
                VALUES (%s, %s, %s, %s, %s, %s)
                ON CONFLICT (parno) DO NOTHING
            """, (row['parno'], row['land_value'], row['improvement_value'], row['total_value'], row['assessed_value'], row['assessment_date']))
        
        print(f"✅ Created {len(df)} sample parcels for analytics demonstration")
        
    except Exception as e:
        logger.error(f"Failed to create sample data: {e}")
        raise


def demonstrate_market_analytics(db_manager: DatabaseManager):
    """
    Demonstrate market analytics capabilities.
    
    Args:
        db_manager: Database manager instance
    """
    print("\n" + "="*80)
    print("📈 MARKET ANALYTICS DEMONSTRATION")
    print("="*80)
    
    market_analytics = MarketAnalytics(db_manager)
    
    try:
        # 1. Market Trend Analysis
        print("\n1️⃣ Market Trend Analysis")
        print("-" * 40)
        
        trends = market_analytics.analyze_market_trends(
            county_fips='37183',
            time_period_months=36
        )
        
        if 'error' not in trends:
            print(f"📊 Analysis Period: {trends['period_analyzed']}")
            print(f"📊 Total Properties: {trends['total_properties']:,}")
            
            overall = trends['overall_trends']
            print(f"📊 Current Avg Value: ${overall['current_avg_value']:,.0f}")
            print(f"📊 Monthly Value Growth: {overall['avg_monthly_value_growth']:.2%}")
            print(f"📊 Value Volatility: {overall['value_volatility']:.2%}")
            
            # Show trends by property type
            print("\n🏠 Trends by Property Type:")
            for prop_type, data in trends['trends_by_type'].items():
                print(f"  {prop_type}: {data['property_count']:,} properties, "
                      f"avg ${data['avg_value']:,.0f}, growth {data['avg_growth_rate']:.2%}")
        
        # 2. Build Valuation Model
        print("\n2️⃣ Predictive Valuation Model")
        print("-" * 40)
        
        model_results = market_analytics.build_valuation_model(
            target_column='total_value',
            model_type='random_forest',
            county_fips='37183'
        )
        
        perf = model_results['performance']
        print(f"🤖 Model Type: {model_results['model_type']}")
        print(f"🤖 R² Score: {perf['r2_score']:.3f}")
        print(f"🤖 RMSE: ${perf['rmse']:,.0f}")
        print(f"🤖 MAPE: {perf['mean_absolute_percentage_error']:.1f}%")
        
        if model_results['feature_importance']:
            print("\n🎯 Top Feature Importance:")
            for feature, importance in list(model_results['feature_importance'].items())[:3]:
                print(f"  {feature}: {importance:.3f}")
        
        # 3. Make Predictions
        print("\n3️⃣ Property Value Predictions")
        print("-" * 40)
        
        # Get some sample parcel IDs
        sample_parcels = db_manager.execute_query("""
            SELECT parno FROM parcel LIMIT 5
        """)
        
        if not sample_parcels.empty:
            parcel_ids = sample_parcels['parno'].tolist()
            
            predictions = market_analytics.predict_property_values(
                parcel_ids=parcel_ids,
                model_key=model_results['model_key']
            )
            
            print(f"🔮 Predictions for {len(predictions)} parcels:")
            for _, row in predictions.iterrows():
                print(f"  {row['parno']}: Actual ${row['actual_value']:,.0f}, "
                      f"Predicted ${row['predicted_value']:,.0f} "
                      f"({row['percentage_error']:+.1f}% error)")
        
        # 4. Comparative Market Analysis
        print("\n4️⃣ Comparative Market Analysis")
        print("-" * 40)
        
        if not sample_parcels.empty:
            target_parcel = sample_parcels['parno'].iloc[0]
            
            cma_results = market_analytics.comparative_market_analysis(
                target_parcel_id=target_parcel,
                radius_meters=5000,
                max_comparables=10
            )
            
            if 'error' not in cma_results:
                target = cma_results['target_parcel']
                comp = cma_results['comparables']
                analysis = cma_results['analysis']
                
                print(f"🎯 Target: {target['parno']} (${target['current_value']:,.0f})")
                print(f"🏠 Comparables: {comp['count']} found")
                print(f"📊 Market Position: {analysis['target_percentile']:.0f}th percentile")
                print(f"📊 vs Market Mean: {analysis['target_vs_mean_pct']:+.1f}%")
        
        # 5. Investment Opportunity Scoring
        print("\n5️⃣ Investment Opportunity Scoring")
        print("-" * 40)
        
        opportunities = market_analytics.investment_opportunity_scoring(
            county_fips='37183',
            limit=10
        )
        
        print(f"💰 Top Investment Opportunities:")
        for i, (_, row) in enumerate(opportunities.head(5).iterrows(), 1):
            print(f"  #{i} {row['parno']}: Score {row['investment_score']:.1f}, "
                  f"${row['total_value']:,.0f}, {row['acres']:.1f} acres")
        
    except Exception as e:
        logger.error(f"Market analytics demonstration failed: {e}")
        print(f"❌ Error: {e}")


def demonstrate_risk_analytics(db_manager: DatabaseManager):
    """
    Demonstrate risk analytics capabilities.
    
    Args:
        db_manager: Database manager instance
    """
    print("\n" + "="*80)
    print("⚠️ RISK ANALYTICS DEMONSTRATION")
    print("="*80)
    
    risk_analytics = RiskAnalytics(db_manager)
    
    try:
        # 1. Flood Risk Assessment
        print("\n1️⃣ Flood Risk Assessment")
        print("-" * 40)
        
        flood_risk = risk_analytics.assess_flood_risk(
            county_fips='37183'
        )
        
        if not flood_risk.empty:
            risk_dist = flood_risk['flood_risk_level'].value_counts()
            print(f"🌊 Flood Risk Distribution:")
            for level, count in risk_dist.items():
                print(f"  {level} Risk: {count} properties ({count/len(flood_risk):.1%})")
            
            high_risk = flood_risk[flood_risk['flood_risk_level'] == 'High']
            if not high_risk.empty:
                avg_loss = high_risk['potential_loss_estimate'].mean()
                print(f"🚨 High-risk properties: {len(high_risk)}")
                print(f"💰 Avg potential loss: ${avg_loss:,.0f}")
        
        # 2. Tax Assessment Risk
        print("\n2️⃣ Tax Assessment Risk")
        print("-" * 40)
        
        tax_risk = risk_analytics.assess_tax_assessment_risk(
            county_fips='37183'
        )
        
        if not tax_risk.empty:
            tax_dist = tax_risk['tax_increase_risk'].value_counts()
            print(f"💰 Tax Risk Distribution:")
            for level, count in tax_dist.items():
                print(f"  {level} Risk: {count} properties ({count/len(tax_risk):.1%})")
            
            high_tax_risk = tax_risk[tax_risk['tax_increase_risk'] == 'High']
            if not high_tax_risk.empty:
                avg_increase = high_tax_risk['potential_assessment_increase'].mean()
                print(f"🚨 High tax risk properties: {len(high_tax_risk)}")
                print(f"💰 Avg potential increase: ${avg_increase:,.0f}")
        
        # 3. Market Volatility Risk
        print("\n3️⃣ Market Volatility Risk")
        print("-" * 40)
        
        volatility_risk = risk_analytics.assess_market_volatility_risk(
            county_fips='37183'
        )
        
        if not volatility_risk.empty:
            print(f"📈 Market Volatility Analysis:")
            for _, row in volatility_risk.head(3).iterrows():
                print(f"  {row['property_type']}: {row['volatility_percentage']:.1f}% volatility "
                      f"({row['volatility_risk_level']} risk)")
        
        # 4. Comprehensive Risk Assessment
        print("\n4️⃣ Comprehensive Risk Assessment")
        print("-" * 40)
        
        # Get sample parcels for comprehensive assessment
        sample_parcels = db_manager.execute_query("""
            SELECT parno FROM parcel LIMIT 20
        """)
        
        if not sample_parcels.empty:
            parcel_ids = sample_parcels['parno'].tolist()
            
            comprehensive_risk = risk_analytics.comprehensive_risk_assessment(
                parcel_ids=parcel_ids
            )
            
            if not comprehensive_risk.empty:
                risk_dist = comprehensive_risk['overall_risk_level'].value_counts()
                print(f"🎯 Overall Risk Distribution:")
                for level, count in risk_dist.items():
                    print(f"  {level}: {count} properties ({count/len(comprehensive_risk):.1%})")
                
                # Show highest risk properties
                high_risk = comprehensive_risk.nlargest(3, 'composite_risk_score')
                print(f"\n🚨 Highest Risk Properties:")
                for _, row in high_risk.iterrows():
                    print(f"  {row['parno']}: Risk Score {row['composite_risk_score']:.1f}/100")
                    print(f"    Flood: {row['flood_risk_score']:.0f}, "
                          f"Tax: {row['tax_risk_score']:.0f}, "
                          f"Market: {row['market_risk_score']:.0f}")
                    if row['risk_recommendations']:
                        print(f"    Recommendations: {', '.join(row['risk_recommendations'][:2])}")
                
                # Summary statistics
                avg_risk = comprehensive_risk['composite_risk_score'].mean()
                total_impact = comprehensive_risk['total_potential_impact'].sum()
                print(f"\n📊 Risk Summary:")
                print(f"  Average Risk Score: {avg_risk:.1f}/100")
                print(f"  Total Potential Impact: ${total_impact:,.0f}")
        
    except Exception as e:
        logger.error(f"Risk analytics demonstration failed: {e}")
        print(f"❌ Error: {e}")


def demonstrate_integration_scenarios(db_manager: DatabaseManager):
    """
    Demonstrate integrated analytics scenarios.
    
    Args:
        db_manager: Database manager instance
    """
    print("\n" + "="*80)
    print("🔗 INTEGRATED ANALYTICS SCENARIOS")
    print("="*80)
    
    market_analytics = MarketAnalytics(db_manager)
    risk_analytics = RiskAnalytics(db_manager)
    
    try:
        # Scenario 1: Investment Decision Support
        print("\n1️⃣ Investment Decision Support")
        print("-" * 40)
        
        # Find investment opportunities
        opportunities = market_analytics.investment_opportunity_scoring(
            county_fips='37183',
            limit=20
        )
        
        if not opportunities.empty:
            # Get top 5 opportunities
            top_opportunities = opportunities.head(5)['parno'].tolist()
            
            # Assess risk for these opportunities
            risk_assessment = risk_analytics.comprehensive_risk_assessment(
                parcel_ids=top_opportunities
            )
            
            print(f"💰 Investment Analysis (Top 5 Opportunities):")
            
            for parno in top_opportunities:
                opp_data = opportunities[opportunities['parno'] == parno].iloc[0]
                risk_data = risk_assessment[risk_assessment['parno'] == parno]
                
                print(f"\n  📍 Parcel: {parno}")
                print(f"    Investment Score: {opp_data['investment_score']:.1f}/100")
                print(f"    Value: ${opp_data['total_value']:,.0f}")
                print(f"    Value/Acre: ${opp_data['value_per_acre']:,.0f}")
                
                if not risk_data.empty:
                    risk_row = risk_data.iloc[0]
                    print(f"    Risk Score: {risk_row['composite_risk_score']:.1f}/100")
                    print(f"    Risk Level: {risk_row['overall_risk_level']}")
                    
                    # Investment recommendation
                    if opp_data['investment_score'] > 70 and risk_row['composite_risk_score'] < 50:
                        recommendation = "🟢 STRONG BUY"
                    elif opp_data['investment_score'] > 60 and risk_row['composite_risk_score'] < 60:
                        recommendation = "🟡 CONSIDER"
                    else:
                        recommendation = "🔴 AVOID"
                    
                    print(f"    Recommendation: {recommendation}")
        
        # Scenario 2: Portfolio Risk Analysis
        print("\n2️⃣ Portfolio Risk Analysis")
        print("-" * 40)
        
        # Simulate a property portfolio
        portfolio_parcels = db_manager.execute_query("""
            SELECT parno FROM parcel 
            WHERE parno IN (
                SELECT parno FROM property_values 
                WHERE total_value > 200000 
                ORDER BY RANDOM() 
                LIMIT 10
            )
        """)
        
        if not portfolio_parcels.empty:
            portfolio_ids = portfolio_parcels['parno'].tolist()
            
            # Assess portfolio risk
            portfolio_risk = risk_analytics.comprehensive_risk_assessment(
                parcel_ids=portfolio_ids
            )
            
            if not portfolio_risk.empty:
                print(f"📊 Portfolio Risk Analysis ({len(portfolio_ids)} properties):")
                
                # Risk distribution
                risk_dist = portfolio_risk['overall_risk_level'].value_counts()
                for level, count in risk_dist.items():
                    print(f"  {level} Risk: {count} properties ({count/len(portfolio_risk):.1%})")
                
                # Financial impact
                total_value = portfolio_risk['total_value'].sum()
                total_impact = portfolio_risk['total_potential_impact'].sum()
                risk_percentage = (total_impact / total_value) * 100
                
                print(f"  Total Portfolio Value: ${total_value:,.0f}")
                print(f"  Total Risk Exposure: ${total_impact:,.0f} ({risk_percentage:.1f}%)")
                
                # Diversification analysis
                property_types = portfolio_risk['property_type'].value_counts()
                print(f"  Property Type Diversification:")
                for prop_type, count in property_types.items():
                    print(f"    {prop_type}: {count} properties")
        
        # Scenario 3: Market Timing Analysis
        print("\n3️⃣ Market Timing Analysis")
        print("-" * 40)
        
        # Analyze market trends
        trends = market_analytics.analyze_market_trends(
            county_fips='37183',
            time_period_months=24
        )
        
        if 'error' not in trends:
            overall = trends['overall_trends']
            
            print(f"📈 Market Timing Indicators:")
            print(f"  Monthly Value Growth: {overall['avg_monthly_value_growth']:.2%}")
            print(f"  Market Volatility: {overall['value_volatility']:.2%}")
            
            # Market timing recommendation
            if overall['avg_monthly_value_growth'] > 0.01 and overall['value_volatility'] < 0.1:
                timing_rec = "🟢 GOOD TIME TO BUY"
            elif overall['avg_monthly_value_growth'] > 0.005:
                timing_rec = "🟡 NEUTRAL MARKET"
            else:
                timing_rec = "🔴 WAIT FOR BETTER CONDITIONS"
            
            print(f"  Market Timing: {timing_rec}")
        
    except Exception as e:
        logger.error(f"Integration scenarios demonstration failed: {e}")
        print(f"❌ Error: {e}")


def main():
    """Main demonstration function."""
    print("🚀 ParcelPy Analytics and Intelligence Demo")
    print("=" * 80)
    
    try:
        # Initialize database manager with PostgreSQL
        # You'll need to update these connection details
        db_manager = DatabaseManager(
            host="localhost",
            port=5432,
            database="parcelpy",
            user="parcelpy",
            password="your_password"
        )
        
        # Test connection
        if not db_manager.test_connection():
            print("❌ Cannot connect to PostgreSQL database")
            print("Please ensure PostgreSQL is running and connection details are correct")
            return
        
        # Create sample data
        create_sample_analytics_data(db_manager)
        
        # Demonstrate market analytics
        demonstrate_market_analytics(db_manager)
        
        # Demonstrate risk analytics
        demonstrate_risk_analytics(db_manager)
        
        # Demonstrate integration scenarios
        demonstrate_integration_scenarios(db_manager)
        
        print("\n" + "="*80)
        print("✅ ANALYTICS DEMONSTRATION COMPLETED")
        print("="*80)
        print("\nKey Features Demonstrated:")
        print("📈 Market trend analysis and forecasting")
        print("🤖 Predictive valuation modeling")
        print("🏘️ Comparative market analysis (CMA)")
        print("💰 Investment opportunity scoring")
        print("⚠️ Comprehensive risk assessment")
        print("🔗 Integrated decision support scenarios")
        print("\nNext Steps:")
        print("• Integrate with real parcel data")
        print("• Add external data sources (FEMA, Census)")
        print("• Implement web dashboard")
        print("• Add automated reporting")
        
    except Exception as e:
        logger.error(f"Demo failed: {e}")
        print(f"\n❌ Demo failed: {e}")
        print("\nTroubleshooting:")
        print("1. Ensure PostgreSQL is running")
        print("2. Check database connection details")
        print("3. Verify PostGIS extension is installed")
        print("4. Install required dependencies: pip install scikit-learn")
        sys.exit(1)


if __name__ == "__main__":
    main() 