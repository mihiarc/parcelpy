"""
CLI for ParcelPy Analytics and Intelligence Features

Provides command-line access to market analysis, risk assessment, and predictive modeling.
"""

import argparse
import sys
import logging
from pathlib import Path
from typing import Optional, List
import pandas as pd

from .core.database_manager import DatabaseManager
from .core.market_analytics import MarketAnalytics
from .core.risk_analytics import RiskAnalytics
from .config import get_connection_config

logger = logging.getLogger(__name__)


def setup_analytics(host: str, port: int, database: str, user: str = None, password: str = None) -> tuple:
    """Setup analytics components with PostgreSQL database."""
    try:
        # Initialize database manager
        db_manager = DatabaseManager(
            host=host,
            port=port,
            database=database,
            user=user,
            password=password
        )
        
        # Initialize analytics components
        market_analytics = MarketAnalytics(db_manager)
        risk_analytics = RiskAnalytics(db_manager)
        
        return db_manager, market_analytics, risk_analytics
        
    except Exception as e:
        logger.error(f"Failed to setup analytics: {e}")
        raise


def cmd_market_trends(args):
    """Analyze market trends."""
    print(f"📈 Analyzing market trends...")
    print(f"Database: {args.database}")
    print(f"County: {args.county or 'All counties'}")
    print(f"Period: {args.period} months")
    
    _, market_analytics, _ = setup_analytics(args.host, args.port, args.database, args.user, args.password)
    
    try:
        # Analyze market trends
        trends = market_analytics.analyze_market_trends(
            county_fips=args.county,
            time_period_months=args.period,
            property_types=args.property_types
        )
        
        if 'error' in trends:
            print(f"\n❌ {trends['error']}")
            return
        
        print(f"\n✅ Market trend analysis completed")
        print("=" * 80)
        
        # Overall trends
        overall = trends['overall_trends']
        print(f"\n📊 Overall Market Trends ({trends['period_analyzed']}):")
        print(f"  Total Properties: {trends['total_properties']:,}")
        print(f"  Date Range: {trends['date_range']['start']} to {trends['date_range']['end']}")
        print(f"  Avg Monthly Value Growth: {overall['avg_monthly_value_growth']:.2%}")
        print(f"  Avg Monthly Volume Growth: {overall['avg_monthly_volume_growth']:.2%}")
        print(f"  Current Avg Value: ${overall['current_avg_value']:,.0f}")
        print(f"  Value Volatility: {overall['value_volatility']:.2%}")
        
        # Trends by property type
        if trends['trends_by_type']:
            print(f"\n🏠 Trends by Property Type:")
            for prop_type, data in trends['trends_by_type'].items():
                print(f"\n  {prop_type}:")
                print(f"    Properties: {data['property_count']:,}")
                print(f"    Avg Value: ${data['avg_value']:,.0f}")
                print(f"    Avg Growth Rate: {data['avg_growth_rate']:.2%}")
                print(f"    Value Range: ${data['value_range']['min']:,.0f} - ${data['value_range']['max']:,.0f}")
        
        # Export results if requested
        if args.output:
            output_path = Path(args.output)
            
            # Create summary DataFrame
            summary_data = []
            for prop_type, data in trends['trends_by_type'].items():
                summary_data.append({
                    'property_type': prop_type,
                    'property_count': data['property_count'],
                    'avg_value': data['avg_value'],
                    'avg_growth_rate': data['avg_growth_rate'],
                    'min_value': data['value_range']['min'],
                    'max_value': data['value_range']['max']
                })
            
            if summary_data:
                summary_df = pd.DataFrame(summary_data)
                summary_df.to_csv(output_path, index=False)
                print(f"\n💾 Results exported to: {output_path}")
        
    except Exception as e:
        logger.error(f"Failed to analyze market trends: {e}")
        print(f"\n❌ Error: {e}")
        sys.exit(1)


def cmd_build_model(args):
    """Build predictive valuation model."""
    print(f"🤖 Building valuation model...")
    print(f"Database: {args.database}")
    print(f"Model Type: {args.model_type}")
    print(f"Target: {args.target}")
    print(f"County: {args.county or 'All counties'}")
    
    _, market_analytics, _ = setup_analytics(args.host, args.port, args.database, args.user, args.password)
    
    try:
        # Build model
        results = market_analytics.build_valuation_model(
            target_column=args.target,
            feature_columns=args.features,
            model_type=args.model_type,
            county_fips=args.county,
            test_size=args.test_size
        )
        
        print(f"\n✅ Model training completed")
        print("=" * 80)
        
        # Model performance
        perf = results['performance']
        print(f"\n📊 Model Performance:")
        print(f"  Model Type: {results['model_type']}")
        print(f"  Target: {results['target_column']}")
        print(f"  Training Samples: {results['training_samples']:,}")
        print(f"  Test Samples: {results['test_samples']:,}")
        print(f"  R² Score: {perf['r2_score']:.3f}")
        print(f"  RMSE: ${perf['rmse']:,.0f}")
        print(f"  MAE: ${perf['mae']:,.0f}")
        print(f"  MAPE: {perf['mean_absolute_percentage_error']:.1f}%")
        
        # Feature importance
        if results['feature_importance']:
            print(f"\n🎯 Feature Importance:")
            for feature, importance in list(results['feature_importance'].items())[:5]:
                print(f"  {feature}: {importance:.3f}")
        
        print(f"\n🔑 Model Key: {results['model_key']}")
        print("Use this key to make predictions with the trained model.")
        
        # Save model info if requested
        if args.output:
            output_path = Path(args.output)
            
            # Create model summary
            model_summary = {
                'model_key': results['model_key'],
                'model_type': results['model_type'],
                'target_column': results['target_column'],
                'performance': results['performance'],
                'feature_importance': results['feature_importance']
            }
            
            import json
            with open(output_path, 'w') as f:
                json.dump(model_summary, f, indent=2)
            
            print(f"\n💾 Model summary saved to: {output_path}")
        
    except Exception as e:
        logger.error(f"Failed to build model: {e}")
        print(f"\n❌ Error: {e}")
        sys.exit(1)


def cmd_predict_values(args):
    """Predict property values."""
    print(f"🔮 Predicting property values...")
    print(f"Database: {args.database}")
    print(f"Model Key: {args.model_key}")
    print(f"Parcels: {len(args.parcel_ids)}")
    
    _, market_analytics, _ = setup_analytics(args.host, args.port, args.database, args.user, args.password)
    
    try:
        # Make predictions
        predictions = market_analytics.predict_property_values(
            parcel_ids=args.parcel_ids,
            model_key=args.model_key
        )
        
        print(f"\n✅ Predictions completed for {len(predictions)} parcels")
        print("=" * 80)
        
        # Show results
        print(f"\n🎯 Prediction Results:")
        for _, row in predictions.head(args.limit).iterrows():
            print(f"\n  Parcel: {row['parno']}")
            print(f"    Actual Value: ${row['actual_value']:,.0f}")
            print(f"    Predicted Value: ${row['predicted_value']:,.0f}")
            print(f"    Error: ${row['prediction_error']:,.0f} ({row['percentage_error']:+.1f}%)")
        
        # Summary statistics
        print(f"\n📊 Prediction Summary:")
        print(f"  Mean Absolute Error: ${predictions['prediction_error'].abs().mean():,.0f}")
        print(f"  Mean Percentage Error: {predictions['percentage_error'].abs().mean():.1f}%")
        print(f"  Predictions within 10%: {(predictions['percentage_error'].abs() <= 10).mean():.1%}")
        print(f"  Predictions within 20%: {(predictions['percentage_error'].abs() <= 20).mean():.1%}")
        
        # Export results if requested
        if args.output:
            output_path = Path(args.output)
            predictions.to_csv(output_path, index=False)
            print(f"\n💾 Predictions exported to: {output_path}")
        
    except Exception as e:
        logger.error(f"Failed to predict values: {e}")
        print(f"\n❌ Error: {e}")
        sys.exit(1)


def cmd_market_analysis(args):
    """Perform comparative market analysis."""
    print(f"🏘️ Performing comparative market analysis...")
    print(f"Database: {args.database}")
    print(f"Target Parcel: {args.parcel_id}")
    print(f"Search Radius: {args.radius}m")
    
    _, market_analytics, _ = setup_analytics(args.host, args.port, args.database, args.user, args.password)
    
    try:
        # Perform CMA
        cma_results = market_analytics.comparative_market_analysis(
            target_parcel_id=args.parcel_id,
            radius_meters=args.radius,
            max_comparables=args.max_comparables,
            property_type_match=args.match_type
        )
        
        if 'error' in cma_results:
            print(f"\n❌ {cma_results['error']}")
            return
        
        print(f"\n✅ CMA completed")
        print("=" * 80)
        
        # Target property
        target = cma_results['target_parcel']
        print(f"\n🎯 Target Property:")
        print(f"  Parcel ID: {target['parno']}")
        print(f"  Current Value: ${target['current_value']:,.0f}")
        print(f"  Acres: {target['acres']:.2f}" if target['acres'] else "  Acres: N/A")
        print(f"  Property Type: {target['property_type']}")
        
        # Comparables summary
        comp = cma_results['comparables']
        stats = comp['value_statistics']
        print(f"\n🏠 Comparable Properties ({comp['count']} found):")
        print(f"  Average Distance: {comp['avg_distance_meters']:.0f}m")
        print(f"  Value Statistics:")
        print(f"    Mean: ${stats['mean']:,.0f}")
        print(f"    Median: ${stats['median']:,.0f}")
        print(f"    Range: ${stats['min']:,.0f} - ${stats['max']:,.0f}")
        print(f"    Std Dev: ${stats['std']:,.0f}")
        
        # Analysis
        analysis = cma_results['analysis']
        print(f"\n📊 Market Analysis:")
        print(f"  Target vs Mean Difference: ${analysis['target_vs_mean_diff']:+,.0f}")
        print(f"  Target vs Mean Percentage: {analysis['target_vs_mean_pct']:+.1f}%")
        print(f"  Target Percentile: {analysis['target_percentile']:.0f}th percentile")
        print(f"  Suggested Value Range: ${analysis['suggested_value_range']['low']:,.0f} - ${analysis['suggested_value_range']['high']:,.0f}")
        
        # Export results if requested
        if args.output:
            output_path = Path(args.output)
            
            # Create comparables DataFrame
            comparables_df = pd.DataFrame(cma_results['comparable_properties'])
            comparables_df.to_csv(output_path, index=False)
            print(f"\n💾 Comparable properties exported to: {output_path}")
        
    except Exception as e:
        logger.error(f"Failed to perform CMA: {e}")
        print(f"\n❌ Error: {e}")
        sys.exit(1)


def cmd_investment_scoring(args):
    """Score investment opportunities."""
    print(f"💰 Scoring investment opportunities...")
    print(f"Database: {args.database}")
    print(f"County: {args.county or 'All counties'}")
    print(f"Value Range: ${args.min_value or 0:,} - ${args.max_value or 'unlimited'}")
    
    _, market_analytics, _ = setup_analytics(args.host, args.port, args.database, args.user, args.password)
    
    try:
        # Score opportunities
        opportunities = market_analytics.investment_opportunity_scoring(
            county_fips=args.county,
            min_value=args.min_value,
            max_value=args.max_value,
            limit=args.limit
        )
        
        print(f"\n✅ Investment scoring completed for {len(opportunities)} properties")
        print("=" * 80)
        
        # Show top opportunities
        print(f"\n🏆 Top Investment Opportunities:")
        for i, (_, row) in enumerate(opportunities.head(10).iterrows(), 1):
            print(f"\n  #{i} - Parcel: {row['parno']}")
            print(f"    Investment Score: {row['investment_score']:.1f}/100")
            print(f"    Total Value: ${row['total_value']:,.0f}")
            print(f"    Acres: {row['acres']:.2f}")
            print(f"    Property Type: {row['property_type']}")
            print(f"    Value per Acre: ${row['value_per_acre']:,.0f}")
            print(f"    Land Ratio: {row['land_ratio']:.1%}")
        
        # Summary statistics
        print(f"\n📊 Investment Summary:")
        print(f"  Average Investment Score: {opportunities['investment_score'].mean():.1f}")
        print(f"  Score Range: {opportunities['investment_score'].min():.1f} - {opportunities['investment_score'].max():.1f}")
        print(f"  Average Value per Acre: ${opportunities['value_per_acre'].mean():,.0f}")
        print(f"  Average Property Size: {opportunities['acres'].mean():.1f} acres")
        
        # Export results if requested
        if args.output:
            output_path = Path(args.output)
            opportunities.to_csv(output_path, index=False)
            print(f"\n💾 Investment opportunities exported to: {output_path}")
        
    except Exception as e:
        logger.error(f"Failed to score investment opportunities: {e}")
        print(f"\n❌ Error: {e}")
        sys.exit(1)


def cmd_risk_assessment(args):
    """Perform comprehensive risk assessment."""
    print(f"⚠️ Performing risk assessment...")
    print(f"Database: {args.database}")
    print(f"County: {args.county or 'All counties'}")
    print(f"Risk Types: {', '.join(args.risk_types)}")
    
    _, risk_analytics, _ = setup_analytics(args.host, args.port, args.database, args.user, args.password)
    
    try:
        if 'comprehensive' in args.risk_types:
            # Comprehensive risk assessment
            risk_results = risk_analytics.comprehensive_risk_assessment(
                parcel_ids=args.parcel_ids,
                county_fips=args.county
            )
            
            if risk_results.empty:
                print(f"\n❌ No risk data found for specified criteria")
                return
            
            print(f"\n✅ Comprehensive risk assessment completed for {len(risk_results)} properties")
            print("=" * 80)
            
            # Risk level distribution
            risk_dist = risk_results['overall_risk_level'].value_counts()
            print(f"\n📊 Risk Level Distribution:")
            for level, count in risk_dist.items():
                print(f"  {level}: {count} properties ({count/len(risk_results):.1%})")
            
            # High-risk properties
            high_risk = risk_results[risk_results['composite_risk_score'] >= 75]
            if not high_risk.empty:
                print(f"\n🚨 High-Risk Properties (Score ≥ 75):")
                for _, row in high_risk.head(5).iterrows():
                    print(f"\n  Parcel: {row['parno']}")
                    print(f"    Risk Score: {row['composite_risk_score']:.1f}/100")
                    print(f"    Risk Level: {row['overall_risk_level']}")
                    print(f"    Flood Risk: {row['flood_risk_score']:.0f}")
                    print(f"    Tax Risk: {row['tax_risk_score']:.0f}")
                    print(f"    Market Risk: {row['market_risk_score']:.0f}")
                    print(f"    Total Potential Impact: ${row['total_potential_impact']:,.0f}")
                    
                    # Show recommendations
                    if row['risk_recommendations']:
                        print(f"    Recommendations: {', '.join(row['risk_recommendations'][:3])}")
            
            # Summary statistics
            print(f"\n📈 Risk Summary:")
            print(f"  Average Risk Score: {risk_results['composite_risk_score'].mean():.1f}")
            print(f"  High Risk Properties: {(risk_results['composite_risk_score'] >= 75).sum()}")
            print(f"  Total Potential Impact: ${risk_results['total_potential_impact'].sum():,.0f}")
            
        elif 'flood' in args.risk_types:
            # Flood risk only
            flood_risk = risk_analytics.assess_flood_risk(
                parcel_ids=args.parcel_ids,
                county_fips=args.county
            )
            
            if not flood_risk.empty:
                print(f"\n🌊 Flood Risk Assessment ({len(flood_risk)} properties):")
                flood_dist = flood_risk['flood_risk_level'].value_counts()
                for level, count in flood_dist.items():
                    print(f"  {level} Risk: {count} properties")
        
        elif 'tax' in args.risk_types:
            # Tax risk only
            tax_risk = risk_analytics.assess_tax_assessment_risk(
                parcel_ids=args.parcel_ids,
                county_fips=args.county
            )
            
            if not tax_risk.empty:
                print(f"\n💰 Tax Assessment Risk ({len(tax_risk)} properties):")
                tax_dist = tax_risk['tax_increase_risk'].value_counts()
                for level, count in tax_dist.items():
                    print(f"  {level} Risk: {count} properties")
        
        # Export results if requested
        if args.output and 'comprehensive' in args.risk_types:
            output_path = Path(args.output)
            risk_results.to_csv(output_path, index=False)
            print(f"\n💾 Risk assessment exported to: {output_path}")
        
    except Exception as e:
        logger.error(f"Failed to perform risk assessment: {e}")
        print(f"\n❌ Error: {e}")
        sys.exit(1)


def main():
    """Main CLI entry point."""
    parser = argparse.ArgumentParser(
        description="ParcelPy Analytics CLI - Advanced real estate analytics using PostgreSQL",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
  # Analyze market trends for Wake County over 24 months
  parcelpy-analytics market-trends --host localhost --database parcelpy --county 37183 --period 24
  
  # Build a Random Forest valuation model
  parcelpy-analytics build-model --host localhost --database parcelpy --model-type random_forest
  
  # Perform comparative market analysis for a specific parcel
  parcelpy-analytics market-analysis --host localhost --database parcelpy --parcel-id 1234567
  
  # Score investment opportunities (top 50)
  parcelpy-analytics investment-scoring --host localhost --database parcelpy --limit 50
  
  # Comprehensive risk assessment
  parcelpy-analytics risk-assessment --host localhost --database parcelpy --risk-types comprehensive
  
  # Using custom PostgreSQL connection
  parcelpy-analytics market-trends --host myserver.com --port 5432 --database parcels --user myuser --county 37183
        """
    )
    
    # Global arguments
    parser.add_argument('--host', default='localhost', help='PostgreSQL host (default: localhost)')
    parser.add_argument('--port', type=int, default=5432, help='PostgreSQL port (default: 5432)')
    parser.add_argument('--database', '-d', required=True, help='PostgreSQL database name')
    parser.add_argument('--user', '-u', help='PostgreSQL user (default: from config)')
    parser.add_argument('--password', '-p', help='PostgreSQL password (default: from config)')
    parser.add_argument('--verbose', '-v', action='store_true', help='Enable verbose logging')
    parser.add_argument('--output-dir', default='./analytics_output', 
                       help='Directory for output files (default: ./analytics_output)')
    
    subparsers = parser.add_subparsers(dest='command', help='Available analytics commands')
    
    # Market trends command
    trends_parser = subparsers.add_parser('market-trends', help='Analyze market trends over time')
    trends_parser.add_argument('--county', help='County FIPS code (e.g., 37183 for Wake County)')
    trends_parser.add_argument('--period', type=int, default=24, 
                              help='Analysis period in months (default: 24)')
    trends_parser.add_argument('--property-types', nargs='+', 
                              help='Property types to include (e.g., residential commercial)')
    
    # Build model command
    model_parser = subparsers.add_parser('build-model', help='Build property valuation models')
    model_parser.add_argument('--model-type', choices=['random_forest', 'gradient_boosting', 'linear'], 
                             default='random_forest', help='Model type (default: random_forest)')
    model_parser.add_argument('--target', default='total_value', help='Target column (default: total_value)')
    model_parser.add_argument('--features', nargs='+', help='Feature columns to use')
    model_parser.add_argument('--county', help='County FIPS code to focus on')
    model_parser.add_argument('--test-size', type=float, default=0.2, 
                             help='Test set size (default: 0.2)')
    
    # Predict values command
    predict_parser = subparsers.add_parser('predict-values', help='Predict property values')
    predict_parser.add_argument('--parcel-ids', nargs='+', required=True, 
                               help='Parcel IDs to predict values for')
    predict_parser.add_argument('--model-key', required=True, help='Model key to use for predictions')
    
    # Market analysis command
    analysis_parser = subparsers.add_parser('market-analysis', help='Comparative market analysis')
    analysis_parser.add_argument('--parcel-id', required=True, help='Target parcel ID')
    analysis_parser.add_argument('--radius', type=float, default=1000, 
                                help='Search radius in meters (default: 1000)')
    analysis_parser.add_argument('--max-comparables', type=int, default=10, 
                                help='Maximum comparable properties (default: 10)')
    analysis_parser.add_argument('--property-type-match', action='store_true', 
                                help='Require property type match')
    
    # Investment scoring command
    investment_parser = subparsers.add_parser('investment-scoring', help='Score investment opportunities')
    investment_parser.add_argument('--county', help='County FIPS code')
    investment_parser.add_argument('--min-value', type=float, help='Minimum property value')
    investment_parser.add_argument('--max-value', type=float, help='Maximum property value')
    investment_parser.add_argument('--limit', type=int, default=100, 
                                  help='Number of opportunities to return (default: 100)')
    
    # Risk assessment command
    risk_parser = subparsers.add_parser('risk-assessment', help='Assess property risks')
    risk_parser.add_argument('--parcel-ids', nargs='*', help='Specific parcel IDs to assess')
    risk_parser.add_argument('--county', help='County FIPS code')
    risk_parser.add_argument('--risk-types', nargs='+', 
                           choices=['flood', 'market_volatility', 'tax_assessment', 'comprehensive'],
                           default=['comprehensive'], help='Types of risk to assess')
    
    args = parser.parse_args()
    
    if args.verbose:
        logging.basicConfig(level=logging.INFO)
    
    if not args.command:
        parser.print_help()
        return
    
    # Create output directory
    output_dir = Path(args.output_dir)
    output_dir.mkdir(parents=True, exist_ok=True)
    
    # Execute command
    command_map = {
        'market-trends': cmd_market_trends,
        'build-model': cmd_build_model,
        'predict-values': cmd_predict_values,
        'market-analysis': cmd_market_analysis,
        'investment-scoring': cmd_investment_scoring,
        'risk-assessment': cmd_risk_assessment
    }
    
    if args.command in command_map:
        command_map[args.command](args)
    else:
        print(f"Unknown command: {args.command}")
        parser.print_help()


if __name__ == "__main__":
    main() 