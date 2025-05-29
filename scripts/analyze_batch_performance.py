#!/usr/bin/env python3
"""
Analyze batch processing performance to determine optimal batch size
"""

import sys
import pandas as pd
from pathlib import Path
from sqlalchemy import text
import matplotlib.pyplot as plt
import seaborn as sns

# Add the src directory to the Python path
sys.path.insert(0, str(Path(__file__).parent.parent / "src"))

from parcelpy.database.core.database_manager import DatabaseManager

def analyze_batch_performance():
    """Analyze processing performance by batch size and timing."""
    
    db_manager = DatabaseManager()
    
    with db_manager.get_connection() as conn:
        # Get processing log data with batch information
        result = conn.execute(text("""
            SELECT 
                batch_id,
                processing_status,
                processing_time_seconds,
                census_variables_count,
                created_at,
                COUNT(*) OVER (PARTITION BY batch_id) as batch_size
            FROM parcel_processing_log_full 
            WHERE county = 'Nash' 
            AND batch_id IS NOT NULL
            ORDER BY created_at
        """))
        
        data = []
        for row in result:
            data.append({
                'batch_id': row.batch_id,
                'status': row.processing_status,
                'processing_time': row.processing_time_seconds,
                'census_count': row.census_variables_count,
                'created_at': row.created_at,
                'batch_size': row.batch_size
            })
        
        if not data:
            print("❌ No batch processing data found")
            return
        
        df = pd.DataFrame(data)
        
        # Analyze by batch size
        batch_analysis = df.groupby(['batch_id', 'batch_size']).agg({
            'status': lambda x: (x == 'success').sum(),  # success count
            'processing_time': 'mean',
            'census_count': 'mean'
        }).reset_index()
        
        batch_analysis['success_rate'] = batch_analysis['status'] / batch_analysis['batch_size']
        batch_analysis['parcels_per_second'] = batch_analysis['batch_size'] / batch_analysis['processing_time']
        
        print("📊 BATCH PERFORMANCE ANALYSIS")
        print("=" * 60)
        
        # Overall statistics
        total_batches = len(batch_analysis)
        avg_batch_size = batch_analysis['batch_size'].mean()
        overall_success_rate = df[df['status'] == 'success'].shape[0] / df.shape[0]
        
        print(f"📦 Total Batches Analyzed: {total_batches}")
        print(f"📏 Average Batch Size: {avg_batch_size:.1f}")
        print(f"✅ Overall Success Rate: {overall_success_rate:.1%}")
        
        # Performance by batch size
        size_performance = batch_analysis.groupby('batch_size').agg({
            'success_rate': 'mean',
            'processing_time': 'mean',
            'parcels_per_second': 'mean',
            'batch_id': 'count'
        }).round(3)
        
        print(f"\n📈 Performance by Batch Size:")
        print("=" * 60)
        print(f"{'Size':<6} {'Batches':<8} {'Success%':<10} {'Avg Time':<10} {'Parcels/s':<12} {'Efficiency':<12}")
        print("-" * 60)
        
        for size, row in size_performance.iterrows():
            efficiency = row['parcels_per_second'] * row['success_rate']  # Effective parcels/second
            print(f"{size:<6} {row['batch_id']:<8} {row['success_rate']:.1%}      {row['processing_time']:<10.1f} {row['parcels_per_second']:<12.2f} {efficiency:<12.2f}")
        
        # Find optimal batch size
        size_performance['efficiency'] = size_performance['parcels_per_second'] * size_performance['success_rate']
        optimal_size = size_performance['efficiency'].idxmax()
        optimal_efficiency = size_performance.loc[optimal_size, 'efficiency']
        
        print(f"\n🎯 OPTIMAL BATCH SIZE: {optimal_size}")
        print(f"⚡ Peak Efficiency: {optimal_efficiency:.2f} effective parcels/second")
        
        # Recent performance trends
        recent_batches = batch_analysis.tail(10)
        if len(recent_batches) > 0:
            recent_avg_time = recent_batches['processing_time'].mean()
            recent_avg_rate = recent_batches['parcels_per_second'].mean()
            recent_success = recent_batches['success_rate'].mean()
            
            print(f"\n📅 Recent Performance (Last 10 Batches):")
            print(f"⏱️  Average Processing Time: {recent_avg_time:.1f}s")
            print(f"🚀 Average Rate: {recent_avg_rate:.2f} parcels/second")
            print(f"✅ Success Rate: {recent_success:.1%}")
        
        # Recommendations
        print(f"\n💡 RECOMMENDATIONS:")
        print("=" * 60)
        
        if optimal_size <= 10:
            print("🔸 Small batches (≤10) are optimal - likely due to:")
            print("  • Network locality (parcels are geographically close)")
            print("  • Lower memory usage")
            print("  • Faster failure recovery")
        elif optimal_size <= 30:
            print("🔸 Medium batches (10-30) are optimal - good balance of:")
            print("  • Cache efficiency")
            print("  • Processing overhead")
            print("  • Error isolation")
        else:
            print("🔸 Large batches (30+) are optimal - benefits from:")
            print("  • Maximum cache reuse")
            print("  • Reduced startup overhead")
            print("  • Bulk processing efficiency")
        
        # SocialMapper-specific considerations
        print(f"\n🗺️  SocialMapper Considerations:")
        print("• Road network caching is most effective with geographically close parcels")
        print("• Census API has rate limits that favor larger batches")
        print("• DuckDB memory usage increases with batch size")
        print("• Isochrone generation benefits from shared road networks")
        
        return size_performance

def test_batch_sizes():
    """Suggest a testing strategy for different batch sizes."""
    
    print(f"\n🧪 BATCH SIZE TESTING STRATEGY:")
    print("=" * 60)
    print("To find the optimal batch size, consider testing:")
    print("1. Small batches: 5, 10, 15 parcels")
    print("2. Medium batches: 20, 25, 30 parcels") 
    print("3. Large batches: 40, 50, 75 parcels")
    print("4. Very large batches: 100+ parcels")
    
    print(f"\nTest each size with ~50-100 parcels to get reliable metrics.")
    print(f"Monitor: processing time, success rate, memory usage, cache hits.")

def estimate_completion_time():
    """Estimate completion time based on current performance."""
    
    db_manager = DatabaseManager()
    
    with db_manager.get_connection() as conn:
        # Get remaining parcels
        result = conn.execute(text("""
            SELECT COUNT(*) FROM nash_parcels np
            WHERE np.centroid IS NOT NULL
            AND np.parno NOT IN (
                SELECT DISTINCT parno FROM parcel_demographics_full 
                WHERE county = 'Nash'
            )
        """))
        remaining = result.fetchone()[0]
        
        # Get recent processing rate
        result = conn.execute(text("""
            SELECT 
                COUNT(*) as recent_parcels,
                EXTRACT(EPOCH FROM (MAX(created_at) - MIN(created_at))) as time_span
            FROM parcel_processing_log_full 
            WHERE county = 'Nash' 
            AND processing_status = 'success'
            AND created_at > NOW() - INTERVAL '1 hour'
        """))
        
        recent_data = result.fetchone()
        
        if recent_data.recent_parcels > 0 and recent_data.time_span > 0:
            rate_per_second = recent_data.recent_parcels / recent_data.time_span
            eta_seconds = remaining / rate_per_second
            eta_hours = eta_seconds / 3600
            
            print(f"\n⏰ COMPLETION ESTIMATE:")
            print(f"📍 Remaining Parcels: {remaining:,}")
            print(f"🚀 Current Rate: {rate_per_second:.2f} parcels/second")
            print(f"⏱️  Estimated Time: {eta_hours:.1f} hours")
        else:
            print(f"\n⏰ Remaining Parcels: {remaining:,}")
            print("📊 Not enough recent data to estimate completion time")

def main():
    """Main analysis function."""
    
    try:
        performance_data = analyze_batch_performance()
        test_batch_sizes()
        estimate_completion_time()
        
    except Exception as e:
        print(f"❌ Error analyzing batch performance: {e}")

if __name__ == "__main__":
    main() 