#!/usr/bin/env python3
"""
Monitor Nash County processing progress in real-time
"""

import sys
import time
from pathlib import Path
from sqlalchemy import text

# Add the src directory to the Python path
sys.path.insert(0, str(Path(__file__).parent.parent / "src"))

from parcelpy.database.core.database_manager import DatabaseManager

def get_progress():
    """Get current processing progress."""
    
    db_manager = DatabaseManager()
    
    with db_manager.get_connection() as conn:
        # Total parcels
        result = conn.execute(text("""
            SELECT COUNT(*) FROM nash_parcels WHERE centroid IS NOT NULL
        """))
        total_parcels = result.fetchone()[0]
        
        # Processed parcels
        result = conn.execute(text("""
            SELECT COUNT(DISTINCT parno) FROM parcel_demographics_full WHERE county = 'Nash'
        """))
        processed_parcels = result.fetchone()[0]
        
        # Recent activity (last 2 minutes)
        result = conn.execute(text("""
            SELECT COUNT(*) FROM parcel_demographics_full 
            WHERE county = 'Nash' 
            AND created_at > NOW() - INTERVAL '2 minutes'
        """))
        recent_activity = result.fetchone()[0]
        
        return total_parcels, processed_parcels, recent_activity

def main():
    """Monitor progress with live updates."""
    
    print("🔍 Nash County Processing Monitor")
    print("Press Ctrl+C to stop monitoring\n")
    
    try:
        last_processed = 0
        
        while True:
            total, processed, recent = get_progress()
            remaining = total - processed
            progress_pct = (processed / total * 100) if total > 0 else 0
            
            # Calculate processing rate
            new_processed = processed - last_processed
            rate_per_minute = new_processed / 0.5 if new_processed > 0 else 0  # 30-second intervals
            
            # Clear line and print progress
            print(f"\r📊 Progress: {processed}/{total} ({progress_pct:.1f}%) | Remaining: {remaining} | Recent: {recent} | Rate: {rate_per_minute:.1f}/min", end="", flush=True)
            
            # Check if complete
            if processed >= total:
                print(f"\n🎉 Processing complete! All {total} parcels processed.")
                break
            
            last_processed = processed
            time.sleep(30)  # Update every 30 seconds
            
    except KeyboardInterrupt:
        print(f"\n👋 Monitoring stopped")

if __name__ == "__main__":
    main() 