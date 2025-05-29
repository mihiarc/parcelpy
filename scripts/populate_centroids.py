#!/usr/bin/env python3
"""
Populate centroids column for all parcels in the consolidated table
"""

import sys
from pathlib import Path
from sqlalchemy import text
import time

# Add the src directory to the Python path
sys.path.insert(0, str(Path(__file__).parent.parent / "src"))

from parcelpy.database.core.database_manager import DatabaseManager

def main():
    db = DatabaseManager()
    
    with db.get_connection() as conn:
        try:
            print("🎯 POPULATING PARCEL CENTROIDS")
            print("=" * 50)
            
            # Check if parcels table exists
            result = conn.execute(text("""
                SELECT EXISTS (
                    SELECT FROM information_schema.tables 
                    WHERE table_name = 'parcels'
                );
            """))
            table_exists = result.fetchone()[0]
            
            if not table_exists:
                print("❌ 'parcels' table not found. Run consolidation first.")
                return
            
            # Check current state
            result = conn.execute(text('SELECT COUNT(*) FROM parcels'))
            total_parcels = result.fetchone()[0]
            print(f"📦 Total parcels: {total_parcels:,}")
            
            # Check if centroid column exists
            result = conn.execute(text("""
                SELECT EXISTS (
                    SELECT FROM information_schema.columns 
                    WHERE table_name = 'parcels' AND column_name = 'centroid'
                );
            """))
            centroid_exists = result.fetchone()[0]
            
            if not centroid_exists:
                print("🏗️  Adding centroid column...")
                conn.execute(text('ALTER TABLE parcels ADD COLUMN centroid geometry(POINT, 4326)'))
                conn.commit()
                print("✅ Centroid column added")
            else:
                # Check how many already have centroids
                result = conn.execute(text('SELECT COUNT(*) FROM parcels WHERE centroid IS NOT NULL'))
                existing_centroids = result.fetchone()[0]
                print(f"📍 Existing centroids: {existing_centroids:,}")
                
                if existing_centroids > 0:
                    response = input(f"Do you want to recalculate all centroids? (y/N): ").strip().lower()
                    if response != 'y':
                        print("❌ Centroid population cancelled")
                        return
            
            # Check geometry coverage
            result = conn.execute(text('SELECT COUNT(*) FROM parcels WHERE geometry IS NOT NULL'))
            parcels_with_geometry = result.fetchone()[0]
            print(f"🗺️  Parcels with geometry: {parcels_with_geometry:,}")
            
            if parcels_with_geometry == 0:
                print("❌ No parcels have geometry data. Cannot calculate centroids.")
                return
            
            # Calculate centroids in batches for better performance
            batch_size = 10000
            total_batches = (parcels_with_geometry + batch_size - 1) // batch_size
            
            print(f"\n🔄 Calculating centroids in batches of {batch_size:,}...")
            print(f"📊 Total batches: {total_batches:,}")
            
            start_time = time.time()
            processed = 0
            
            # Process in batches using row_number() for consistent pagination
            for batch in range(total_batches):
                batch_start_time = time.time()
                
                try:
                    # Update centroids for this batch
                    result = conn.execute(text(f'''
                        UPDATE parcels 
                        SET centroid = ST_Centroid(geometry)
                        WHERE geometry IS NOT NULL 
                        AND parno IN (
                            SELECT parno FROM (
                                SELECT parno, ROW_NUMBER() OVER (ORDER BY parno) as rn
                                FROM parcels 
                                WHERE geometry IS NOT NULL
                            ) ranked
                            WHERE rn > {batch * batch_size} 
                            AND rn <= {(batch + 1) * batch_size}
                        );
                    '''))
                    
                    batch_processed = result.rowcount
                    processed += batch_processed
                    
                    batch_elapsed = time.time() - batch_start_time
                    total_elapsed = time.time() - start_time
                    
                    # Calculate progress and ETA
                    progress = (batch + 1) / total_batches * 100
                    if batch > 0:
                        avg_batch_time = total_elapsed / (batch + 1)
                        eta_seconds = avg_batch_time * (total_batches - batch - 1)
                        eta_minutes = eta_seconds / 60
                        eta_str = f"ETA: {eta_minutes:.1f}m"
                    else:
                        eta_str = "ETA: calculating..."
                    
                    print(f"  Batch {batch + 1:4}/{total_batches}: {batch_processed:>6,} centroids ({batch_elapsed:.1f}s) - {progress:5.1f}% - {eta_str}")
                    
                    # Commit every 10 batches
                    if (batch + 1) % 10 == 0:
                        conn.commit()
                        
                except Exception as e:
                    print(f"  Batch {batch + 1:4}/{total_batches}: ERROR - {str(e)[:50]}...")
                    conn.rollback()
                    continue
            
            # Final commit
            conn.commit()
            
            total_elapsed = time.time() - start_time
            
            print(f"\n🎯 CENTROID CALCULATION COMPLETE:")
            print(f"  Processed: {processed:,} parcels")
            print(f"  Total time: {total_elapsed:.1f} seconds")
            print(f"  Rate: {processed/total_elapsed:.0f} parcels/second")
            
            # Verify results
            result = conn.execute(text('SELECT COUNT(*) FROM parcels WHERE centroid IS NOT NULL'))
            final_centroids = result.fetchone()[0]
            
            result = conn.execute(text('SELECT COUNT(*) FROM parcels WHERE geometry IS NOT NULL'))
            total_with_geometry = result.fetchone()[0]
            
            print(f"\n✅ VERIFICATION:")
            print(f"  Parcels with centroids: {final_centroids:,}")
            print(f"  Parcels with geometry: {total_with_geometry:,}")
            print(f"  Success rate: {final_centroids/total_with_geometry*100:.1f}%")
            
            # Create index on centroid column for fast spatial queries
            print(f"\n🔧 Creating spatial index on centroids...")
            try:
                conn.execute(text('CREATE INDEX IF NOT EXISTS idx_parcels_centroid ON parcels USING GIST(centroid)'))
                conn.commit()
                print("✅ Centroid spatial index created")
            except Exception as e:
                print(f"⚠️  Index creation warning: {e}")
            
            # Sample centroid coordinates
            print(f"\n📍 Sample centroid coordinates:")
            result = conn.execute(text('''
                SELECT parno, county, 
                       ST_Y(centroid) as lat, ST_X(centroid) as lon
                FROM parcels 
                WHERE centroid IS NOT NULL 
                ORDER BY RANDOM()
                LIMIT 5;
            '''))
            
            for row in result.fetchall():
                print(f"  Parcel {row.parno} ({row.county}): {row.lat:.6f}, {row.lon:.6f}")
            
            print(f"\n🚀 Centroids ready for fast point-based spatial analysis!")
            print(f"💡 Use centroid column for distance queries, clustering, and point-in-polygon operations")
            
        except Exception as e:
            print(f'❌ Error during centroid calculation: {e}')
            import traceback
            traceback.print_exc()

if __name__ == "__main__":
    main() 