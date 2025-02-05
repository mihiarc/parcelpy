"""
Verify land use transitions directly from the data.
"""

import pandas as pd
import logging

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

def analyze_transitions(results_path: str, min_area: float = 1.0):
    """Analyze land use transitions directly."""
    
    # Read data
    df = pd.read_csv(results_path)
    logger.info(f"Loaded {len(df)} records")
    
    # Get only changes
    changes = df[df['start_lu_class'] != df['end_lu_class']].copy()
    logger.info(f"Found {len(changes)} changes")
    
    # Calculate total areas
    total_area = df['area_ha'].sum()
    changed_area = changes['area_ha'].sum()
    logger.info(f"\nArea Summary:")
    logger.info(f"Total area: {total_area:.2f} ha")
    logger.info(f"Changed area: {changed_area:.2f} ha")
    logger.info(f"Percent changed: {(changed_area/total_area)*100:.1f}%")
    
    # Analyze transitions
    transitions = changes.groupby(['start_lu_class', 'end_lu_class']).agg({
        'area_ha': ['sum', 'count']
    }).round(2)
    transitions.columns = ['Total Area (ha)', 'Count']
    transitions = transitions.reset_index()
    
    # Sort by area and filter by minimum area
    transitions = transitions[transitions['Total Area (ha)'] >= min_area].sort_values(
        'Total Area (ha)', ascending=False
    )
    
    logger.info(f"\nTransitions >= {min_area} ha:")
    logger.info("-" * 80)
    logger.info(f"{'From':20s} {'To':20s} {'Area (ha)':>12s} {'Count':>8s} {'% of Changes':>12s}")
    logger.info("-" * 80)
    
    for _, row in transitions.iterrows():
        pct = (row['Total Area (ha)'] / changed_area) * 100
        logger.info(f"{row['start_lu_class']:20s} {row['end_lu_class']:20s} {row['Total Area (ha)']:12.2f} {row['Count']:8.0f} {pct:11.1f}%")
    
    # Verify flow balance for each class
    logger.info(f"\nFlow Balance by Class:")
    logger.info("-" * 50)
    
    for lu_class in df['start_lu_class'].unique():
        start_area = changes[changes['start_lu_class'] == lu_class]['area_ha'].sum()
        end_area = changes[changes['end_lu_class'] == lu_class]['area_ha'].sum()
        net_change = end_area - start_area
        
        logger.info(f"\n{lu_class}:")
        logger.info(f"  Area lost:    {start_area:10.2f} ha")
        logger.info(f"  Area gained:  {end_area:10.2f} ha")
        logger.info(f"  Net change:   {net_change:10.2f} ha")
        
        # Show detailed flows
        logger.info("  Losses to:")
        losses = changes[changes['start_lu_class'] == lu_class].groupby('end_lu_class')['area_ha'].sum()
        for end_class, area in losses.items():
            logger.info(f"    → {end_class:20s}: {area:10.2f} ha")
        
        logger.info("  Gains from:")
        gains = changes[changes['end_lu_class'] == lu_class].groupby('start_lu_class')['area_ha'].sum()
        for start_class, area in gains.items():
            logger.info(f"    ← {start_class:20s}: {area:10.2f} ha")

if __name__ == "__main__":
    analyze_transitions("tests/outputs/land_use_changes.csv", min_area=1.0) 