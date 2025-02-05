"""
Script to generate all visualizations from the merged land use change data.
"""

import os
from pathlib import Path
import logging
from matrix_plot import create_matrix_plot
from sankey import create_sankey_diagram

# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

def generate_visualizations(
    input_file: str = "data/processed/merged_land_use_changes.csv",
    output_dir: str = "outputs/figures",
    min_flow_threshold: float = 1.0,
    dpi: int = 300
) -> None:
    """
    Generate all visualizations from the merged land use change data.
    
    Args:
        input_file: Path to the merged CSV file
        output_dir: Directory to save the visualizations
        min_flow_threshold: Minimum area (ha) threshold for Sankey diagram flows
        dpi: Resolution of output images
    """
    try:
        # Create output directory
        output_path = Path(output_dir)
        output_path.mkdir(parents=True, exist_ok=True)
        
        # Generate matrix plot
        logger.info("Generating matrix plot...")
        matrix_output = output_path / "land_use_matrix.png"
        create_matrix_plot(
            results_path=input_file,
            output_path=matrix_output,
            title="Land Use Transition Matrix (1985-2023)",
            dpi=dpi
        )
        logger.info(f"Matrix plot saved to {matrix_output}")
        
        # Generate Sankey diagram
        logger.info("Generating Sankey diagram...")
        sankey_output = output_path / "land_use_sankey.png"
        create_sankey_diagram(
            results_path=input_file,
            output_path=sankey_output,
            min_flow_threshold=min_flow_threshold,
            title="Land Use Transitions (1985-2023)",
            dpi=dpi
        )
        logger.info(f"Sankey diagram saved to {sankey_output}")
        
    except Exception as e:
        logger.error(f"Error generating visualizations: {str(e)}")
        raise

if __name__ == "__main__":
    import argparse
    
    parser = argparse.ArgumentParser(description="Generate all visualizations from merged land use change data")
    parser.add_argument("--input", "-i", 
                       default="data/processed/merged_land_use_changes.csv",
                       help="Path to merged land use changes CSV file")
    parser.add_argument("--output-dir", "-o",
                       default="outputs/figures",
                       help="Directory to save visualizations")
    parser.add_argument("--threshold", "-t",
                       type=float, default=1.0,
                       help="Minimum flow threshold for Sankey diagram (hectares)")
    parser.add_argument("--dpi",
                       type=int, default=300,
                       help="Output image resolution")
    
    args = parser.parse_args()
    
    generate_visualizations(
        input_file=args.input,
        output_dir=args.output_dir,
        min_flow_threshold=args.threshold,
        dpi=args.dpi
    ) 