"""
Module for creating a visual comparison of land area units.
"""

import matplotlib.pyplot as plt
import matplotlib.patches as patches

def create_area_comparison(output_path=None):
    """Create a visual comparison between a hectare and an acre."""
    # Create figure and axis with extra space for labels
    fig, ax = plt.subplots(figsize=(14, 10))
    
    # Draw a hectare (100m x 100m = 10,000 sq m)
    hectare = patches.Rectangle(
        (0, 0), 100, 100,
        facecolor='lightgreen',
        alpha=0.3
    )
    
    # Draw an acre (63.61m x 63.61m ≈ 4,047 sq m)
    acre = patches.Rectangle(
        (0, 0), 63.61, 63.61,
        facecolor='lightblue',
        alpha=0.3
    )
    
    # Add shapes to plot
    ax.add_patch(hectare)
    ax.add_patch(acre)
    
    # Add grid for scale reference
    ax.grid(True, linestyle='--', alpha=0.2)
    
    # Set axis limits with more padding for labels
    ax.set_xlim(-15, 115)  # Reduced right margin since fact box is inside
    ax.set_ylim(-20, 120)
    
    # Add labels
    ax.set_xlabel('Meters', fontsize=12, labelpad=10)
    ax.set_ylabel('Meters', fontsize=12, labelpad=10)
    ax.set_title('Size Comparison: 1 Hectare vs 1 Acre', fontsize=14, pad=20)
    
    # Add dimension lines and labels for hectare
    # Horizontal dimension
    plt.arrow(-2, -5, 104, 0, head_width=2, head_length=2, fc='black', ec='black', 
              length_includes_head=True, clip_on=False)
    plt.text(50, -10, '100 meters', ha='center', va='top', fontsize=10)
    
    # Vertical dimension
    plt.arrow(-5, -2, 0, 104, head_width=2, head_length=2, fc='black', ec='black', 
              length_includes_head=True, clip_on=False)
    plt.text(-10, 50, '100 meters', va='center', ha='right', rotation=90, fontsize=10)
    
    # Add dimension lines and labels for acre
    # Horizontal dimension only (to avoid clutter)
    plt.arrow(-2, -15, 67.61, 0, head_width=2, head_length=2, fc='blue', ec='blue', 
              length_includes_head=True, clip_on=False)
    plt.text(31.8, -18, '63.61 meters', ha='center', va='top', color='blue', fontsize=10)
    
    # Add area labels in the center of each shape
    # Hectare label - moved to top right quadrant
    plt.text(75, 75, 'HECTARE', 
             ha='center', va='bottom', fontsize=12, color='darkgreen', weight='bold')
    plt.text(75, 70, '10,000 square meters', 
             ha='center', va='top', fontsize=10, color='darkgreen')
    
    # Acre label
    plt.text(31.8, 31.8, 'ACRE', 
             ha='center', va='bottom', fontsize=12, color='darkblue', weight='bold')
    plt.text(31.8, 26.8, '4,047 square meters', 
             ha='center', va='top', fontsize=10, color='darkblue')
    
    # Add comparison facts in a neat box
    facts = [
        'Area Comparisons:',
        '────────────────',
        '1 Hectare ≈ 2.47 Acres',
        '',
        'Dimensions:',
        '────────────',
        '1 Hectare = 100m × 100m',
        '1 Acre ≈ 63.61m × 63.61m',
        '',
        'Real-world Examples:',
        '──────────────────',
        'A hectare is about the size of:',
        '• 2.5 American football fields',
        '• 1 rugby field'
    ]
    
    # Create fact box with light background - moved inside plot frame
    fact_box = plt.text(102, 90, '\n'.join(facts), 
                       fontsize=9,  # Slightly smaller font
                       va='top', 
                       bbox=dict(facecolor='white', 
                                alpha=0.9,  # More opaque background
                                edgecolor='gray',
                                boxstyle='round,pad=0.8',
                                mutation_scale=0.8))  # Slightly smaller padding
    
    # Equal aspect ratio to ensure square appearance
    ax.set_aspect('equal')
    
    # Adjust layout to prevent text cutoff
    plt.tight_layout()
    
    # Save if output path provided
    if output_path:
        plt.savefig(output_path, bbox_inches='tight', dpi=300)
        plt.close()
    else:
        plt.show()

if __name__ == "__main__":
    import argparse
    
    parser = argparse.ArgumentParser(description="Create area comparison visualization")
    parser.add_argument("--output", "-o", type=str, help="Path to save plot", default=None)
    
    args = parser.parse_args()
    
    # Use default output name if none provided
    if args.output is None:
        args.output = 'area_unit_comparison.png'
        
    create_area_comparison(args.output) 