#!/usr/bin/env python3
"""
Script to analyze and visualize land use changes over time.
Includes alluvial plots for transitions, transition matrices, and temporal trends.
"""

import pandas as pd
import numpy as np
import matplotlib.pyplot as plt
import seaborn as sns
from pathlib import Path
import logging
from datetime import datetime
from typing import Dict, List, Tuple, Optional

# Set up logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler(f'land_use_analysis_{datetime.now():%Y%m%d_%H%M%S}.log'),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger(__name__)

# Simplified Land Use Classifications
LAND_USE_CODES = {
    1: "Agriculture",
    2: "Developed",
    3: "Forest",  # Simplified from Forest/Wetland
    6: "Pasture"  # Simplified from Range/Pasture
}

class LandUseChangeAnalyzer:
    """Analyzes and visualizes land use changes over time."""
    
    def __init__(
        self,
        data_path: str,
        output_dir: str,
        start_year: int = 1985,
        end_year: int = 2023
    ):
        """Initialize the analyzer with data path and year range."""
        self.data_path = Path(data_path)
        self.output_dir = Path(output_dir)
        self.years = list(range(start_year, end_year + 1))
        self.output_dir.mkdir(parents=True, exist_ok=True)
        
        # Load and preprocess data
        self.df = self._load_and_preprocess_data()
        
        # Set up color palette for consistent visualization
        self.colors = {
            "Agriculture": "#ffd700",
            "Developed": "#ff4444",
            "Forest": "#228b22",
            "Pasture": "#deb887"
        }
    
    def _load_and_preprocess_data(self) -> pd.DataFrame:
        """Load and preprocess data with new land use groupings."""
        df = pd.read_csv(self.data_path)
        
        # Process each year column
        year_cols = [str(year) for year in self.years]
        for year in year_cols:
            # Combine Forest and Wetland (codes 3 and 4)
            df[year] = df[year].replace({4: 3})
            # Filter to keep only the classes we want
            valid_codes = list(LAND_USE_CODES.keys())
            mask = df[year].isin(valid_codes)
            df = df[mask]
        
        return df
    
    def create_transition_matrix(
        self,
        start_year: int,
        end_year: int
    ) -> pd.DataFrame:
        """Create a transition matrix between two years."""
        # Get land use codes for start and end years
        start_codes = self.df[str(start_year)]
        end_codes = self.df[str(end_year)]
        
        # Create transition matrix
        transitions = pd.crosstab(
            start_codes.map(LAND_USE_CODES),
            end_codes.map(LAND_USE_CODES),
            normalize='index'
        ) * 100
        
        return transitions
    
    def plot_transition_matrices(
        self,
        transition_years: List[Tuple[int, int]],
        min_value: float = 1.0
    ):
        """Plot side-by-side transition matrices."""
        fig, axes = plt.subplots(1, len(transition_years), figsize=(20, 8))
        
        for idx, (start_year, end_year) in enumerate(transition_years):
            transitions = self.create_transition_matrix(start_year, end_year)
            transitions = transitions.where(transitions >= min_value, 0)
            
            sns.heatmap(
                transitions,
                annot=True,
                fmt='.1f',
                cmap='YlOrRd',
                vmin=0,
                vmax=100,
                cbar_kws={'label': 'Percentage'},
                ax=axes[idx]
            )
            
            axes[idx].set_title(f'{start_year} → {end_year}\n(>={min_value}% shown)')
            axes[idx].set_xlabel(f'Land Use in {end_year}')
            if idx == 0:
                axes[idx].set_ylabel(f'Land Use in {start_year}')
            else:
                axes[idx].set_ylabel('')
        
        plt.tight_layout()
        output_path = self.output_dir / 'transition_matrices_comparison.png'
        plt.savefig(output_path, bbox_inches='tight', dpi=300)
        plt.close()
        
        logger.info(f"Saved transition matrices comparison to {output_path}")
    
    def plot_alluvial_comparison(
        self,
        transition_years: List[Tuple[int, int]],
        min_flow: float = 0.01
    ):
        """Plot side-by-side alluvial diagrams."""
        fig, axes = plt.subplots(1, len(transition_years), figsize=(25, 10))
        
        for idx, (start_year, end_year) in enumerate(transition_years):
            self._plot_alluvial_subplot(axes[idx], start_year, end_year, min_flow)
            if idx > 0:
                axes[idx].set_ylabel('')
        
        plt.tight_layout()
        output_path = self.output_dir / 'alluvial_comparison.png'
        plt.savefig(output_path, bbox_inches='tight', dpi=300)
        plt.close()
        
        logger.info(f"Saved alluvial comparison to {output_path}")
    
    def _plot_alluvial_subplot(self, ax, start_year, end_year, min_flow):
        """Create an alluvial plot on a specific subplot."""
        # Get land use for start and end years
        start_uses = self.df[str(start_year)].map(LAND_USE_CODES)
        end_uses = self.df[str(end_year)].map(LAND_USE_CODES)
        
        # Create transition counts with area weights
        transitions = pd.crosstab(
            start_uses,
            end_uses,
            values=self.df['area_m2'],
            aggfunc='sum'
        ) / 4046.86
        
        # Filter small flows
        min_area = transitions.sum().sum() * min_flow
        transitions = transitions.where(transitions >= min_area, 0)
        
        # Calculate positions for bars
        left_labels = transitions.index
        right_labels = transitions.columns
        
        # Plot left bars (start year)
        y_start = 0
        left_positions = {}
        for label in left_labels:
            height = transitions.loc[label].sum()
            if height > 0:
                ax.fill_between(
                    [0, 0.1],
                    [y_start, y_start],
                    [y_start + height, y_start + height],
                    color=self.colors[label],
                    alpha=0.8,
                    label=f"{label} ({start_year})"
                )
                left_positions[label] = y_start + height/2
                y_start += height
        
        # Plot right bars (end year)
        y_start = 0
        right_positions = {}
        for label in right_labels:
            height = transitions[label].sum()
            if height > 0:
                ax.fill_between(
                    [0.9, 1],
                    [y_start, y_start],
                    [y_start + height, y_start + height],
                    color=self.colors[label],
                    alpha=0.8,
                    label=f"{label} ({end_year})"
                )
                right_positions[label] = y_start + height/2
                y_start += height
        
        # Plot flows between bars
        for start_label in left_labels:
            for end_label in right_labels:
                flow = transitions.loc[start_label, end_label]
                if flow > 0:
                    # Create bezier curve points
                    x = np.array([0.1, 0.4, 0.6, 0.9])
                    y = np.array([
                        left_positions[start_label],
                        left_positions[start_label],
                        right_positions[end_label],
                        right_positions[end_label]
                    ])
                    
                    # Plot flow
                    ax.fill_between(
                        x,
                        y - flow/2,
                        y + flow/2,
                        alpha=0.3,
                        color=self.colors[start_label]
                    )
        
        # Customize plot
        ax.set_xlim(-0.1, 1.1)
        ax.set_title(f'Land Use Transitions {start_year} → {end_year}\n(flows ≥{min_flow*100:.1f}% shown)')
        ax.set_xticks([0, 1])
        ax.set_xticklabels([str(start_year), str(end_year)])
        ax.set_ylabel('Area (acres)')
        
        # Add legend
        handles, labels = ax.get_legend_handles_labels()
        by_label = dict(zip(labels, handles))
        ax.legend(
            by_label.values(),
            by_label.keys(),
            bbox_to_anchor=(1.05, 1),
            loc='upper left'
        )
        
        # Remove unnecessary spines
        ax.spines['top'].set_visible(False)
        ax.spines['right'].set_visible(False)
    
    def plot_temporal_trends_panels(self):
        """Plot temporal trends in a 2x2 panel, one land use type per panel."""
        # Calculate area by land use for each year
        areas = pd.DataFrame(index=LAND_USE_CODES.keys())
        for year in self.years:
            year_areas = self.df.groupby(str(year))['area_m2'].sum() / 4046.86
            areas[str(year)] = year_areas
        
        # Create subplots
        fig, axes = plt.subplots(2, 2, figsize=(15, 12))
        axes = axes.ravel()
        
        for idx, (code, label) in enumerate(LAND_USE_CODES.items()):
            ax = axes[idx]
            
            # Get data for this land use type
            data = areas.loc[code]
            
            # Plot trend
            ax.plot(
                self.years,
                [data[str(year)] for year in self.years],
                color=self.colors[label],
                linewidth=2
            )
            
            ax.set_title(label)
            ax.set_xlabel('Year')
            ax.set_ylabel('Area (acres)')
            ax.grid(True, alpha=0.3)
            
            # Add percentage change annotation
            start_area = data[str(min(self.years))]
            end_area = data[str(max(self.years))]
            pct_change = ((end_area - start_area) / start_area) * 100
            ax.text(
                0.05, 0.95,
                f'Change: {pct_change:+.1f}%',
                transform=ax.transAxes,
                verticalalignment='top'
            )
        
        plt.tight_layout()
        output_path = self.output_dir / 'temporal_trends_panels.png'
        plt.savefig(output_path, bbox_inches='tight', dpi=300)
        plt.close()
        
        logger.info(f"Saved temporal trends panels to {output_path}")
    
    def analyze_and_visualize(
        self,
        transition_years: Optional[List[Tuple[int, int]]] = None
    ):
        """Run all analyses and create visualizations."""
        try:
            # Default transition years if not provided
            if transition_years is None:
                transition_years = [(1985, 2000), (2000, 2023)]
            
            # Create all visualizations
            logger.info("Creating temporal trends panels...")
            self.plot_temporal_trends_panels()
            
            logger.info("Creating transition matrices comparison...")
            self.plot_transition_matrices(transition_years)
            
            logger.info("Creating alluvial comparison...")
            self.plot_alluvial_comparison(transition_years)
            
            logger.info("Analyzing land use trends...")
            self.analyze_landuse_trends()
            
            logger.info("Analysis complete!")
            
        except Exception as e:
            logger.error(f"Error during analysis: {str(e)}")
            raise

    def analyze_landuse_trends(self):
        """Analyze trends for each land use type in detail."""
        # Calculate area by year for all land uses
        areas = pd.DataFrame(index=LAND_USE_CODES.keys())
        for year in self.years:
            year_areas = self.df.groupby(str(year))['area_m2'].sum() / 4046.86
            areas[str(year)] = year_areas
        
        # Process each land use type
        all_stats = {}
        
        for code, label in LAND_USE_CODES.items():
            # Get data for this land use
            data = pd.Series([areas.loc[code, str(year)] for year in self.years], index=self.years)
            
            # Calculate year-over-year changes
            yoy_changes = data.pct_change() * 100
            
            # Calculate period statistics
            period1 = data[data.index <= 2000]
            period2 = data[data.index > 2000]
            
            stats = {
                "Period 1 (1985-2000)": {
                    "Mean Area": period1.mean(),
                    "Std Dev": period1.std(),
                    "Annual Change (%)": period1.pct_change().mean() * 100,
                    "Total Change (%)": ((period1.iloc[-1] - period1.iloc[0]) / period1.iloc[0]) * 100,
                    "Max Annual Change (%)": period1.pct_change().max() * 100,
                    "Min Annual Change (%)": period1.pct_change().min() * 100
                },
                "Period 2 (2001-2023)": {
                    "Mean Area": period2.mean(),
                    "Std Dev": period2.std(),
                    "Annual Change (%)": period2.pct_change().mean() * 100,
                    "Total Change (%)": ((period2.iloc[-1] - period2.iloc[0]) / period2.iloc[0]) * 100,
                    "Max Annual Change (%)": period2.pct_change().max() * 100,
                    "Min Annual Change (%)": period2.pct_change().min() * 100
                }
            }
            
            all_stats[label] = stats
            
            # Create detailed plot for this land use
            fig, (ax1, ax2) = plt.subplots(2, 1, figsize=(15, 12))
            
            # Plot 1: Area over time with period means and rate of change
            ax1.plot(data.index, data.values, color=self.colors[label], 
                    label=f"{label} Area", linewidth=2)
            
            # Secondary axis: Rate of change
            ax1_twin = ax1.twinx()
            # Calculate rolling average of year-over-year change
            rolling_change = data.pct_change().rolling(window=3, center=True).mean() * 100
            ax1_twin.plot(data.index, rolling_change, color='gray', linestyle='--', alpha=0.7,
                         label='Rate of Change (%/year)')
            
            # Add period means for area
            ax1.axhline(y=period1.mean(), color='gray', linestyle='--', 
                       xmin=0, xmax=(2000-1985)/(2023-1985),
                       label="Period 1 Mean")
            ax1.axhline(y=period2.mean(), color='gray', linestyle=':',
                       xmin=(2000-1985)/(2023-1985), xmax=1,
                       label="Period 2 Mean")
            
            # Add period separation
            ax1.axvline(x=2000, color='black', linestyle='-', alpha=0.2)
            
            ax1.set_title(f"{label} Area and Rate of Change Over Time")
            ax1.set_xlabel("Year")
            ax1.set_ylabel("Area (acres)")
            ax1_twin.set_ylabel("Rate of Change (%/year)")
            ax1.grid(True, alpha=0.3)
            
            # Combine legends from both axes
            lines1, labels1 = ax1.get_legend_handles_labels()
            lines2, labels2 = ax1_twin.get_legend_handles_labels()
            ax1.legend(lines1 + lines2, labels1 + labels2, loc='upper left')
            
            # Plot 2: Year-over-year changes
            ax2.bar(yoy_changes.index[yoy_changes.index <= 2000], 
                    yoy_changes[yoy_changes.index <= 2000],
                    color=self.colors[label], alpha=0.6)
            ax2.bar(yoy_changes.index[yoy_changes.index > 2000],
                    yoy_changes[yoy_changes.index > 2000],
                    color=self.colors[label], alpha=0.3)
            
            ax2.axhline(y=0, color='black', linestyle='-', alpha=0.2)
            ax2.axvline(x=2000, color='black', linestyle='-', alpha=0.2)
            
            # Add period means for changes
            ax2.axhline(y=period1.pct_change().mean() * 100, color='gray', linestyle='--',
                       xmin=0, xmax=(2000-1985)/(2023-1985),
                       label="Period 1 Mean Change")
            ax2.axhline(y=period2.pct_change().mean() * 100, color='gray', linestyle=':',
                       xmin=(2000-1985)/(2023-1985), xmax=1,
                       label="Period 2 Mean Change")
            
            ax2.set_title(f"Year-over-Year Changes in {label}")
            ax2.set_xlabel("Year")
            ax2.set_ylabel("Change (%)")
            ax2.grid(True, alpha=0.3)
            ax2.legend()
            
            plt.tight_layout()
            output_path = self.output_dir / f'{label.lower().replace("/", "_")}_analysis.png'
            plt.savefig(output_path, bbox_inches='tight', dpi=300)
            plt.close()
            
            logger.info(f"Saved {label} analysis to {output_path}")
        
        # Print statistics for all land uses
        logger.info("\nLand Use Statistics by Type:")
        for label, stats in all_stats.items():
            logger.info(f"\n{label}:")
            for period, metrics in stats.items():
                logger.info(f"\n  {period}:")
                for metric, value in metrics.items():
                    logger.info(f"    {metric}: {value:.2f}")
        
        return all_stats

def main():
    """Main execution function."""
    import argparse
    
    parser = argparse.ArgumentParser(
        description="Analyze and visualize land use changes"
    )
    parser.add_argument(
        "--input-file",
        required=True,
        help="Path to merged land use changes CSV"
    )
    parser.add_argument(
        "--output-dir",
        required=True,
        help="Directory for analysis outputs"
    )
    parser.add_argument(
        "--start-year",
        type=int,
        default=1985,
        help="Start year for analysis"
    )
    parser.add_argument(
        "--end-year",
        type=int,
        default=2023,
        help="End year for analysis"
    )
    parser.add_argument(
        "--transition-years",
        nargs='+',
        type=int,
        help="Years to analyze transitions between (must be even number of years)"
    )
    
    args = parser.parse_args()
    
    try:
        analyzer = LandUseChangeAnalyzer(
            data_path=args.input_file,
            output_dir=args.output_dir,
            start_year=args.start_year,
            end_year=args.end_year
        )
        
        # Process transition years if provided
        transition_years = None
        if args.transition_years:
            if len(args.transition_years) % 2 != 0:
                raise ValueError("Must provide pairs of years for transitions")
            transition_years = list(zip(
                args.transition_years[::2],
                args.transition_years[1::2]
            ))
        
        analyzer.analyze_and_visualize(transition_years)
        
    except Exception as e:
        logger.error(f"Error: {str(e)}")
        raise

if __name__ == "__main__":
    main() 