#!/usr/bin/env python3
"""
Production version of land use change analysis script.
Optimized for large datasets with enhanced error handling, memory management,
and comprehensive analysis features.
"""

import pandas as pd
import numpy as np
import matplotlib.pyplot as plt
import seaborn as sns
import plotly.graph_objects as go
from pathlib import Path
import logging
from datetime import datetime
import json
from typing import Dict, List, Tuple, Optional, Union
import psutil
from tqdm import tqdm
import gc
import warnings
from concurrent.futures import ProcessPoolExecutor, as_completed
import multiprocessing
import argparse

# Configure warnings
warnings.filterwarnings('ignore', category=UserWarning)

# Set up logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

def log_memory_usage():
    """Log current memory usage."""
    process = psutil.Process()
    mem_info = process.memory_info()
    logger.info(f"Memory usage: {mem_info.rss / 1024 / 1024:.1f} MB")

# Land Use Classifications with metadata
LAND_USE_METADATA = {
    1: {
        "name": "Agriculture",
        "color": "#ffd700",
        "description": "Agricultural land including cropland",
        "min_area_threshold": 900  # Minimum area in m² for reliable classification
    },
    2: {
        "name": "Developed",
        "color": "#ff4444",
        "description": "Urban and developed areas",
        "min_area_threshold": 900
    },
    3: {
        "name": "Forest",
        "color": "#228b22",
        "description": "Forest and woodland areas",
        "min_area_threshold": 900
    },
    6: {
        "name": "Pasture",
        "color": "#deb887",
        "description": "Rangeland and pasture areas",
        "min_area_threshold": 900
    }
}

# Extract commonly used mappings
LAND_USE_CODES = {k: v["name"] for k, v in LAND_USE_METADATA.items()}
LAND_USE_COLORS = {v["name"]: v["color"] for k, v in LAND_USE_METADATA.items()}

def process_land_use(args):
    """Process a single land use type (for parallel execution)."""
    code, label, df, years = args
    
    # Calculate total area once (constant across years)
    total_area_acres = df['area_m2'].sum() / 4046.86  # Convert to acres
    
    # Calculate areas for all years
    areas = []
    for year in years:
        # Calculate area for this land use type
        mask = df[str(year)] == code
        area = df.loc[mask, 'area_m2'].sum() / 4046.86  # Convert to acres
        areas.append(area)
    
    data = pd.Series(areas, index=years)
    
    # Calculate absolute changes
    absolute_changes = data.diff()  # Get year-over-year changes in acres
    mean_annual_change_acres = absolute_changes.mean()  # Average change in acres per year
    
    # Calculate detailed statistics
    stats = {
        "current_area": data.iloc[-1],
        "peak_area": data.max(),
        "peak_year": years[data.argmax()],
        "total_change_acres": data.iloc[-1] - data.iloc[0],  # Total change in acres
        "total_change_pct": ((data.iloc[-1] - data.iloc[0]) / data.iloc[0]) * 100 if data.iloc[0] != 0 else 0,
        "mean_annual_change_acres": mean_annual_change_acres,  # New metric
        "mean_annual_change_pct": data.pct_change().mean() * 100,
        "volatility_acres": absolute_changes.std(),  # Standard deviation of changes in acres
        "volatility_pct": data.pct_change().std() * 100,
        "trend_acres_per_year": np.polyfit(range(len(years)), data.values, 1)[0],  # Linear trend in acres/year
        "periodic_changes": {
            "1985-2000": {
                "total_acres": data[2000] - data[1985],
                "acres_per_year": (data[2000] - data[1985]) / 15,  # Average change per year in acres
                "percent": ((data[2000] - data[1985]) / data[1985]) * 100 if data[1985] != 0 else 0
            },
            "2001-2023": {
                "total_acres": data[2023] - data[2001],
                "acres_per_year": (data[2023] - data[2001]) / 22,  # Average change per year in acres
                "percent": ((data[2023] - data[2001]) / data[2001]) * 100 if data[2001] != 0 else 0
            }
        }
    }
    
    return label, stats

class LandUseChangeAnalyzerProd:
    """Production analyzer for land use changes with optimizations."""
    
    def __init__(
        self,
        data_path: Union[str, Path],
        output_dir: Union[str, Path],
        start_year: int = 1985,
        end_year: int = 2023,
        chunk_size: int = 50000,
        n_workers: Optional[int] = None
    ):
        """Initialize the analyzer with enhanced configuration."""
        self.data_path = Path(data_path)
        self.output_dir = Path(output_dir)
        
        # Extract county name from input path
        county_path = Path(data_path).parent.name
        self.county_name = county_path.split('_')[1].replace('_', ' ') if '_' in county_path else 'Unknown'
        
        self.years = list(range(start_year, end_year + 1))
        self.chunk_size = chunk_size
        self.n_workers = n_workers or max(1, multiprocessing.cpu_count() - 1)
        
        # Create output directories
        self.output_dir.mkdir(parents=True, exist_ok=True)
        for subdir in ['plots', 'data', 'reports']:
            (self.output_dir / subdir).mkdir(exist_ok=True)
        
        # Initialize data storage
        self.df = None
        self.stats_cache = {}
        
        # Set up logging for this instance
        self.log_file = self.output_dir / 'reports' / f'analysis_{datetime.now():%Y%m%d_%H%M%S}.log'
        self.setup_logging()
        
        logger.info(f"Initializing analysis for {self.county_name}, years {start_year}-{end_year}")
        logger.info(f"Using {self.n_workers} worker processes")
        log_memory_usage()
        
        # Configure plotting
        plt.style.use('default')
        self.colors = LAND_USE_COLORS
        
        # Load and validate data
        self._load_and_preprocess_data()

    def setup_logging(self):
        """Configure logging with file rotation."""
        file_handler = logging.FileHandler(self.log_file)
        file_handler.setFormatter(
            logging.Formatter('%(asctime)s - %(levelname)s - %(message)s')
        )
        logger.addHandler(file_handler)
        
    def _load_and_preprocess_data(self) -> None:
        """Load and preprocess data with chunking and optimization."""
        logger.info(f"Loading data from {self.data_path}")
        
        # Initialize empty list for chunks
        chunks = []
        total_rows = 0
        
        # Read data in chunks with initial string types for potentially problematic columns
        chunk_iterator = pd.read_csv(
            self.data_path,
            chunksize=self.chunk_size,
            dtype={
                'PRCL_NBR': str,
                'area_m2': str,  # Read as string first to handle potential formatting issues
                'is_sub_resolution': str,  # Read as string to handle 'True'/'False' values
                **{str(year): 'int8' for year in self.years}
            }
        )
        
        for chunk in tqdm(chunk_iterator, desc="Loading data chunks"):
            # Convert area_m2 to float32 with error handling
            try:
                chunk['area_m2'] = pd.to_numeric(chunk['area_m2'], errors='coerce').astype('float32')
            except Exception as e:
                logger.warning(f"Error converting area_m2: {e}. Attempting cleanup...")
                # Try to clean the string and convert again
                chunk['area_m2'] = chunk['area_m2'].str.replace(',', '').str.strip()
                chunk['area_m2'] = pd.to_numeric(chunk['area_m2'], errors='coerce').astype('float32')
            
            # Convert is_sub_resolution to boolean with flexible handling
            try:
                # Handle various string formats
                chunk['is_sub_resolution'] = chunk['is_sub_resolution'].map({
                    'True': True, 'true': True, 'TRUE': True, '1': True, 1: True,
                    'False': False, 'false': False, 'FALSE': False, '0': False, 0: False
                }).fillna(False)  # Default to False for any unmatched values
            except Exception as e:
                logger.warning(f"Error converting is_sub_resolution: {e}. Setting to False.")
                chunk['is_sub_resolution'] = False
            
            # Process year columns
            year_cols = [str(year) for year in self.years]
            for year in year_cols:
                # Ensure numeric type and handle NaN values
                chunk[year] = pd.to_numeric(chunk[year], errors='coerce').fillna(0).astype('int8')
                # Combine Forest and Wetland (codes 3 and 4)
                chunk.loc[chunk[year] == 4, year] = 3
            
            chunks.append(chunk)
            total_rows += len(chunk)
            
            if total_rows % 100000 == 0:
                logger.info(f"Processed {total_rows:,} rows")
                log_memory_usage()
        
        # Combine chunks and optimize memory
        self.df = pd.concat(chunks, ignore_index=True)
        
        # Validate data
        logger.info("Validating data...")
        invalid_area = (self.df['area_m2'] <= 0) | self.df['area_m2'].isna()
        if invalid_area.any():
            logger.warning(f"Found {invalid_area.sum()} rows with invalid area values. Setting to minimum valid area.")
            self.df.loc[invalid_area, 'area_m2'] = 1.0  # Set to 1 square meter as minimum
        
        # Clean up
        del chunks
        gc.collect()
        
        logger.info(f"Loaded {len(self.df):,} rows, total area: {self.df['area_m2'].sum() / 4046.86:.2f} acres")
        log_memory_usage()
    
    def create_transition_matrix(
        self,
        start_year: int,
        end_year: int,
        weighted: bool = True
    ) -> pd.DataFrame:
        """Create a transition matrix between two years."""
        # Get land use codes for start and end years
        start_codes = self.df[str(start_year)]
        end_codes = self.df[str(end_year)]
        
        # Create transition matrix
        if weighted:
            # Calculate transitions weighted by area
            transitions = pd.crosstab(
                start_codes.map(LAND_USE_CODES),
                end_codes.map(LAND_USE_CODES),
                values=self.df['area_m2'],
                aggfunc='sum'
            ) / 4046.86  # Convert to acres
        else:
            # Calculate transitions based on parcel counts
            transitions = pd.crosstab(
                start_codes.map(LAND_USE_CODES),
                end_codes.map(LAND_USE_CODES),
                normalize='index'
            ) * 100
        
        # Ensure all land use categories are present
        all_categories = list(LAND_USE_CODES.values())
        for cat in all_categories:
            if cat not in transitions.index:
                transitions.loc[cat] = 0
            if cat not in transitions.columns:
                transitions[cat] = 0
        
        # Sort index and columns to match the original order
        transitions = transitions.reindex(index=all_categories, columns=all_categories, fill_value=0)
        
        return transitions
    
    def plot_transition_matrices(
        self,
        transition_years: List[Tuple[int, int]],
        min_value: float = 1.0,
        weighted: bool = True
    ):
        """Plot transition matrices with enhanced visualization."""
        fig, axes = plt.subplots(1, len(transition_years), figsize=(20, 8))
        if len(transition_years) == 1:
            axes = [axes]
        
        for idx, (start_year, end_year) in enumerate(transition_years):
            # Get transition matrix
            transitions = self.create_transition_matrix(
                start_year, end_year, weighted=weighted
            )
            
            # Create mask for values below threshold
            mask = transitions < min_value if weighted else transitions < 0.1
            
            # Create heatmap with explicit value display
            heatmap = sns.heatmap(
                transitions,
                annot=True,  # Show values in cells
                fmt=',.0f' if weighted else '.1f',  # Format for numbers
                cmap='Blues',
                vmin=0,
                ax=axes[idx],
                square=True,
                cbar_kws={'label': 'Acres' if weighted else 'Percentage'},
                annot_kws={'size': 10},  # Make annotations more visible
                mask=mask  # Mask small values
            )
            
            # Manually add annotations
            for i in range(len(transitions.index)):
                for j in range(len(transitions.columns)):
                    value = transitions.iloc[i, j]
                    if not mask.iloc[i, j]:  # Only annotate non-masked values
                        text = f'{value:,.0f}' if weighted else f'{value:.1f}'
                        heatmap.text(
                            j + 0.5,  # Center in cell
                            i + 0.5,
                            text,
                            ha='center',
                            va='center',
                            fontsize=10
                        )
            
            # Customize plot
            title = f'Land Use Transitions {start_year} → {end_year}\n'
            title += '(acres)' if weighted else '(% of starting land use)'
            axes[idx].set_title(title, pad=20)
            
            # Rotate labels for better readability
            axes[idx].set_xticklabels(
                axes[idx].get_xticklabels(),
                rotation=45,
                ha='right',
                fontsize=10
            )
            axes[idx].set_yticklabels(
                axes[idx].get_yticklabels(),
                rotation=0,
                fontsize=10
            )
            
            # Add labels
            if idx == 0:
                axes[idx].set_ylabel('Starting Land Use', fontsize=12)
            axes[idx].set_xlabel('Ending Land Use', fontsize=12)
            
            # Force drawing of annotations
            fig.canvas.draw()
        
        # Adjust layout to prevent text cutoff
        plt.tight_layout()
        
        # Save with extra padding
        output_path = self.output_dir / 'plots' / 'transition_matrices_comparison.png'
        plt.savefig(output_path, bbox_inches='tight', dpi=300, pad_inches=0.5)
        plt.close()
        
        logger.info(f"Saved transition matrices comparison to {output_path}")
    
    def plot_temporal_trends(self):
        """Plot temporal trends with enhanced visualization and statistics."""
        # Calculate area by land use for each year
        areas = pd.DataFrame(index=LAND_USE_CODES.keys())
        for year in self.years:
            year_areas = self.df.groupby(str(year))['area_m2'].sum() / 4046.86
            areas[str(year)] = year_areas
        
        # Create individual plots for each land use type
        for code, label in LAND_USE_CODES.items():
            # Create figure and primary axis with extra space on right for legend
            fig = plt.figure(figsize=(14, 8))
            
            # Create gridspec to manage layout
            gs = plt.GridSpec(1, 2, width_ratios=[4, 1], figure=fig)
            
            # Create main axis for plot and legend axis for text
            ax = fig.add_subplot(gs[0])
            legend_ax = fig.add_subplot(gs[1])
            legend_ax.axis('off')  # Hide the legend axis
            
            # Get data for this land use type
            data = pd.Series(
                [areas.loc[code, str(year)] for year in self.years],
                index=self.years
            )
            
            # Calculate absolute changes
            absolute_changes = data.diff()  # Get year-over-year changes in acres
            
            # Calculate statistics
            total_change_acres = data.iloc[-1] - data.iloc[0]
            mean_annual_change_acres = absolute_changes.mean()
            volatility_acres = absolute_changes.std()
            
            # Plot 1: Area over time with absolute rate of change
            line1 = ax.plot(self.years, data.values, color=self.colors[label], 
                    label=f"{label} Area", linewidth=2.5)
            
            # Secondary axis: Absolute rate of change in acres
            ax_twin = ax.twinx()
            # Calculate rolling average of year-over-year absolute changes
            rolling_change = absolute_changes.rolling(window=3, center=True).mean()
            line2 = ax_twin.plot(data.index, rolling_change, color='gray', linestyle='dotted', alpha=0.7,
                         label='Rate of Change (acres/year)', linewidth=1.5)
            
            # Add period means for area
            period1 = data[data.index <= 2000]
            period2 = data[data.index > 2000]
            line3 = ax.axhline(y=period1.mean(), color='darkgray', linestyle='solid', 
                       xmin=0, xmax=(2000-1985)/(2023-1985),
                       label="Period 1 Mean", linewidth=1.5)
            line4 = ax.axhline(y=period2.mean(), color='darkgray', linestyle='dashed',
                       xmin=(2000-1985)/(2023-1985), xmax=1,
                       label="Period 2 Mean", linewidth=1.5)
            
            # Add period separation
            ax.axvline(x=2000, color='black', linestyle='-', alpha=0.2)
            
            # Add statistics text to the legend axis
            stats_text = (
                f'Summary Statistics:\n'
                f'Total Change: {total_change_acres:+,.0f} acres\n'
                f'Mean Annual Change: {mean_annual_change_acres:+,.1f} acres/year\n'
                f'Volatility: {volatility_acres:.1f} acres (std. dev. of annual changes)\n\n'
                f'Period Changes:\n'
                f'1985-2000:\n'
                f'  * Total change: {period1.iloc[-1] - period1.iloc[0]:+,.0f} acres\n'
                f'  * Rate: {(period1.iloc[-1] - period1.iloc[0])/15:+,.1f} acres/year\n'
                f'2001-2023:\n'
                f'  * Total change: {period2.iloc[-1] - period2.iloc[0]:+,.0f} acres\n'
                f'  * Rate: {(period2.iloc[-1] - period2.iloc[0])/22:+,.1f} acres/year\n'
            )
            
            # First add the legend at the top of the right column
            lines = line1 + line2 + [line3, line4]
            labels = [l.get_label() for l in lines]
            legend = legend_ax.legend(lines, labels, 
                                    loc='upper center',
                                    bbox_to_anchor=(0.5, 1.0),
                                    fontsize=10)
            
            # Get the bottom coordinate of the legend
            legend_bbox = legend.get_window_extent(fig.canvas.get_renderer())
            legend_bottom = legend_bbox.transformed(legend_ax.transAxes.inverted()).y0
            
            # Add stats text below the legend
            legend_ax.text(0.05, legend_bottom - 0.1, stats_text,
                         transform=legend_ax.transAxes,
                         verticalalignment='top',
                         bbox=dict(facecolor='white', alpha=0.8, edgecolor='gray', 
                                 boxstyle='round,pad=0.5'),
                         fontsize=10)
            
            # Customize axes with county name
            ax.set_title(f'{self.county_name} {label} Area and Rate of Change Over Time', pad=20, fontsize=12)
            ax.set_xlabel('Year', fontsize=10)
            ax.set_ylabel('Area (acres)', fontsize=10)
            ax_twin.set_ylabel('Rate of Change (acres/year)', fontsize=10)
            
            # Add grid to primary axis
            ax.grid(True, alpha=0.3, which='both')
            
            # Adjust layout
            plt.tight_layout()
            
            # Save individual plot
            output_path = self.output_dir / 'plots' / f'temporal_trends_{label.lower()}.png'
            plt.savefig(output_path, bbox_inches='tight', dpi=300)
            plt.close()
            
            logger.info(f"Saved {label} temporal trends to {output_path}")
    
    def plot_sankey_diagram(
        self,
        transition_years: List[Tuple[int, int]],
        min_flow: float = 0.01
    ):
        """Create Sankey diagrams for land use transitions."""
        for start_year, end_year in transition_years:
            # Get land use for start and end years
            start_uses = self.df[str(start_year)].map(LAND_USE_CODES)
            end_uses = self.df[str(end_year)].map(LAND_USE_CODES)
            
            # Create transition counts with area weights
            transitions = pd.crosstab(
                start_uses,
                end_uses,
                values=self.df['area_m2'],
                aggfunc='sum'
            ) / 4046.86  # Convert to acres
            
            # Filter small flows
            min_area = transitions.sum().sum() * min_flow
            transitions = transitions.where(transitions >= min_area, 0)
            
            # Create node lists and color mapping
            unique_labels = sorted(set(transitions.index) | set(transitions.columns))
            label_to_idx = {label: i for i, label in enumerate(unique_labels)}
            
            # Create source, target, and value lists for Sankey
            sources = []
            targets = []
            values = []
            
            for start_label in transitions.index:
                for end_label in transitions.columns:
                    flow = transitions.loc[start_label, end_label]
                    if flow > 0:
                        sources.append(label_to_idx[start_label])
                        targets.append(label_to_idx[end_label] + len(unique_labels))
                        values.append(flow)
            
            # Create node labels with years
            node_labels = [f"{label} ({start_year})" for label in unique_labels] + \
                         [f"{label} ({end_year})" for label in unique_labels]
            
            # Create node colors
            node_colors = [self.colors[label] for label in unique_labels] * 2
            
            # Create Sankey diagram
            fig = go.Figure(data=[go.Sankey(
                node=dict(
                    pad=15,
                    thickness=20,
                    line=dict(color="black", width=0.5),
                    label=node_labels,
                    color=node_colors
                ),
                link=dict(
                    source=sources,
                    target=targets,
                    value=values,
                    color=[f"rgba{tuple(int(c.lstrip('#')[i:i+2], 16) for i in (0, 2, 4)) + (0.4,)}" 
                          for c in [self.colors[unique_labels[s]] for s in sources]]
                )
            )])
            
            # Update layout with county name
            fig.update_layout(
                title=dict(
                    text=f'{self.county_name} Land Use Transitions {start_year} → {end_year}<br>(flows ≥{min_flow*100:.1f}% shown)',
                    x=0.5,
                    y=0.95,
                    xanchor='center',
                    yanchor='top'
                ),
                font=dict(size=12),
                width=1200,
                height=800
            )
            
            # Save as HTML for interactivity
            output_path_html = self.output_dir / 'plots' / f'sankey_diagram_{start_year}_{end_year}.html'
            fig.write_html(str(output_path_html))
            
            # Save as PNG for static version
            output_path_png = self.output_dir / 'plots' / f'sankey_diagram_{start_year}_{end_year}.png'
            fig.write_image(str(output_path_png))
            
            logger.info(f"Saved Sankey diagram to {output_path_html} and {output_path_png}")

    def plot_forest_loss_sankey(
        self,
        transition_years: List[Tuple[int, int]],
        min_flow: float = 0.01
    ):
        """Create Sankey diagrams showing conversion of forest land to other uses."""
        for start_year, end_year in transition_years:
            # Get land use for start and end years
            start_uses = self.df[str(start_year)].map(LAND_USE_CODES)
            end_uses = self.df[str(end_year)].map(LAND_USE_CODES)
            
            # Create transition counts with area weights
            transitions = pd.crosstab(
                start_uses,
                end_uses,
                values=self.df['area_m2'],
                aggfunc='sum'
            ) / 4046.86  # Convert to acres
            
            # Filter to only show forest conversions to other uses
            forest_transitions = transitions.loc['Forest'].copy()
            forest_transitions['Forest'] = 0  # Remove forest-to-forest transition
            forest_transitions = forest_transitions[forest_transitions > 0]
            
            if forest_transitions.empty:
                continue
            
            # Create node lists
            target_labels = forest_transitions.index
            
            # Create source, target, and value lists for Sankey
            sources = [0] * len(target_labels)  # Forest is always source 0
            targets = list(range(1, len(target_labels) + 1))
            values = forest_transitions.values
            
            # Create node labels
            node_labels = [f"Forest ({start_year})"] + [f"{label} ({end_year})" for label in target_labels]
            node_colors = [self.colors["Forest"]] + [self.colors[label] for label in target_labels]
            
            # Calculate total forest loss
            total_loss = values.sum()
            
            # Create Sankey diagram
            fig = go.Figure(data=[go.Sankey(
                node=dict(
                    pad=15,
                    thickness=20,
                    line=dict(color="black", width=0.5),
                    label=node_labels,
                    color=node_colors
                ),
                link=dict(
                    source=sources,
                    target=targets,
                    value=values,
                    color=[f"rgba{tuple(int(self.colors['Forest'].lstrip('#')[i:i+2], 16) for i in (0, 2, 4)) + (0.4,)}"] * len(values)
                )
            )])
            
            # Update layout with county name
            fig.update_layout(
                title=dict(
                    text=f'{self.county_name} Forest Land Conversion {start_year} → {end_year}<br>Total Forest Loss: {total_loss:,.0f} acres',
                    x=0.5,
                    y=0.95,
                    xanchor='center',
                    yanchor='top'
                ),
                font=dict(size=12),
                width=1000,
                height=600
            )
            
            # Save as HTML for interactivity
            output_path_html = self.output_dir / 'plots' / f'forest_loss_sankey_{start_year}_{end_year}.html'
            fig.write_html(str(output_path_html))
            
            # Save as PNG for static version
            output_path_png = self.output_dir / 'plots' / f'forest_loss_sankey_{start_year}_{end_year}.png'
            fig.write_image(str(output_path_png))

    def plot_development_gains_sankey(
        self,
        transition_years: List[Tuple[int, int]],
        min_flow: float = 0.01
    ):
        """Create Sankey diagrams showing sources of newly developed land."""
        for start_year, end_year in transition_years:
            # Get land use for start and end years
            start_uses = self.df[str(start_year)].map(LAND_USE_CODES)
            end_uses = self.df[str(end_year)].map(LAND_USE_CODES)
            
            # Create transition counts with area weights
            transitions = pd.crosstab(
                start_uses,
                end_uses,
                values=self.df['area_m2'],
                aggfunc='sum'
            ) / 4046.86  # Convert to acres
            
            # Filter to only show transitions to developed land
            development_transitions = transitions['Developed'].copy()
            development_transitions['Developed'] = 0  # Remove developed-to-developed
            development_transitions = development_transitions[development_transitions > 0]
            
            if development_transitions.empty:
                continue
            
            # Create node lists
            source_labels = development_transitions.index
            
            # Create source, target, and value lists for Sankey
            sources = list(range(len(source_labels)))
            targets = [len(source_labels)] * len(source_labels)  # Developed is always the last target
            values = development_transitions.values
            
            # Create node labels
            node_labels = [f"{label} ({start_year})" for label in source_labels] + [f"Developed ({end_year})"]
            node_colors = [self.colors[label] for label in source_labels] + [self.colors["Developed"]]
            
            # Calculate total development gain
            total_gain = values.sum()
            
            # Create Sankey diagram
            fig = go.Figure(data=[go.Sankey(
                node=dict(
                    pad=15,
                    thickness=20,
                    line=dict(color="black", width=0.5),
                    label=node_labels,
                    color=node_colors
                ),
                link=dict(
                    source=sources,
                    target=targets,
                    value=values,
                    color=[f"rgba{tuple(int(self.colors[label].lstrip('#')[i:i+2], 16) for i in (0, 2, 4)) + (0.4,)}" 
                          for label in source_labels]
                )
            )])
            
            # Update layout with county name
            fig.update_layout(
                title=dict(
                    text=f'{self.county_name} New Development Sources {start_year} → {end_year}<br>Total New Development: {total_gain:,.0f} acres',
                    x=0.5,
                    y=0.95,
                    xanchor='center',
                    yanchor='top'
                ),
                font=dict(size=12),
                width=1000,
                height=600
            )
            
            # Save as HTML for interactivity
            output_path_html = self.output_dir / 'plots' / f'development_gains_sankey_{start_year}_{end_year}.html'
            fig.write_html(str(output_path_html))
            
            # Save as PNG for static version
            output_path_png = self.output_dir / 'plots' / f'development_gains_sankey_{start_year}_{end_year}.png'
            fig.write_image(str(output_path_png))

    def analyze_and_visualize(
        self,
        transition_years: Optional[List[Tuple[int, int]]] = None
    ):
        """Run complete analysis with enhanced error handling."""
        try:
            # Set default transition years if not provided
            if transition_years is None:
                transition_years = [(1985, 2000), (2000, 2023)]
            
            # Create visualizations
            self.plot_temporal_trends()
            self.plot_transition_matrices(transition_years, weighted=True)
            self.plot_transition_matrices(transition_years, weighted=False)
            self.plot_forest_loss_sankey(transition_years)
            self.plot_development_gains_sankey(transition_years)
            
            # Analyze trends
            trends = self.analyze_landuse_trends()
            
            return trends
            
        except Exception as e:
            logger.error(f"Error during analysis: {str(e)}")
            raise

    def analyze_landuse_trends(self) -> Dict:
        """Analyze trends with enhanced statistics and parallel processing."""
        # Process land uses in parallel
        with ProcessPoolExecutor(max_workers=self.n_workers) as executor:
            futures = [
                executor.submit(
                    process_land_use,
                    (code, label, self.df, self.years)
                )
                for code, label in LAND_USE_CODES.items()
            ]
            
            results = {}
            for future in as_completed(futures):
                label, stats = future.result()
                results[label] = stats
        
        # Save results
        output_path = self.output_dir / 'data' / 'land_use_trends.json'
        with open(output_path, 'w') as f:
            json.dump(results, f, indent=2)
        
        # Generate summary report
        self._generate_trend_report(results)
        
        return results
    
    def _generate_trend_report(self, trends: Dict):
        """Generate a detailed markdown report of trends with embedded figures."""
        report = [
            f"# {self.county_name} Land Use Change Analysis Report\n",
            f"*Generated on: {datetime.now():%Y-%m-%d %H:%M:%S}*\n",
            "## Summary Statistics\n"
        ]
        
        # Add overall statistics
        total_area = sum(trend["current_area"] for trend in trends.values())
        report.extend([
            f"Total analyzed area: {total_area:,.0f} acres\n",
            "## Land Use Type Analysis\n"
        ])
        
        # Add detailed analysis for each land use type with corresponding figures
        for label, stats in trends.items():
            report.extend([
                f"### {label}\n",
                f"![{label} Temporal Trends](../plots/temporal_trends_{label.lower()}.png)\n",
                f"- Current area: {stats['current_area']:,.0f} acres",
                f"- Peak area: {stats['peak_area']:,.0f} acres ({stats['peak_year']})",
                f"- Total change: {stats['total_change_acres']:+,.0f} acres ({stats['total_change_pct']:+.1f}%)",
                f"- Mean annual change: {stats['mean_annual_change_acres']:+,.1f} acres/year",
                f"- Volatility: {stats['volatility_acres']:.1f} acres (std. dev. of annual changes)",
                "\nPeriod Changes:",
                f"- 1985-2000:",
                f"  * Total change: {stats['periodic_changes']['1985-2000']['total_acres']:+,.0f} acres",
                f"  * Rate: {stats['periodic_changes']['1985-2000']['acres_per_year']:+,.1f} acres/year",
                f"- 2001-2023:",
                f"  * Total change: {stats['periodic_changes']['2001-2023']['total_acres']:+,.0f} acres",
                f"  * Rate: {stats['periodic_changes']['2001-2023']['acres_per_year']:+,.1f} acres/year\n"
            ])
        
        # Add transition analysis section with matrices
        report.extend([
            "## Land Use Transitions\n",
            "### Transition Matrices\n",
            "The following matrices show the area (in acres) that transitioned between different land use types during each period:\n",
            "![Transition Matrices](../plots/transition_matrices_comparison.png)\n",
            "\nReading the matrices:\n",
            "- Each row represents the starting land use\n",
            "- Each column represents the ending land use\n",
            "- Values show the number of acres that transitioned from one use to another\n",
            "- Diagonal values represent land that maintained the same use\n",
            "\n### Sankey Diagrams\n",
            "#### Forest Loss Analysis\n",
            "The following diagrams show the conversion of forest land to other uses:\n",
            "![Forest Loss 1985-2000](../plots/forest_loss_sankey_1985_2000.png)\n",
            "![Forest Loss 2000-2023](../plots/forest_loss_sankey_2000_2023.png)\n",
            "\n#### Development Gains Analysis\n",
            "The following diagrams show the sources of newly developed land:\n",
            "![Development Gains 1985-2000](../plots/development_gains_sankey_1985_2000.png)\n",
            "![Development Gains 2000-2023](../plots/development_gains_sankey_2000_2023.png)\n",
            "\n### Interactive Visualizations\n",
            "Interactive versions of the Sankey diagrams are available as HTML files in the `plots` directory:\n",
            "- `forest_loss_sankey_1985_2000.html`\n",
            "- `forest_loss_sankey_2000_2023.html`\n",
            "- `development_gains_sankey_1985_2000.html`\n",
            "- `development_gains_sankey_2000_2023.html`\n"
        ])
        
        # Save report
        report_path = self.output_dir / 'reports' / 'trend_analysis.md'
        with open(report_path, 'w') as f:
            f.write('\n'.join(report))
        
        logger.info(f"Saved trend analysis report with figures to {report_path}")

def main():
    """Main execution function with enhanced CLI."""
    parser = argparse.ArgumentParser(
        description="Production Land Use Change Analyzer"
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
        "--chunk-size",
        type=int,
        default=50000,
        help="Number of rows to process at once"
    )
    parser.add_argument(
        "--workers",
        type=int,
        help="Number of worker processes (default: CPU count - 1)"
    )
    parser.add_argument(
        "--transition-years",
        nargs='+',
        type=int,
        help="Years to analyze transitions between (must be even number of years)"
    )
    
    args = parser.parse_args()
    
    try:
        analyzer = LandUseChangeAnalyzerProd(
            data_path=args.input_file,
            output_dir=args.output_dir,
            start_year=args.start_year,
            end_year=args.end_year,
            chunk_size=args.chunk_size,
            n_workers=args.workers
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
        logger.info("Analysis complete!")
        
    except Exception as e:
        logger.error(f"Error: {str(e)}")
        raise

if __name__ == "__main__":
    main() 