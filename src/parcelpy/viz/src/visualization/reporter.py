"""
Module for generating visualization reports.
"""

import asyncio
from datetime import datetime
from pathlib import Path
from typing import List, Dict, Any
from concurrent.futures import ThreadPoolExecutor

class ReportGenerator:
    def __init__(self, output_dir: str = 'reports'):
        """
        Initialize the report generator.
        
        Parameters:
        -----------
        output_dir : str
            Directory where reports will be saved
        """
        self.output_dir = Path(output_dir)
        self.output_dir.mkdir(parents=True, exist_ok=True)

    async def generate_report(
        self,
        analysis_results: List[Dict[str, Any]],
        plots_dir: str,
        data_info: Dict[str, str]
    ) -> str:
        """
        Generate a markdown report including plots and analysis.
        
        Parameters:
        -----------
        analysis_results : List[Dict[str, Any]]
            Results from parcel analysis
        plots_dir : str
            Directory containing plot images
        data_info : Dict[str, str]
            Information about input data sources
            
        Returns:
        --------
        str
            Path to the generated report
        """
        report_path = self.output_dir / 'parcel_visualization_report.md'
        
        report_lines = [
            "# Parcel Visualization Report",
            f"\nGenerated on: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}",
            "\n## Overview",
            "### Input Data Sources",
            "#### Parcel Data",
            f"- Source: {data_info.get('parcel_file', 'Unknown')}",
            "- Format: Parquet",
            "\n#### Land Use Data",
            f"- Source: LCMS ({data_info.get('region', 'Unknown')})",
            f"- Version: {data_info.get('version', 'Unknown')}",
            f"- Year: {data_info.get('year', 'Unknown')}",
            "- Format: GeoTIFF",
            "\n### Analysis Summary",
            f"- Number of parcels analyzed: {len(analysis_results)}",
            f"- Total area analyzed: {sum(r['acres'] for r in analysis_results):.1f} acres",
            "- Land use categories found:",
        ]
        
        # Add unique land use categories
        unique_categories = set()
        for result in analysis_results:
            unique_categories.update(result['land_use_counts'].keys())
        for category in sorted(unique_categories):
            report_lines.append(f"  - {category}")
        
        report_lines.append("\n## Sample Parcels Analysis\n")
        
        # Process parcel sections in parallel
        with ThreadPoolExecutor() as executor:
            tasks = []
            
            for result in analysis_results:
                task = asyncio.get_event_loop().run_in_executor(
                    executor,
                    self._generate_parcel_section,
                    result,
                    plots_dir
                )
                tasks.append(task)
            
            # Wait for all sections to complete
            parcel_sections = await asyncio.gather(*tasks)
            report_lines.extend([line for section in parcel_sections for line in section])
        
        # Write the report
        with open(report_path, 'w') as f:
            f.write('\n'.join(report_lines))
        
        return report_path

    def _generate_parcel_section(
        self,
        result: Dict[str, Any],
        plots_dir: str
    ) -> List[str]:
        """Generate report section for a single parcel."""
        section_lines = [
            f"### Parcel {result['parcel_id']}",
            f"- Area: {result['acres']:.2f} acres",
            f"- Location Bounds: ({result['bounds'].minx:.2f}, {result['bounds'].miny:.2f}, {result['bounds'].maxx:.2f}, {result['bounds'].maxy:.2f})",
            "\n#### Land Use Composition:",
            "| Category | Pixels | Percentage |",
            "|----------|---------|------------|"
        ]
        
        for category, stats in result['land_use_counts'].items():
            section_lines.append(
                f"| {category} | {stats['pixels']} | {stats['percentage']:.1f}% |"
            )
        
        # Add plot to the report with relative path to plots directory
        section_lines.extend([
            "\n#### Visualization:",
            f"![Parcel {result['parcel_id']} Land Use](../{plots_dir}/{result['plot_path']})\n"
        ])
        
        return section_lines 