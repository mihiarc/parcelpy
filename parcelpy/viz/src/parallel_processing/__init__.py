"""
Parallel processing module for parcel analysis.
Handles multiprocessing coordination and task distribution.
"""

from .processor import ParallelProcessor
from .utils import create_chunks, setup_progress_bar

__all__ = [
    'ParallelProcessor',
    'create_chunks',
    'setup_progress_bar'
] 