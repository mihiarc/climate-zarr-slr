#!/usr/bin/env python
"""Simple region-based strategy selection for climate data processing."""

from typing import Any
import geopandas as gpd
from rich.console import Console

from .processing_strategies import VectorizedStrategy, SpatialChunkedStrategy

console = Console()


def get_strategy_for_region(region: str, gdf: gpd.GeoDataFrame, n_workers: int = 4) -> Any:
    """Select processing strategy based on region.
    
    Simple logic:
    - CONUS: Use SpatialChunkedStrategy (most counties, ~3100)
    - All others: Use VectorizedStrategy (fewer counties, <100 each)
    
    Args:
        region: Region identifier (e.g., 'conus', 'alaska', 'hawaii')
        gdf: County geodataframe (for logging county count)
        n_workers: Number of workers for chunked strategy
        
    Returns:
        Processing strategy instance
    """
    num_counties = len(gdf)
    
    if region.lower() == 'conus':
        console.print(f"[green]Using SpatialChunkedStrategy for CONUS ({num_counties} counties)[/green]")
        console.print("[cyan]Benefits: Parallel processing, spatial optimization, memory efficiency[/cyan]")
        
        return SpatialChunkedStrategy(
            target_memory_usage=0.75,
            min_chunk_size=10,
            max_chunk_size=50,
            enable_spatial_cache=True
        )
    else:
        console.print(f"[green]Using VectorizedStrategy for {region.upper()} ({num_counties} counties)[/green]")
        console.print("[cyan]Benefits: Simple processing, predictable memory, precise geometry handling[/cyan]")
        
        return VectorizedStrategy()


def infer_region_from_gdf(gdf: gpd.GeoDataFrame) -> str:
    """Infer region from geodataframe based on county count.
    
    Fallback logic for cases where region isn't explicitly provided.
    
    Args:
        gdf: County geodataframe
        
    Returns:
        Inferred region name
    """
    num_counties = len(gdf)
    
    if num_counties > 2000:
        return 'conus'  # Only CONUS has this many counties
    elif num_counties > 50:
        return 'puerto_rico'  # PR has ~78 counties  
    elif num_counties > 20:
        return 'alaska'  # Alaska has ~30 boroughs/census areas
    elif num_counties > 10:
        return 'hawaii'  # Hawaii has ~5 counties but with subdivisions
    else:
        return 'guam'  # Small territories