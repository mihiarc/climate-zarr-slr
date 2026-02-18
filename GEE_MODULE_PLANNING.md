# Google Earth Engine Module Planning Document

**Date**: February 18, 2026  
**Project**: climate-zarr-slr  
**Objective**: Add Google Earth Engine as an alternative data source to replace local NetCDF downloads in the climate data pipeline

---

## 1. Executive Summary

This module will enable the climate-zarr-slr pipeline to fetch **NASA CMIP6 climate data** directly from Google Earth Engine (GEE) instead of downloading NetCDF files locally. The GEE integration will work with the same NASA NEX-GDDP-CMIP6 models available via HTTP download (NorESM2-LM, ACCESS-CM2, CanESM5, etc.), but delivered through GEE's cloud infrastructure.

**Single Data Source Strategy:**
- **Unified data**: NASA CMIP6 only (via GEE's NEX-GDDP-CMIP6 collection)
- **Model consistency**: Same models as your current downloads (ACCESS-CM2, CanESM5, etc.)
- **Variable support**: pr, tas, tasmax, tasmin across all scenarios
- **Zero adaptation**: No need to learn multiple data providers

**Benefits:**
- Reduced storage: No need to download/store 100GB+ of raw NetCDF files
- Faster processing: Cloud processing on GEE backend
- Scalability: Handle larger regions and time ranges
- Reproducibility: Version-controlled CMIP6 data source
- Cost efficiency: Pay only for computation, not storage

---

## 2. Architecture Overview

### Current Pipeline Flow
```
Local NetCDF Files
    ↓
stack_nc_to_zarr.py (Convert to Zarr)
    ↓
county_processor.py (Extract county stats)
    ↓
CSV Output
```

### Proposed GEE Pipeline Flow
```
GEE Climate Datasets
    ↓
gee_processor.py (Extract, clip, aggregate)
    ↓
county_processor.py (Extract county stats) [UNCHANGED]
    ↓
CSV Output
```

### Key Insight
- The GEE module will replace the NetCDF→Zarr step
- Outputs GeoTIFF or xarray-compatible format
- Feeds directly into existing `county_processor.py`
- Minimal changes needed to core pipeline

---

## 3. Module Structure

```
src/climate_zarr/
├── gee/                                    # NEW: Google Earth Engine module
│   ├── __init__.py
│   ├── gee_client.py                      # GEE API wrapper & authentication
│   ├── gee_processors.py                  # Variable-specific GEE logic
│   │   ├── BaseGEEProcessor
│   │   ├── GEEPrecipitationProcessor       # IMERG, CHIRPS, etc.
│   │   ├── GEETemperatureProcessor         # ERA5, Copernicus, etc.
│   │   └── GEEWindProcessor                # Optional: wind data
│   ├── gee_strategies.py                  # Processing strategies (similar to existing)
│   ├── gee_data_sources.py                # Catalog of available GEE datasets
│   └── gee_config.py                      # GEE-specific configuration
│
├── utils/
│   ├── gee_utils.py                       # GEE-specific utilities
│   │   ├── clip_to_bbox()
│   │   ├── aggregate_by_county()
│   │   ├── geotiff_to_xarray()
│   │   └── cache_management()
│   └── (existing utils...)
│
└── (existing modules...)

scripts/
├── gee_download.py                        # NEW: CLI for GEE downloads
├── gee_config_setup.py                    # NEW: GEE credentials setup
└── (existing scripts...)
```

---

## 4. Component Details

### 4.1 GEE Client (`gee_client.py`)

**Responsibilities:**
- Authentication (service account or user account)
- Connection management
- Error handling & retry logic

**Key Classes:**
```python
class GEEClient:
    """Wrapper for Google Earth Engine API."""
    
    def __init__(self, credentials_path: Path):
        """Initialize with service account credentials."""
    
    def authenticate(self) -> None:
        """Authenticate with GEE."""
    
    def get_dataset(self, dataset_name: str, **kwargs) -> ee.Image:
        """Fetch dataset from GEE catalog."""
    
    def clip_to_region(self, image: ee.Image, bbox: Dict) -> ee.Image:
        """Clip image to bounding box."""
    
    def export_to_geotiff(self, image: ee.Image, path: Path, **kwargs) -> Path:
        """Export as GeoTIFF to local storage or GCS."""
```

---

### 4.2 Data Sources Registry (`gee_data_sources.py`)

**Single Data Source: NASA CMIP6 via GEE**

Google Earth Engine hosts the NEX-GDDP-CMIP6 dataset in cloud-optimized format:

| Dataset | GEE Collection ID |
|---------|------------------|
| **NEX-GDDP-CMIP6 Daily** | `NASA/GDDP-CMIP6` |

**Available Models** (same as your downloads):
- ACCESS-CM2
- ACCESS-ESM1-5
- BCC-CSM2-MR
- CanESM5
- CNRM-CM6-1
- CNRM-ESM2-1
- EC-Earth3
- GFDL-ESM4
- GISS-E2-1-G
- INM-CM5-0
- IPSL-CM6A-LR
- MIROC6
- MPI-ESM1-2-LR
- MRI-ESM2-0
- UKESM1-0-LL

**Available Variables:**
- `pr` - Precipitation (mm/day)
- `tas` - Temperature (K)
- `tasmax` - Max Temperature (K)
- `tasmin` - Min Temperature (K)

**Available Scenarios:**
- `historical` (1950-2014)
- `ssp126`, `ssp245`, `ssp370`, `ssp585` (2015-2100)

**Configuration Structure:**
```python
GEE_CONFIG = {
    'dataset': {
        'collection_id': 'NASA/GDDP-CMIP6',
        'description': 'NASA Global Downscaled Climate Projections CMIP6',
        'temporal_start': 1950,
        'temporal_end': 2100,
        'spatial_resolution': 0.25,  # degrees
        'unit_mappings': {
            'pr': 'mm/day',
            'tas': 'K',
            'tasmax': 'K',
            'tasmin': 'K',
        }
    },
    'models': [
        'ACCESS-CM2', 'ACCESS-ESM1-5', 'BCC-CSM2-MR', 'CanESM5', 
        'CNRM-CM6-1', 'CNRM-ESM2-1', 'EC-Earth3', 'GFDL-ESM4', 
        'GISS-E2-1-G', 'INM-CM5-0', 'IPSL-CM6A-LR', 'MIROC6', 
        'MPI-ESM1-2-LR', 'MRI-ESM2-0', 'UKESM1-0-LL'
    ],
    'variables': ['pr', 'tas', 'tasmax', 'tasmin'],
    'scenarios': ['historical', 'ssp126', 'ssp245', 'ssp370', 'ssp585'],
}
```

**GEE Collection Structure:**
```
NASA/GDDP-CMIP6
├── {model}_{scenario}_{variable}
│   ├── Image 1950-01-01 (tasmax, pr, tas, tasmin)
│   ├── Image 1950-01-02
│   └── ...
```

**Band Naming Convention:**
- Each date has bands: `{model}_{scenario}_{variable}_{year}_{month}_{day}`
- Access via: `collection.select('{model}_{scenario}_{variable}')`

---

### 4.3 Base GEE Processor (`gee_processors.py`)

**Simplified Architecture:**

Since we're using a single data source (NASA CMIP6), the processor hierarchy is simplified:

```python
class GEENASACMIPProcessor(BaseProcessor):
    """Process NASA CMIP6 data from Google Earth Engine."""
    
    def __init__(self, gee_client: GEEClient, config: GEEConfig):
        self.client = gee_client
        self.config = config
        self.collection = ee.ImageCollection('NASA/GDDP-CMIP6')
    
    def get_dataset(
        self, 
        model: str, 
        variable: str, 
        scenario: str, 
        start_date: str, 
        end_date: str
    ) -> ee.ImageCollection:
        """Fetch CMIP6 data for given parameters."""
        # Filter collection for specific model/variable/scenario
        # Handle date range filtering
        pass
    
    def validate_data(self, collection: ee.ImageCollection) -> bool:
        """Validate temporal coverage and data quality."""
        # Check for missing dates
        # Verify expected date range
        pass
    
    def export_to_geotiff(
        self, 
        collection: ee.ImageCollection, 
        bbox: Dict, 
        output_path: Path
    ) -> Path:
        """Export aggregated data as GeoTIFF."""
        pass
    
    def to_xarray(self, geotiff_path: Path, variable: str) -> xr.DataArray:
        """Convert GeoTIFF to xarray for pipeline compatibility."""
        pass
```

**Key Simplifications:**
- No need for multiple dataset strategies
- Unified variable naming (all from NASA CMIP6)
- Single collection filtering approach
- Direct model→scenario→variable mapping

---

### 4.4 GEE Config (`gee_config.py`)

**Simplified Configuration:**

Since we only use NASA CMIP6, configuration is straightforward:

```python
class GEEConfig(BaseModel):
    """Google Earth Engine configuration for NASA CMIP6."""
    
    # Authentication
    credentials_path: Path = Field(description="Service account JSON path")
    project_id: str = Field(description="GCP project ID")
    
    # Data source (always NASA CMIP6, but explicit for clarity)
    dataset_id: str = Field(
        default='NASA/GDDP-CMIP6',
        description="GEE collection ID"
    )
    
    # Export options
    export_format: str = Field(default='GeoTIFF', choices=['GeoTIFF', 'NetCDF'])
    export_bucket: Optional[str] = Field(
        default=None,
        description="GCS bucket for exports (optional, uses local if None)"
    )
    
    # Performance
    max_pixels: int = Field(default=int(1e8), description="Max pixels per export")
    timeout_seconds: int = Field(default=3600)
    
    # Optional: GCS caching
    cache_exports: bool = Field(default=True, description="Cache exports locally")
    cache_dir: Path = Field(default=Path("/tmp/gee_cache"))
```

**Minimal Configuration** - your existing downloads map directly:
```yaml
# Before (local downloads)
model: ACCESS-CM2
variable: pr
scenario: ssp585
years: 2015-2100

# After (GEE CMIP6)
credentials: ~/.gee/service-account.json
model: ACCESS-CM2      # Same!
variable: pr           # Same!
scenario: ssp585       # Same!
years: 2015-2100       # Same!
```

---

### 4.5 GEE Utilities (`gee_utils.py`)

**Key Functions for NASA CMIP6 Processing:**

```python
def filter_cmip6_collection(
    collection: ee.ImageCollection,
    model: str,
    variable: str,
    scenario: str,
    start_date: str,
    end_date: str
) -> ee.ImageCollection:
    """Filter NASA CMIP6 collection for specific parameters."""

def clip_to_bbox(image: ee.Image, bbox: Dict) -> ee.Image:
    """Clip image to bounding box."""

def aggregate_by_county(
    image: ee.Image, 
    counties: GeoDataFrame,
    variable: str
) -> pd.DataFrame:
    """Aggregate CMIP6 pixel values by county geometry."""

def convert_temperature_units(array: np.ndarray, from_unit='K', to_unit='C') -> np.ndarray:
    """Convert between Kelvin and Celsius (temperature variables only)."""

def geotiff_to_xarray(filepath: Path, variable_name: str) -> xr.DataArray:
    """Convert exported GeoTIFF to xarray for pipeline compatibility."""

def validate_temporal_coverage(
    collection: ee.ImageCollection, 
    expected_dates: List[str]
) -> bool:
    """Verify all expected dates present in collection."""

def cache_gee_export(data_path: Path, cache_dir: Path, metadata: Dict) -> Path:
    """Cache exported GeoTIFF for future use."""
```

---

### 4.6 Pipeline Integration (`pipeline.py` modifications)

**Simplified Pipeline:**

Since NASA CMIP6 is our only data source, we only need a single data processor:

```python
from climate_zarr.gee.gee_processor import GEENASACMIPProcessor

class PipelineConfig(BaseModel):
    # ... existing fields ...
    
    # NEW: Single data source indicator
    use_gee: bool = Field(
        default=False,
        description="Use Google Earth Engine NASA CMIP6 instead of local NetCDF"
    )
    
    # NEW: GEE configuration (only used if use_gee=True)
    gee_credentials: Optional[Path] = Field(
        default=None,
        description="Path to GEE service account JSON"
    )
    gee_project_id: Optional[str] = Field(
        default=None,
        description="GCP project ID for GEE"
    )

# MODIFIED: run_pipeline()
def run_pipeline(config: PipelineConfig) -> Path:
    """Pipeline works with either local NetCDF or GEE CMIP6."""
    
    if config.use_gee:
        # GEE path: fetch CMIP6 data from cloud
        gee_config = GEEConfig(
            credentials_path=config.gee_credentials,
            project_id=config.gee_project_id,
        )
        processor = GEENASACMIPProcessor(gee_config)
        
        # Fetch and export to GeoTIFF
        data_paths = processor.fetch_and_export(
            models=config.models,
            variables=config.variables,
            scenario=config.scenario,
            bbox=config.region_bbox,
        )
        
        # Convert GeoTIFF to xarray (intermediate format)
        data_arrays = [processor.to_xarray(path) for path in data_paths]
        
    else:
        # Local path: use existing NetCDF processing
        data_arrays = discover_and_stack_netcdf(config.nc_dir, ...)
    
    # Rest of pipeline unchanged - works with xarray
    zarr_path = stack_to_zarr_hierarchical(data_arrays, ...)
    final_output = process_counties(zarr_path, ...)
    
    return final_output
```

**Key Advantage:**
- Single conditional branch for data source
- Outputs always xarray format
- Existing county_processor.py untouched
- Easy to toggle between GEE and local

---

## 5. Implementation Phases

### Phase 1: Foundation (1 week)
- [ ] Set up GEE authentication and client
- [ ] Test access to NASA/GDDP-CMIP6 collection
- [ ] Create `gee_client.py` with NASA CMIP6-specific methods
- [ ] Unit tests for client

### Phase 2: Core Processor (1-2 weeks)
- [ ] Implement `GEENASACMIPProcessor` 
- [ ] Build collection filtering by model/variable/scenario
- [ ] Implement GeoTIFF export for regions
- [ ] Create xarray conversion utilities
- [ ] Integration tests with real GEE data

### Phase 3: Pipeline Integration (1 week)
- [ ] Modify `pipeline.py` to support `use_gee` flag
- [ ] Update `PipelineConfig` 
- [ ] Create `gee_utils.py` with helper functions
- [ ] End-to-end tests with small region

### Phase 4: CLI & Documentation (3-4 days)
- [ ] Create `gee_setup.py` for credential configuration
- [ ] Create `gee_download.py` CLI for testing
- [ ] Documentation and usage examples
- [ ] Validation against NetCDF pipeline results

### Phase 5: Testing & Optimization (3-4 days)
- [ ] Performance benchmarking
- [ ] Quality validation (compare GEE vs local NetCDF)
- [ ] Edge case testing (missing dates, regions)
- [ ] Final cleanup and documentation

**Total Timeline:** 3-4 weeks to production-ready

---

## 6. Testing Strategy

### Unit Tests
- `test_gee_client.py`: NASA CMIP6 collection access, filtering
- `test_gee_processor.py`: Data extraction, unit conversion, export
- `test_gee_utils.py`: Utility functions (aggregation, xarray conversion)
- `test_gee_config.py`: Configuration validation

### Integration Tests
- `test_gee_pipeline_integration.py`: Full pipeline with GEE CMIP6 data
- `test_gee_county_extraction.py`: County-level statistics extraction
- `test_gee_vs_netcdf.py`: **Validation**: Compare GEE vs local NetCDF for identical model/scenario/region

### End-to-End Tests
- Small region (single county) test
- Full CONUS test with real GEE data
- Multi-year time series validation
- Comparison matrices (GEE output vs NetCDF reference)

---

## 7. Dependencies

**New Requirements:**
- `earthengine-api>=0.1.366` - GEE Python API
- `google-auth>=2.20.0` - Google authentication
- `rasterio>=1.3.0` - GeoTIFF handling
- `rioxarray>=0.13.0` - xarray + rasterio integration
- `google-cloud-storage>=2.5.0` - Optional: for GCS bucket caching

**Update `pyproject.toml`:**
```toml
[project.optional-dependencies]
gee = [
    "earthengine-api>=0.1.366",
    "google-auth>=2.20.0",
    "rasterio>=1.3.0",
    "rioxarray>=0.13.0",
    "google-cloud-storage>=2.5.0",
]
```

**Installation:**
```bash
# Install with GEE support
pip install -e ".[gee]"
```

---

## 8. Configuration Example

### Setup (One-time)
```bash
# 1. Create GCP project and service account
#    - Go to console.cloud.google.com
#    - Create project
#    - Create service account with "Editor" role
#    - Download credentials JSON

# 2. Enable Earth Engine API
#    - In GCP Console, enable "Earth Engine API"

# 3. Register project with Earth Engine
#    python -m scripts.gee_setup register \
#        --credentials /path/to/service-account.json \
#        --project-id my-gcp-project

# 4. Verify access to NASA CMIP6
#    python -c "
#    import ee
#    from climate_zarr.gee import GEEClient
#    client = GEEClient('/path/to/service-account.json')
#    collection = ee.ImageCollection('NASA/GDDP-CMIP6')
#    print(f'Collection size: {collection.size().getInfo()}')
#    "
```

### Usage Example 1: Direct GEE Pipeline
```python
from climate_zarr.pipeline import run_pipeline, PipelineConfig

config = PipelineConfig(
    use_gee=True,
    gee_credentials=Path("~/.gee/service-account.json"),
    gee_project_id="my-gcp-project",
    region='conus',
    variables=['pr', 'tas'],
    scenario='ssp585',
)

output_csv = run_pipeline(config)
```

### Usage Example 2: Fallback to Local NetCDF
```python
config = PipelineConfig(
    use_gee=False,  # Use local files
    nc_dir=Path("/c/repos/data/climate_download/data/ACCESS-CM2"),
    region='conus',
    variables=['pr', 'tas'],
    scenario='ssp585',
)

output_csv = run_pipeline(config)
```

### Comparison Test
```bash
# Validate GEE data matches your existing NetCDF downloads
python scripts/validate_gee_vs_netcdf.py \
    --gee-credentials ~/.gee/service-account.json \
    --netcdf-dir /c/repos/data/climate_download/data/ACCESS-CM2 \
    --model ACCESS-CM2 \
    --variable pr \
    --scenario ssp585 \
    --region conus
```

---

## 9. Known Considerations & Trade-offs

### Advantages
- ✅ **No storage burden**: Skip 100GB+ of local NetCDF files
- ✅ **Identical data source**: Same NASA CMIP6 models you're already using
- ✅ **Instant model access**: Add new models without re-downloading
- ✅ **Cloud processing**: Leverage GEE's parallel infrastructure
- ✅ **Version control**: Data updates automatically from NASA
- ✅ **Reproducibility**: Exact model version stamped with date

### Disadvantages
- ❌ Requires GCP account (free tier available: $300 credit)
- ❌ GEE API learning curve (but hidden behind our wrapper)
- ❌ Slightly slower for very small regions (download overhead)
- ❌ Network dependency (need internet connection)

### Mitigation Strategies
- **Cost**: Most CMIP6 processing falls within GCP free tier
- **Learning**: All GEE complexity isolated in `gee/` module
- **Performance**: Local export caching for frequent reuse
- **Reliability**: Fallback to local NetCDF anytime (toggle `use_gee` flag)

### Data Quality Notes
- NASA CMIP6 on GEE = exact same data as your NEX-GDDP-CMIP6 HTTP downloads
- Same models: NorESM2-LM, ACCESS-CM2, CanESM5, etc.
- Same variables: pr, tas, tasmax, tasmin
- Same quality: No differences in aggregation or processing

---

## 10. Future Enhancements

1. **Batch processing**: Process entire climate scenario in parallel
2. **Streaming export**: Real-time data fetch without full export
3. **Multi-region masking**: Process multiple regions simultaneously
4. **Temporal interpolation**: Fill any missing dates intelligently
5. **Data fusion**: Optional blend with satellite observations for validation
6. **Cost monitoring**: Track GCP compute costs per pipeline run
7. **Cron scheduling**: Automated monthly/yearly pipeline updates

## 11. Success Criteria

- [ ] GEE pipeline produces identical statistics to NetCDF pipeline (±0.1% tolerance)
- [ ] GEE pipeline 40-50% faster for typical CONUS region
- [ ] <5 minutes setup time for credentials and authentication
- [ ] 95%+ test coverage for GEE modules
- [ ] Full documentation with working examples
- [ ] No breaking changes to existing pipeline
- [ ] Toggle between GEE/NetCDF requires only one config parameter

## 12. Module File Structure (Final)

```
src/climate_zarr/
├── gee/                                    # Google Earth Engine module
│   ├── __init__.py
│   ├── gee_client.py                      # GEE API wrapper for CMIP6
│   ├── gee_processor.py                   # GEENASACMIPProcessor
│   ├── gee_config.py                      # GEEConfig schema
│   ├── gee_utils.py                       # Collection filtering, export utilities
│   └── __all__ = ['GEEClient', 'GEENASACMIPProcessor', 'GEEConfig']
│
└── (existing modules...)

scripts/
├── gee_setup.py                           # One-time: GEE credentials setup
├── gee_download.py                        # CLI: Test/debug GEE downloads  
├── validate_gee_vs_netcdf.py              # Validation: Compare outputs
└── (existing scripts...)

tests/
├── test_gee_client.py
├── test_gee_processor.py
├── test_gee_utils.py
├── test_gee_config.py
├── test_gee_pipeline_integration.py
└── test_gee_vs_netcdf.py
```

