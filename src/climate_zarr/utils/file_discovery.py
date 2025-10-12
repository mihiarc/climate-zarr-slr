"""
Robust NetCDF file discovery and validation utility.

Handles common issues with file system artifacts, hidden files, and corrupted data.
"""

from pathlib import Path
from typing import List, Optional, Tuple
import xarray as xr
from rich.console import Console
from rich.progress import Progress, SpinnerColumn, TextColumn

console = Console()


def is_valid_netcdf(file_path: Path, quick_check: bool = True) -> Tuple[bool, Optional[str]]:
    """
    Validate if a file is a readable NetCDF file.

    Args:
        file_path: Path to the file to validate
        quick_check: If True, only check if xarray can open the file.
                    If False, also verify dataset structure.

    Returns:
        Tuple of (is_valid, error_message)
    """
    try:
        # Try to open the file with xarray
        with xr.open_dataset(file_path, engine='netcdf4') as ds:
            if not quick_check:
                # Verify it has dimensions and variables
                if not ds.dims:
                    return False, "No dimensions found"
                if not ds.data_vars:
                    return False, "No data variables found"
        return True, None
    except Exception as e:
        error_msg = str(e)
        # Simplify common error messages
        if "did not find a match in any of xarray" in error_msg:
            error_msg = "Not a valid NetCDF file"
        elif "No such file" in error_msg:
            error_msg = "File not found or inaccessible"
        return False, error_msg


def should_exclude_file(file_path: Path) -> Tuple[bool, Optional[str]]:
    """
    Check if a file should be excluded based on common patterns.

    Returns:
        Tuple of (should_exclude, reason)
    """
    filename = file_path.name

    # macOS resource fork files
    if filename.startswith('._'):
        return True, "macOS resource fork file"

    # Hidden files (Unix-style)
    if filename.startswith('.'):
        return True, "Hidden file"

    # Temporary files
    if filename.startswith('~') or filename.endswith('~'):
        return True, "Temporary file"

    # Corrupted or backup files
    if any(suffix in filename for suffix in ['.corrupted', '.backup', '.bak', '.tmp']):
        return True, "Backup or corrupted file marker"

    # macOS DS_Store files
    if filename == '.DS_Store':
        return True, "macOS metadata file"

    # Thumbs.db (Windows thumbnail cache)
    if filename.lower() == 'thumbs.db':
        return True, "Windows thumbnail cache"

    return False, None


def discover_netcdf_files(
    directory: Path,
    pattern: str = "*.nc",
    validate: bool = True,
    verbose: bool = True,
    fail_on_invalid: bool = False
) -> List[Path]:
    """
    Discover and validate NetCDF files in a directory.

    Args:
        directory: Directory to search for NetCDF files
        pattern: Glob pattern to match files (default: "*.nc")
        validate: If True, validate each file can be opened by xarray
        verbose: If True, print discovery progress and warnings
        fail_on_invalid: If True, raise exception on invalid files

    Returns:
        List of valid NetCDF file paths

    Raises:
        ValueError: If fail_on_invalid=True and invalid files are found
    """
    if not directory.exists():
        raise FileNotFoundError(f"Directory not found: {directory}")

    if not directory.is_dir():
        raise ValueError(f"Path is not a directory: {directory}")

    # Find all matching files
    all_files = sorted(directory.glob(pattern))

    if verbose:
        console.print(f"[dim]Scanning {directory} for {pattern} files...[/dim]")

    valid_files = []
    excluded_files = []
    invalid_files = []

    # Filter and validate files
    for file_path in all_files:
        # Check if file should be excluded
        should_exclude, exclude_reason = should_exclude_file(file_path)
        if should_exclude:
            excluded_files.append((file_path, exclude_reason))
            if verbose:
                console.print(f"[yellow]⏭️  Skipping {file_path.name}: {exclude_reason}[/yellow]")
            continue

        # Validate file if requested
        if validate:
            is_valid, error_msg = is_valid_netcdf(file_path, quick_check=True)
            if is_valid:
                valid_files.append(file_path)
            else:
                invalid_files.append((file_path, error_msg))
                if verbose:
                    console.print(f"[red]❌ Invalid {file_path.name}: {error_msg}[/red]")
        else:
            valid_files.append(file_path)

    # Summary
    if verbose:
        console.print(f"\n[bold cyan]Discovery Summary:[/bold cyan]")
        console.print(f"  • Found: {len(all_files)} files matching {pattern}")
        console.print(f"  • Valid: [green]{len(valid_files)}[/green]")
        if excluded_files:
            console.print(f"  • Excluded: [yellow]{len(excluded_files)}[/yellow] (system files)")
        if invalid_files:
            console.print(f"  • Invalid: [red]{len(invalid_files)}[/red] (cannot be read)")

    # Handle invalid files
    if invalid_files and fail_on_invalid:
        error_msg = f"Found {len(invalid_files)} invalid NetCDF files:\n"
        for file_path, error in invalid_files[:5]:  # Show first 5
            error_msg += f"  - {file_path.name}: {error}\n"
        if len(invalid_files) > 5:
            error_msg += f"  ... and {len(invalid_files) - 5} more"
        raise ValueError(error_msg)

    return valid_files


def validate_netcdf_batch(
    file_paths: List[Path],
    show_progress: bool = True
) -> Tuple[List[Path], List[Tuple[Path, str]]]:
    """
    Validate a batch of NetCDF files.

    Args:
        file_paths: List of file paths to validate
        show_progress: If True, show progress bar

    Returns:
        Tuple of (valid_files, invalid_files_with_errors)
    """
    valid_files = []
    invalid_files = []

    if show_progress:
        with Progress(
            SpinnerColumn(),
            TextColumn("[progress.description]{task.description}"),
            console=console,
        ) as progress:
            task = progress.add_task(
                f"Validating {len(file_paths)} NetCDF files...",
                total=len(file_paths)
            )

            for file_path in file_paths:
                is_valid, error_msg = is_valid_netcdf(file_path, quick_check=True)
                if is_valid:
                    valid_files.append(file_path)
                else:
                    invalid_files.append((file_path, error_msg))
                progress.advance(task)
    else:
        for file_path in file_paths:
            is_valid, error_msg = is_valid_netcdf(file_path, quick_check=True)
            if is_valid:
                valid_files.append(file_path)
            else:
                invalid_files.append((file_path, error_msg))

    return valid_files, invalid_files


def get_netcdf_info(file_path: Path) -> dict:
    """
    Get basic information about a NetCDF file.

    Returns:
        Dictionary with file information (dims, vars, coords, size)
    """
    try:
        with xr.open_dataset(file_path) as ds:
            return {
                'dims': dict(ds.dims),
                'data_vars': list(ds.data_vars.keys()),
                'coords': list(ds.coords.keys()),
                'size_mb': file_path.stat().st_size / (1024 * 1024),
                'valid': True
            }
    except Exception as e:
        return {
            'valid': False,
            'error': str(e),
            'size_mb': file_path.stat().st_size / (1024 * 1024) if file_path.exists() else 0
        }


# Convenience function for common use case
def safe_glob_netcdf(directory: Path, pattern: str = "*.nc") -> List[Path]:
    """
    Safe wrapper around glob that filters out problematic files.

    This is a drop-in replacement for Path.glob() that automatically
    excludes hidden files, system files, and validates NetCDF format.

    Args:
        directory: Directory to search
        pattern: Glob pattern (default: "*.nc")

    Returns:
        List of valid NetCDF file paths

    Example:
        >>> from pathlib import Path
        >>> from climate_zarr.utils.file_discovery import safe_glob_netcdf
        >>> files = safe_glob_netcdf(Path("/data/climate"))
    """
    return discover_netcdf_files(
        directory=directory,
        pattern=pattern,
        validate=True,
        verbose=False,
        fail_on_invalid=False
    )
