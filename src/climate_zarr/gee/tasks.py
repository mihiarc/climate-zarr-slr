"""GEE batch export task management.

Submits ``Export.table.*`` tasks for all (variable, year) combos, polls
for completion with a rich progress bar, reads results back, and cleans
up temporary assets/files.

Supports two export backends:
- **asset** (default) — ``Export.table.toAsset``.  No GCS bucket needed.
- **gcs** — ``Export.table.toCloudStorage``.  Requires ``gcsfs`` and a bucket.
"""

from __future__ import annotations

import signal
import time
from dataclasses import dataclass, field
from typing import Optional

import ee
import pandas as pd
from rich.console import Console
from rich.progress import (
    BarColumn,
    Progress,
    SpinnerColumn,
    TaskProgressColumn,
    TextColumn,
    TimeElapsedColumn,
)

from climate_zarr.gee.config import ExportBackend, GEEConfig

console = Console()

# Terminal GEE task states
_TERMINAL_STATES = {"COMPLETED", "FAILED", "CANCELLED", "CANCEL_REQUESTED"}


@dataclass
class TaskSpec:
    """Tracks one GEE batch export task."""

    variable: str
    year: int
    model: str
    scenario: str
    feature_collection: ee.FeatureCollection
    task: Optional[ee.batch.Task] = None
    asset_id: Optional[str] = None
    gcs_path: Optional[str] = None
    state: str = "PENDING"
    error_message: Optional[str] = None
    description: str = field(default="", init=False)

    def __post_init__(self) -> None:
        self.description = f"{self.variable}_{self.year}_{self.model}_{self.scenario}"


def ensure_asset_folder(project_id: str, folder_name: str) -> str:
    """Create a temporary asset folder if it does not already exist.

    Parameters
    ----------
    project_id : str
        GEE project ID (e.g. ``"my-project"``).
    folder_name : str
        Folder name under ``projects/{project_id}/assets/``.

    Returns
    -------
    str
        Full asset folder path.
    """
    folder_path = f"projects/{project_id}/assets/{folder_name}"
    try:
        ee.data.getAsset(folder_path)
        console.print(f"  [dim]Asset folder exists: {folder_path}[/dim]")
    except ee.EEException:
        console.print(f"  [cyan]Creating asset folder: {folder_path}[/cyan]")
        ee.data.createAsset({"type": "FOLDER"}, folder_path)
    return folder_path


def submit_export_tasks(
    specs: list[TaskSpec],
    gee_config: GEEConfig,
) -> list[TaskSpec]:
    """Submit all export tasks to GEE and call ``task.start()``.

    Each spec gets a ``task``, ``asset_id`` (or ``gcs_path``), and is
    set to ``"SUBMITTED"`` state.

    Parameters
    ----------
    specs : list[TaskSpec]
        Task specifications with server-side FeatureCollections.
    gee_config : GEEConfig
        Configuration with export backend settings.

    Returns
    -------
    list[TaskSpec]
        The same list, updated in place.
    """
    backend = gee_config.export_backend

    if backend == ExportBackend.ASSET:
        folder_path = ensure_asset_folder(
            gee_config.project_id, gee_config.asset_folder
        )

    console.print(
        f"[bold blue]Submitting {len(specs)} export tasks "
        f"(backend={backend.value})[/bold blue]"
    )

    for spec in specs:
        if backend == ExportBackend.ASSET:
            asset_id = f"{folder_path}/{spec.description}"
            spec.asset_id = asset_id

            # Delete pre-existing asset to avoid name collision
            try:
                ee.data.getAsset(asset_id)
                ee.data.deleteAsset(asset_id)
                console.print(f"  [dim]Deleted stale asset: {asset_id}[/dim]")
            except ee.EEException:
                pass

            export_task = ee.batch.Export.table.toAsset(
                collection=spec.feature_collection,
                description=spec.description,
                assetId=asset_id,
            )
        elif backend == ExportBackend.GCS:
            gcs_key = f"{gee_config.gcs_prefix}/{spec.description}"
            spec.gcs_path = f"gs://{gee_config.gcs_bucket}/{gcs_key}"

            export_task = ee.batch.Export.table.toCloudStorage(
                collection=spec.feature_collection,
                description=spec.description,
                bucket=gee_config.gcs_bucket,
                fileNamePrefix=gcs_key,
                fileFormat="CSV",
            )
        else:
            raise ValueError(f"Unsupported export backend: {backend}")

        export_task.start()
        spec.task = export_task
        spec.state = "SUBMITTED"

    console.print(
        f"[green]All {len(specs)} tasks submitted to GEE[/green]"
    )
    return specs


def poll_tasks(
    specs: list[TaskSpec],
    poll_interval: int = 30,
) -> list[TaskSpec]:
    """Poll GEE tasks until all reach a terminal state.

    Displays a rich progress bar.  Handles Ctrl+C by cancelling all
    running tasks to prevent orphaned GEE jobs.

    Parameters
    ----------
    specs : list[TaskSpec]
        Task specifications with started tasks.
    poll_interval : int
        Seconds between status checks.

    Returns
    -------
    list[TaskSpec]
        Updated specs with final states.
    """
    total_tasks = len(specs)
    cancelled = False

    def handle_interrupt(signum, frame):
        nonlocal cancelled
        cancelled = True
        console.print(
            "\n[bold red]Ctrl+C detected — cancelling running GEE tasks...[/bold red]"
        )
        for spec in specs:
            if spec.task and spec.state not in _TERMINAL_STATES:
                try:
                    spec.task.cancel()
                    spec.state = "CANCEL_REQUESTED"
                except Exception:
                    pass

    original_handler = signal.getsignal(signal.SIGINT)
    signal.signal(signal.SIGINT, handle_interrupt)

    try:
        with Progress(
            SpinnerColumn(),
            TextColumn("[progress.description]{task.description}"),
            BarColumn(),
            TaskProgressColumn(),
            TimeElapsedColumn(),
            console=console,
        ) as progress:
            progress_task = progress.add_task(
                "GEE batch export", total=total_tasks
            )
            completed_count = 0

            while completed_count < total_tasks and not cancelled:
                new_completed = 0
                for spec in specs:
                    if spec.state in _TERMINAL_STATES:
                        continue

                    try:
                        status = spec.task.status()
                        spec.state = status.get("state", spec.state)
                    except Exception as error:
                        console.print(
                            f"  [yellow]Status check failed for "
                            f"{spec.description}: {error}[/yellow]"
                        )
                        continue

                    if spec.state in _TERMINAL_STATES:
                        new_completed += 1
                        if spec.state == "COMPLETED":
                            console.print(
                                f"  [green]{spec.description}: COMPLETED[/green]"
                            )
                        elif spec.state == "FAILED":
                            spec.error_message = status.get(
                                "error_message", "Unknown error"
                            )
                            console.print(
                                f"  [red]{spec.description}: FAILED — "
                                f"{spec.error_message}[/red]"
                            )
                        else:
                            console.print(
                                f"  [yellow]{spec.description}: "
                                f"{spec.state}[/yellow]"
                            )

                completed_count += new_completed
                progress.update(
                    progress_task,
                    completed=sum(
                        1 for s in specs if s.state in _TERMINAL_STATES
                    ),
                )

                if completed_count < total_tasks and not cancelled:
                    time.sleep(poll_interval)

    finally:
        signal.signal(signal.SIGINT, original_handler)

    succeeded = sum(1 for s in specs if s.state == "COMPLETED")
    failed = sum(1 for s in specs if s.state == "FAILED")
    other = total_tasks - succeeded - failed

    console.print(
        f"[bold]Task results: {succeeded} completed, {failed} failed"
        + (f", {other} cancelled/other" if other else "")
        + "[/bold]"
    )

    return specs


def read_task_result(
    spec: TaskSpec,
    gee_config: GEEConfig,
) -> pd.DataFrame:
    """Read the result of a completed export task back as a DataFrame.

    For **asset** backend, loads the exported asset as an
    ``ee.FeatureCollection`` and converts via ``extract_to_dataframe()``.
    For **gcs** backend, reads the CSV directly with ``gcsfs``.

    Parameters
    ----------
    spec : TaskSpec
        A completed task specification.
    gee_config : GEEConfig
        Configuration with backend details.

    Returns
    -------
    pd.DataFrame
        Raw DataFrame from the export (needs post-processing).

    Raises
    ------
    RuntimeError
        If the task did not complete successfully.
    """
    if spec.state != "COMPLETED":
        raise RuntimeError(
            f"Cannot read result for {spec.description}: state={spec.state}"
        )

    backend = gee_config.export_backend

    if backend == ExportBackend.ASSET:
        from climate_zarr.gee.extract import extract_to_dataframe

        asset_collection = ee.FeatureCollection(spec.asset_id)
        return extract_to_dataframe(asset_collection)

    elif backend == ExportBackend.GCS:
        try:
            import gcsfs
        except ImportError:
            raise ImportError(
                "gcsfs is required for GCS export backend. "
                "Install with: pip install climate-zarr-slr[gee-gcs]"
            )

        filesystem = gcsfs.GCSFileSystem()
        # GEE appends .csv to the fileNamePrefix
        csv_path = spec.gcs_path
        if not csv_path.endswith(".csv"):
            csv_path = csv_path + ".csv"

        # Strip gs:// prefix for gcsfs
        gcs_key = csv_path.replace("gs://", "")
        with filesystem.open(gcs_key, "r") as file_handle:
            return pd.read_csv(file_handle)

    else:
        raise ValueError(f"Unsupported export backend: {backend}")


def cleanup_exports(
    specs: list[TaskSpec],
    gee_config: GEEConfig,
    remove_folder: bool = True,
) -> None:
    """Delete temporary export assets or GCS files.

    Logs warnings on failure but never raises — cleanup errors should
    not abort the pipeline.

    Parameters
    ----------
    specs : list[TaskSpec]
        Task specifications (only completed ones have assets to clean).
    gee_config : GEEConfig
        Configuration with backend details.
    remove_folder : bool
        Whether to also remove the asset folder (asset backend only).
    """
    backend = gee_config.export_backend
    cleaned = 0
    errors = 0

    if backend == ExportBackend.ASSET:
        for spec in specs:
            if spec.asset_id:
                try:
                    ee.data.deleteAsset(spec.asset_id)
                    cleaned += 1
                except Exception as error:
                    console.print(
                        f"  [yellow]Could not delete asset "
                        f"{spec.asset_id}: {error}[/yellow]"
                    )
                    errors += 1

        if remove_folder:
            folder_path = (
                f"projects/{gee_config.project_id}/assets/"
                f"{gee_config.asset_folder}"
            )
            try:
                ee.data.deleteAsset(folder_path)
                console.print(
                    f"  [dim]Removed asset folder: {folder_path}[/dim]"
                )
            except Exception as error:
                console.print(
                    f"  [yellow]Could not remove folder "
                    f"{folder_path}: {error}[/yellow]"
                )

    elif backend == ExportBackend.GCS:
        try:
            import gcsfs
        except ImportError:
            console.print(
                "[yellow]gcsfs not installed, skipping GCS cleanup[/yellow]"
            )
            return

        filesystem = gcsfs.GCSFileSystem()
        for spec in specs:
            if spec.gcs_path:
                gcs_key = spec.gcs_path.replace("gs://", "")
                csv_key = gcs_key if gcs_key.endswith(".csv") else gcs_key + ".csv"
                try:
                    filesystem.rm(csv_key)
                    cleaned += 1
                except Exception as error:
                    console.print(
                        f"  [yellow]Could not delete "
                        f"{csv_key}: {error}[/yellow]"
                    )
                    errors += 1

    console.print(
        f"[dim]Cleanup: {cleaned} removed, {errors} errors[/dim]"
    )
