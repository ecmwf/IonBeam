import logging
import time
from typing import Any, Dict, List, Tuple

import httpx
from deepdiff import DeepDiff
from fastapi import Request
from pydantic import BaseModel

logger = logging.getLogger(__name__)

# Platform-specific field paths to ignore during comparison
PLATFORM_IGNORE_PATHS = {
    "meteotracker": [
        "root['name']"
        "root['location']['lat']",
        "root['location']['lon']",
    ],
    "acronet": [
        # "root['mars_selection']['date']",
        # "root['time_span']['start']",
        # "root['time_span']['end']",
    ],
}

# Base ignore paths for all platforms (known differences between APIs)
BASE_IGNORE_PATHS = [
    "root['internal_id']",
    "root['mars_selection']['station_id']",
    "root['authors'][0]['id']"
]

# Platform-specific field paths to ignore for observations
OBSERVATION_IGNORE_PATHS = {
    "meteotracker": [
        "root['unknown']",
        "root['station_id']",
        "root['station_name']",
    ],
    "acronet": [
        "root['station_id']",
        "root['wind_gust']", # This is different because IonbeamLegacy uses the original 'Knots' reading - new ionbeam converts this to M/S to match canonicalisation.
    ],
}

# Base ignore paths for all platforms (observations)
OBSERVATION_BASE_IGNORE_PATHS: List[str] = [
    # Add common fields to ignore
    # Examples: "root['chunk_date']", "root['chunk_time']"
]


# ============================================================================
# Models
# ============================================================================

class ComparisonSummary(BaseModel):
    """Simplified comparison summary with essential statistics."""
    total_new: int
    total_legacy: int
    matched_count: int
    differing_count: int
    only_in_new_count: int
    only_in_legacy_count: int
    comparison_duration_ms: float


class ComparisonResult(BaseModel):
    """Complete comparison result."""
    summary: ComparisonSummary
    only_in_new: List[Dict[str, str]]  # List of {"id": external_id, "platform": platform}
    only_in_legacy: List[Dict[str, str]]  # List of {"id": external_id, "platform": platform}
    differing_stations: List[Dict[str, Any]]  # external_id -> diff mapping


class ObservationComparisonSummary(BaseModel):
    """Summary statistics for observation comparison."""
    total_new: int
    total_legacy: int
    matched_count: int
    differing_count: int
    only_in_new_count: int
    only_in_legacy_count: int
    comparison_duration_ms: float


class ObservationComparisonResult(BaseModel):
    """Complete observation comparison result."""
    summary: ObservationComparisonSummary
    only_in_new: List[Dict[str, str]]  # List of {platform, external_station_id, datetime}
    only_in_legacy: List[Dict[str, str]]  # List of {platform, external_station_id, datetime}
    differing_observations: List[Dict[str, Any]]  # Composite key -> diff mapping


# ============================================================================
# Comparison Service
# ============================================================================

class StationsComparisonServiceV2:
    """Minimal comparison service using DeepDiff."""

    def __init__(
        self,
        legacy_base_url: str,
        significant_digits: int = 2,
        timeout: float = 5.0,
    ):
        self.legacy_base_url = legacy_base_url.rstrip("/")
        self.significant_digits = significant_digits
        self.timeout = timeout
        self.http_client = httpx.AsyncClient(timeout=timeout)

    async def close(self):
        """Close the HTTP client."""
        await self.http_client.aclose()

    async def call_legacy_api(
        self, endpoint: str, params: Dict[str, Any]
    ) -> List[Dict[str, Any]]:
        """Call the legacy API with given parameters."""
        url = f"{self.legacy_base_url}{endpoint}"
        filtered_params = {k: v for k, v in params.items() if v is not None}
        
        logger.info(f"Calling legacy API: {url} with params: {filtered_params}")
        
        response = await self.http_client.get(url, params=filtered_params)
        response.raise_for_status()
        return response.json()

    def _match_items(
        self,
        new_items: List[dict],
        legacy_items: List[dict],
        key_fn
    ) -> Tuple[List[Tuple[dict, dict]], List[dict], List[dict]]:
        """Match items by key function.
        
        Args:
            new_items: Items from new API
            legacy_items: Items from legacy API
            key_fn: Function to extract key from item
            
        Returns:
            Tuple of (matched_pairs, only_in_new, only_in_legacy)
        """
        new_dict = {key_fn(item): item for item in new_items if key_fn(item) is not None}
        legacy_dict = {key_fn(item): item for item in legacy_items if key_fn(item) is not None}
        
        # Find exact matches
        matched = [
            (new_dict[key], legacy_dict[key])
            for key in new_dict
            if key in legacy_dict
        ]
        
        # Find only in new/legacy
        only_new = [item for key, item in new_dict.items() if key not in legacy_dict]
        only_legacy = [item for key, item in legacy_dict.items() if key not in new_dict]
        
        return matched, only_new, only_legacy

    def _get_ignore_paths(self, platform: str) -> List[str]:
        """Get combined ignore paths for a platform (stations)."""
        platform_paths = PLATFORM_IGNORE_PATHS.get(platform, [])
        return BASE_IGNORE_PATHS + platform_paths

    def _get_ignore_paths_for_observations(self, platform: str) -> List[str]:
        """Get combined ignore paths for observation comparisons."""
        platform_paths = OBSERVATION_IGNORE_PATHS.get(platform, [])
        return OBSERVATION_BASE_IGNORE_PATHS + platform_paths

    def _compare_items(
        self,
        new_item: dict,
        legacy_item: dict,
        ignore_paths: List[str]
    ) -> dict | None:
        """Compare two items using DeepDiff with tree view.
        
        Args:
            new_item: Item from new API
            legacy_item: Item from legacy API
            ignore_paths: Paths to ignore during comparison
            
        Returns:
            Dict representing differences, or None if items match
        """
        diff = DeepDiff(
            legacy_item,
            new_item,
            exclude_paths=ignore_paths,
            significant_digits=self.significant_digits,
            ignore_order=False,
            verbose_level=1,
            view='tree',
            ignore_type_in_groups=[(str, type(None))],
        )
        
        # Filter out datetime type changes - semantically equivalent in JSON
        diff = self._filter_datetime_type_changes(diff)
        
        return dict(diff) if diff else None
    
    def _filter_datetime_type_changes(self, diff) -> dict | None:
        """Filter out datetime type changes from diff results.
        
        Args:
            diff: DeepDiff result
            
        Returns:
            Filtered diff or None if no differences remain
        """
        if not diff or 'type_changes' not in diff:
            return diff
            
        filtered_type_changes = set()
        for change in diff['type_changes']:
            old_type_name = type(change.t1).__name__ if hasattr(change, 't1') else ''
            new_type_name = type(change.t2).__name__ if hasattr(change, 't2') else ''
            
            # Skip if one is a string and the other is a datetime-like object
            is_datetime_change = (
                (old_type_name == 'str' and 'Timestamp' in new_type_name) or
                (new_type_name == 'str' and 'Timestamp' in old_type_name) or
                (old_type_name == 'str' and 'datetime' in new_type_name) or
                (new_type_name == 'str' and 'datetime' in old_type_name)
            )
            
            if not is_datetime_change:
                filtered_type_changes.add(change)
        
        if filtered_type_changes:
            diff['type_changes'] = filtered_type_changes
        else:
            del diff['type_changes']
            
        return diff if diff else None

    async def compare_stations(
        self, new_stations: List[dict], request_params: dict
    ) -> ComparisonResult:
        """Compare new API stations response with legacy API."""
        start_time = time.time()
        
        # Call legacy API
        legacy_stations = await self.call_legacy_api("/api/v1/stations", request_params)
        
        # Match stations by external_id
        matched_pairs, only_new, only_legacy = self._match_items(
            new_stations,
            legacy_stations,
            key_fn=lambda s: s.get("external_id")
        )
        
        # Compare matched pairs
        differing = []
        for new_station, legacy_station in matched_pairs:
            platform = new_station.get("platform", "")
            ignore_paths = self._get_ignore_paths(platform)
            diff = self._compare_items(new_station, legacy_station, ignore_paths)
            if diff:
                differing.append({
                    "external_id": new_station.get("external_id", ""),
                    "diff": diff
                })
        
        duration_ms = (time.time() - start_time) * 1000
        matched_count = len(matched_pairs) - len(differing)
        
        # Format only_in lists with platform info
        only_new_formatted = [
            {"id": s.get("external_id", ""), "platform": s.get("platform", "unknown")}
            for s in only_new
        ]
        only_legacy_formatted = [
            {"id": s.get("external_id", ""), "platform": s.get("platform", "unknown")}
            for s in only_legacy
        ]
        
        summary = ComparisonSummary(
            total_new=len(new_stations),
            total_legacy=len(legacy_stations),
            matched_count=matched_count,
            differing_count=len(differing),
            only_in_new_count=len(only_new_formatted),
            only_in_legacy_count=len(only_legacy_formatted),
            comparison_duration_ms=duration_ms,
        )
        
        return ComparisonResult(
            summary=summary,
            only_in_new=only_new_formatted,
            only_in_legacy=only_legacy_formatted,
            differing_stations=differing,
        )

    def _make_observation_key(self, obs: dict) -> Tuple[str, str, str]:
        """Create composite key for observation matching."""
        return (
            obs.get("platform", ""),
            obs.get("external_station_id", ""),
            obs.get("datetime", "")
        )
    
    def _format_observation_key(self, key: Tuple[str, str, str]) -> Dict[str, str]:
        """Format observation key tuple as dictionary."""
        return {
            "platform": key[0],
            "external_station_id": key[1],
            "datetime": key[2]
        }

    async def compare_observations(
        self, new_observations: List[dict], request_params: dict
    ) -> ObservationComparisonResult:
        """Compare new API observation response with legacy API."""
        start_time = time.time()
        
        # Call legacy API
        legacy_observations = await self.call_legacy_api("/api/v1/retrieve", request_params)
        
        # Match observations by composite key
        matched_pairs, only_new, only_legacy = self._match_items(
            new_observations,
            legacy_observations,
            key_fn=self._make_observation_key
        )
        
        # Compare matched pairs
        differing = []
        for new_obs, legacy_obs in matched_pairs:
            platform = new_obs.get("platform", "")
            ignore_paths = self._get_ignore_paths_for_observations(platform)
            diff = self._compare_items(new_obs, legacy_obs, ignore_paths)
            if diff:
                differing.append({
                    "composite_key": self._format_observation_key(
                        self._make_observation_key(new_obs)
                    ),
                    "diff": diff
                })
        
        duration_ms = (time.time() - start_time) * 1000
        matched_count = len(matched_pairs) - len(differing)
        
        # Format only_in lists
        only_new_formatted = [
            self._format_observation_key(self._make_observation_key(obs))
            for obs in only_new
        ]
        only_legacy_formatted = [
            self._format_observation_key(self._make_observation_key(obs))
            for obs in only_legacy
        ]
        
        summary = ObservationComparisonSummary(
            total_new=len(new_observations),
            total_legacy=len(legacy_observations),
            matched_count=matched_count,
            differing_count=len(differing),
            only_in_new_count=len(only_new_formatted),
            only_in_legacy_count=len(only_legacy_formatted),
            comparison_duration_ms=duration_ms,
        )
        
        return ObservationComparisonResult(
            summary=summary,
            only_in_new=only_new_formatted,
            only_in_legacy=only_legacy_formatted,
            differing_observations=differing,
        )


# ============================================================================
# Helper Functions
# ============================================================================

async def compare_stations_response(
    new_stations: List[dict],
    request: Request,
    comparison_service: StationsComparisonServiceV2 | None,
) -> None:
    """Compare stations response with legacy API and log results."""
    if not comparison_service:
        return
    
    try:
        params = dict(request.query_params)
        result = await comparison_service.compare_stations(new_stations, params)
        _log_comparison_result(result)
    except Exception as e:
        logger.error(f"Comparison failed: {str(e)}", exc_info=True)


async def compare_observations_response(
    new_observations: List[dict],
    request: Request,
    comparison_service: StationsComparisonServiceV2 | None,
) -> None:
    """Compare observations response with legacy API and log results."""
    if not comparison_service:
        return
    
    try:
        # Only compare for JSON format
        format_param = request.query_params.get("format", "json")
        if format_param != "json":
            logger.debug(f"Skipping comparison for non-JSON format: {format_param}")
            return
        
        params = dict(request.query_params)
        # Ensure we request JSON from legacy
        params["format"] = "json"
        
        result = await comparison_service.compare_observations(
            new_observations, params
        )
        _log_observation_comparison_result(result)
    except Exception as e:
        logger.error(f"Observation comparison failed: {str(e)}", exc_info=True)


def _format_diff_git_style(diff: dict) -> str:
    """Format DeepDiff tree view output in git-style diff format.
    
    Shows the JSON path and actual values for each difference:
    - Added items show: + path: value (in new only)
    - Removed items show: - path: value (in legacy only)
    - Changed values show both old and new values
    """
    lines = []
    
    # Handle dictionary items added (present in new, not in legacy)
    if 'dictionary_item_added' in diff:
        for item in diff['dictionary_item_added']:
            path = item.path()
            value = item.t2  # New value
            lines.append(f"  + {path}: {value}")
    
    # Handle dictionary items removed (present in legacy, not in new)
    if 'dictionary_item_removed' in diff:
        for item in diff['dictionary_item_removed']:
            path = item.path()
            value = item.t1  # Legacy value
            lines.append(f"  - {path}: {value}")
    
    # Handle values changed (different values at same path)
    if 'values_changed' in diff:
        for item in diff['values_changed']:
            path = item.path()
            old_value = item.t1  # Legacy value
            new_value = item.t2  # New value
            lines.append(f"  ~ {path}:")
            lines.append(f"    - legacy: {old_value}")
            lines.append(f"    + new:    {new_value}")
    
    # Handle type changes
    if 'type_changes' in diff:
        for item in diff['type_changes']:
            path = item.path()
            old_value = item.t1
            new_value = item.t2
            old_type = type(old_value).__name__
            new_type = type(new_value).__name__
            lines.append(f"  ~ {path} (type: {old_type} -> {new_type}):")
            lines.append(f"    - legacy: {old_value}")
            lines.append(f"    + new:    {new_value}")
    
    # Handle iterable items added
    if 'iterable_item_added' in diff:
        for item in diff['iterable_item_added']:
            path = item.path()
            value = item.t2
            lines.append(f"  + {path}: {value}")
    
    # Handle iterable items removed
    if 'iterable_item_removed' in diff:
        for item in diff['iterable_item_removed']:
            path = item.path()
            value = item.t1
            lines.append(f"  - {path}: {value}")
    
    return "\n".join(lines) if lines else "  (no differences)"


def _log_comparison_result(result: ComparisonResult) -> None:
    """Log comparison result in compact format with git-style diffs."""
    s = result.summary
    
    msg = (
        f"Stations comparison: "
        f"new={s.total_new}, legacy={s.total_legacy}, "
        f"matched={s.matched_count}, differing={s.differing_count}, "
        f"only_new={s.only_in_new_count}, only_legacy={s.only_in_legacy_count}, "
        f"duration={s.comparison_duration_ms:.1f}ms"
    )
    
    if s.differing_count > 0 or s.only_in_new_count > 0 or s.only_in_legacy_count > 0:
        log_parts = [msg, ""]
        
        _add_differing_items_to_log(
            log_parts,
            result.differing_stations,
            "Differences found:",
            lambda item: f"  [{item['external_id']}]"
        )
        
        _add_only_in_items_to_log(
            log_parts,
            result.only_in_new,
            "\nStations missing from legacy API (present in new only):",
            lambda item: f"  + {item['id']} (platform: {item['platform']})"
        )
        
        _add_only_in_items_to_log(
            log_parts,
            result.only_in_legacy,
            "\nStations missing from new API (present in legacy only):",
            lambda item: f"  - {item['id']} (platform: {item['platform']})"
        )
        
        logger.warning("\n".join(log_parts))
    else:
        logger.info(msg)


def _log_observation_comparison_result(result: ObservationComparisonResult) -> None:
    """Log observation comparison result in compact format with git-style diffs."""
    s = result.summary
    
    msg = (
        f"Observations comparison: "
        f"new={s.total_new}, legacy={s.total_legacy}, "
        f"matched={s.matched_count}, differing={s.differing_count}, "
        f"only_new={s.only_in_new_count}, only_legacy={s.only_in_legacy_count}, "
        f"duration={s.comparison_duration_ms:.1f}ms"
    )
    
    if s.differing_count > 0 or s.only_in_new_count > 0 or s.only_in_legacy_count > 0:
        log_parts = [msg, ""]
        
        _add_differing_items_to_log(
            log_parts,
            result.differing_observations,
            "Differences found in observations:",
            lambda item: (
                f"  [{item['composite_key']['platform']}/"
                f"{item['composite_key']['external_station_id']}/"
                f"{item['composite_key']['datetime']}]"
            )
        )
        
        _add_only_in_items_to_log(
            log_parts,
            result.only_in_new,
            "\nObservations missing from legacy API:",
            lambda item: (
                f"  + {item['platform']}/"
                f"{item['external_station_id']}/"
                f"{item['datetime']}"
            )
        )
        
        _add_only_in_items_to_log(
            log_parts,
            result.only_in_legacy,
            "\nObservations missing from new API:",
            lambda item: (
                f"  - {item['platform']}/"
                f"{item['external_station_id']}/"
                f"{item['datetime']}"
            )
        )
        
        logger.warning("\n".join(log_parts))
    else:
        logger.info(msg)


def _add_differing_items_to_log(
    log_parts: List[str],
    items: List[Dict[str, Any]],
    header: str,
    format_fn
) -> None:
    """Add differing items to log parts.
    
    Args:
        log_parts: List to append log lines to
        items: List of differing items with 'diff' key
        header: Section header
        format_fn: Function to format item identifier
    """
    if not items:
        return
        
    log_parts.append(header)
    for item in items:
        log_parts.append(format_fn(item))
        log_parts.append(_format_diff_git_style(item['diff']))


def _add_only_in_items_to_log(
    log_parts: List[str],
    items: List[Dict[str, str]],
    header: str,
    format_fn
) -> None:
    """Add 'only in' items to log parts.
    
    Args:
        log_parts: List to append log lines to
        items: List of items only in one API
        header: Section header
        format_fn: Function to format item
    """
    if not items:
        return
        
    log_parts.append(header)
    for item in items:
        log_parts.append(format_fn(item))


__all__ = [
    "ComparisonSummary",
    "ComparisonResult",
    "ObservationComparisonSummary",
    "ObservationComparisonResult",
    "StationsComparisonServiceV2",
    "compare_stations_response",
    "compare_observations_response",
]
