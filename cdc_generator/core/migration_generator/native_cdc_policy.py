"""Native CDC schedule policy normalization for migration generation."""

from __future__ import annotations

from typing import Any, cast

from .data_structures import GenerationResult, NativeCdcPolicySeed
from .service_parsing import get_source_table_config

_DEFAULT_SCHEDULE_PROFILE = "warm"
_DEFAULT_TIER_MODE = "auto"
_DEFAULT_POLL_PRIORITY = 100

_VALID_SCHEDULE_PROFILES = frozenset({"hot", "warm", "cool", "cold"})
_VALID_TIER_MODES = frozenset({"auto", "manual"})

_TIER_DEFAULTS: dict[str, dict[str, int]] = {
    "hot": {
        "base_poll_interval_seconds": 1,
        "min_poll_interval_seconds": 1,
        "max_poll_interval_seconds": 30,
        "max_rows_per_pull": 2000,
        "lease_seconds": 120,
        "jitter_millis": 0,
        "max_backoff_seconds": 60,
    },
    "warm": {
        "base_poll_interval_seconds": 5,
        "min_poll_interval_seconds": 5,
        "max_poll_interval_seconds": 60,
        "max_rows_per_pull": 1000,
        "lease_seconds": 120,
        "jitter_millis": 250,
        "max_backoff_seconds": 300,
    },
    "cool": {
        "base_poll_interval_seconds": 30,
        "min_poll_interval_seconds": 30,
        "max_poll_interval_seconds": 300,
        "max_rows_per_pull": 750,
        "lease_seconds": 120,
        "jitter_millis": 1000,
        "max_backoff_seconds": 300,
    },
    "cold": {
        "base_poll_interval_seconds": 60,
        "min_poll_interval_seconds": 60,
        "max_poll_interval_seconds": 1800,
        "max_rows_per_pull": 500,
        "lease_seconds": 180,
        "jitter_millis": 5000,
        "max_backoff_seconds": 900,
    },
}


def build_native_cdc_policy_seeds(
    sink_tables: dict[str, dict[str, Any]],
    service_config: dict[str, object],
    result: GenerationResult,
) -> list[NativeCdcPolicySeed]:
    """Build normalized native CDC policy seeds for one sink target."""
    seeds: list[NativeCdcPolicySeed] = []
    seen_keys: set[tuple[str, str]] = set()

    for sink_key, sink_cfg in sorted(sink_tables.items()):
        from_ref = sink_cfg.get("from")
        if not isinstance(from_ref, str) or not from_ref:
            continue

        target_parts = sink_key.split(".", 1)
        target_schema_name = target_parts[0] if len(target_parts) > 1 else "public"
        target_table_name = target_parts[-1]
        dedupe_key = (target_schema_name.casefold(), target_table_name.casefold())
        if dedupe_key in seen_keys:
            continue
        seen_keys.add(dedupe_key)

        source_cfg = get_source_table_config(service_config, from_ref)
        seeds.append(
            _build_native_cdc_policy_seed(
                source_key=from_ref,
                target_schema_name=target_schema_name,
                target_table_name=target_table_name,
                source_cfg=source_cfg,
                result=result,
            ),
        )

    return seeds


def _build_native_cdc_policy_seed(
    *,
    source_key: str,
    target_schema_name: str,
    target_table_name: str,
    source_cfg: dict[str, Any],
    result: GenerationResult,
) -> NativeCdcPolicySeed:
    settings = _get_native_cdc_settings(source_key, source_cfg, result)

    enabled = _read_bool(
        settings,
        source_key=source_key,
        field_name="enabled",
        default=True,
        result=result,
    )
    schedule_profile = _read_text(
        settings,
        source_key=source_key,
        field_name="schedule_profile",
        default=_DEFAULT_SCHEDULE_PROFILE,
        result=result,
    ).casefold()
    if schedule_profile not in _VALID_SCHEDULE_PROFILES:
        result.warnings.append(
            f"Source table {source_key}: native_cdc.schedule_profile must be one of "
            + f"{sorted(_VALID_SCHEDULE_PROFILES)}; using {_DEFAULT_SCHEDULE_PROFILE}",
        )
        schedule_profile = _DEFAULT_SCHEDULE_PROFILE

    tier_defaults = _get_tier_defaults(schedule_profile)
    tier_mode = _read_text(
        settings,
        source_key=source_key,
        field_name="tier_mode",
        default=_DEFAULT_TIER_MODE,
        result=result,
    ).casefold()
    if tier_mode not in _VALID_TIER_MODES:
        result.warnings.append(
            f"Source table {source_key}: native_cdc.tier_mode must be one of "
            + f"{sorted(_VALID_TIER_MODES)}; using {_DEFAULT_TIER_MODE}",
        )
        tier_mode = _DEFAULT_TIER_MODE

    manual_schedule_profile = _read_optional_text(
        settings,
        source_key=source_key,
        field_name="manual_schedule_profile",
        result=result,
    )
    if manual_schedule_profile is not None:
        manual_schedule_profile = manual_schedule_profile.casefold()
        if manual_schedule_profile not in _VALID_SCHEDULE_PROFILES:
            result.warnings.append(
                f"Source table {source_key}: native_cdc.manual_schedule_profile must be one of "
                + f"{sorted(_VALID_SCHEDULE_PROFILES)}; ignoring invalid value",
            )
            manual_schedule_profile = None
    if tier_mode == "manual" and manual_schedule_profile is None:
        manual_schedule_profile = schedule_profile

    base_poll_interval_seconds = _read_int(
        settings,
        source_key=source_key,
        field_names=("base_poll_interval_seconds", "poll_interval_seconds"),
        default=tier_defaults["base_poll_interval_seconds"],
        min_value=1,
        result=result,
    )
    min_poll_interval_seconds = _read_int(
        settings,
        source_key=source_key,
        field_names=("min_poll_interval_seconds",),
        default=tier_defaults["min_poll_interval_seconds"],
        min_value=1,
        result=result,
    )
    if min_poll_interval_seconds > base_poll_interval_seconds:
        result.warnings.append(
            f"Source table {source_key}: native_cdc.min_poll_interval_seconds cannot exceed "
            + "the base interval; clamping to base interval",
        )
        min_poll_interval_seconds = base_poll_interval_seconds

    max_poll_interval_seconds = _read_int(
        settings,
        source_key=source_key,
        field_names=("max_poll_interval_seconds",),
        default=tier_defaults["max_poll_interval_seconds"],
        min_value=1,
        result=result,
    )
    if max_poll_interval_seconds < base_poll_interval_seconds:
        result.warnings.append(
            f"Source table {source_key}: native_cdc.max_poll_interval_seconds cannot be smaller "
            + "than the base interval; clamping to base interval",
        )
        max_poll_interval_seconds = base_poll_interval_seconds

    max_rows_per_pull = _read_int(
        settings,
        source_key=source_key,
        field_names=("max_rows_per_pull",),
        default=tier_defaults["max_rows_per_pull"],
        min_value=1,
        result=result,
    )
    lease_seconds = _read_int(
        settings,
        source_key=source_key,
        field_names=("lease_seconds",),
        default=tier_defaults["lease_seconds"],
        min_value=1,
        result=result,
    )
    poll_priority = _read_int(
        settings,
        source_key=source_key,
        field_names=("poll_priority",),
        default=_DEFAULT_POLL_PRIORITY,
        min_value=0,
        result=result,
    )
    jitter_millis = _read_int(
        settings,
        source_key=source_key,
        field_names=("jitter_millis",),
        default=tier_defaults["jitter_millis"],
        min_value=0,
        result=result,
    )
    max_backoff_seconds = _read_int(
        settings,
        source_key=source_key,
        field_names=("max_backoff_seconds",),
        default=tier_defaults["max_backoff_seconds"],
        min_value=1,
        result=result,
    )
    if max_backoff_seconds < base_poll_interval_seconds:
        result.warnings.append(
            f"Source table {source_key}: native_cdc.max_backoff_seconds cannot be smaller "
            + "than the base interval; clamping to base interval",
        )
        max_backoff_seconds = base_poll_interval_seconds

    business_hours_profile_key = _read_optional_text(
        settings,
        source_key=source_key,
        field_name="business_hours_profile_key",
        result=result,
    )

    return NativeCdcPolicySeed(
        logical_table_name=target_table_name,
        target_schema_name=target_schema_name,
        target_table_name=target_table_name,
        enabled=enabled,
        schedule_profile=schedule_profile,
        tier_mode=tier_mode,
        manual_schedule_profile=manual_schedule_profile,
        base_poll_interval_seconds=base_poll_interval_seconds,
        min_poll_interval_seconds=min_poll_interval_seconds,
        max_poll_interval_seconds=max_poll_interval_seconds,
        max_rows_per_pull=max_rows_per_pull,
        lease_seconds=lease_seconds,
        poll_priority=poll_priority,
        jitter_millis=jitter_millis,
        max_backoff_seconds=max_backoff_seconds,
        business_hours_profile_key=business_hours_profile_key,
    )


def _get_native_cdc_settings(
    source_key: str,
    source_cfg: dict[str, Any],
    result: GenerationResult,
) -> dict[str, object]:
    native_cdc_raw = source_cfg.get("native_cdc")
    if native_cdc_raw is None:
        return {}
    if isinstance(native_cdc_raw, dict):
        return cast(dict[str, object], native_cdc_raw)

    result.warnings.append(
        f"Source table {source_key}: native_cdc must be a mapping; using generator defaults",
    )
    return {}


def _get_tier_defaults(schedule_profile: str) -> dict[str, int]:
    return _TIER_DEFAULTS.get(schedule_profile, _TIER_DEFAULTS[_DEFAULT_SCHEDULE_PROFILE])


def _read_bool(
    settings: dict[str, object],
    *,
    source_key: str,
    field_name: str,
    default: bool,
    result: GenerationResult,
) -> bool:
    raw_value = settings.get(field_name)
    if raw_value is None:
        return default
    if isinstance(raw_value, bool):
        return raw_value

    result.warnings.append(
        f"Source table {source_key}: native_cdc.{field_name} must be boolean; using {default}",
    )
    return default


def _read_text(
    settings: dict[str, object],
    *,
    source_key: str,
    field_name: str,
    default: str,
    result: GenerationResult,
) -> str:
    raw_value = settings.get(field_name)
    if raw_value is None:
        return default
    if isinstance(raw_value, str):
        stripped_value = raw_value.strip()
        if stripped_value:
            return stripped_value

    result.warnings.append(
        f"Source table {source_key}: native_cdc.{field_name} must be non-empty text; using {default}",
    )
    return default


def _read_optional_text(
    settings: dict[str, object],
    *,
    source_key: str,
    field_name: str,
    result: GenerationResult,
) -> str | None:
    raw_value = settings.get(field_name)
    if raw_value is None:
        return None
    if isinstance(raw_value, str):
        stripped_value = raw_value.strip()
        return stripped_value or None

    result.warnings.append(
        f"Source table {source_key}: native_cdc.{field_name} must be text if provided; ignoring invalid value",
    )
    return None


def _read_int(
    settings: dict[str, object],
    *,
    source_key: str,
    field_names: tuple[str, ...],
    default: int,
    min_value: int,
    result: GenerationResult,
) -> int:
    for field_name in field_names:
        if field_name not in settings:
            continue

        raw_value = settings[field_name]
        if isinstance(raw_value, bool) or not isinstance(raw_value, int):
            result.warnings.append(
                f"Source table {source_key}: native_cdc.{field_name} must be an integer >= {min_value}; using {default}",
            )
            return default
        if raw_value < min_value:
            result.warnings.append(
                f"Source table {source_key}: native_cdc.{field_name} must be >= {min_value}; using {default}",
            )
            return default
        return raw_value

    return default
