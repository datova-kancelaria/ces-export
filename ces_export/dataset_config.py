from __future__ import annotations

import json
from pathlib import Path
from typing import Any

from .models import (
    AppConfig,
    DatasetSpec,
    DefaultsSpec,
    FormatSpec,
    ScheduleDefaultsSpec,
    ScheduleSpec,
    WindowSpec,
)


def _parse_window(raw: dict[str, Any] | None) -> WindowSpec:
    if raw is None:
        return WindowSpec()
    mode = raw.get("mode", "none")
    size = int(raw.get("size", 1))
    return WindowSpec(mode=mode, size=size)


def _parse_format(raw: dict[str, Any] | None, *, fallback: FormatSpec | None = None) -> FormatSpec:
    raw = raw or {}
    fallback = fallback or FormatSpec()

    enabled = bool(raw.get("enabled", fallback.enabled))
    window = _parse_window(raw.get("window")) if "window" in raw else fallback.window
    merge_strategy = raw.get("merge_strategy", fallback.merge_strategy)
    postprocess = tuple(raw.get("postprocess", list(fallback.postprocess)))
    keep_chunks = bool(raw.get("keep_chunks", fallback.keep_chunks))
    return FormatSpec(
        enabled=enabled,
        window=window,
        merge_strategy=merge_strategy,
        postprocess=postprocess,
        keep_chunks=keep_chunks,
    )


def _parse_schedule(raw: dict[str, Any]) -> ScheduleSpec:
    kind = raw["kind"]
    return ScheduleSpec(
        kind=kind,
        out_dir_template=raw.get("out_dir_template", "."),
        start_year=raw.get("start_year"),
        end_year=raw.get("end_year"),
        date_from=raw.get("date_from"),
        date_to=raw.get("date_to"),
        month=raw.get("month"),
        day=raw.get("day"),
        touch_mtime_to_range_end=raw.get("touch_mtime_to_range_end"),
    )


def load_config(path: Path) -> AppConfig:
    payload = json.loads(path.read_text(encoding="utf-8"))

    defaults_raw = payload.get("defaults", {})
    default_formats_raw = defaults_raw.get("formats", {})
    default_schedule_raw = defaults_raw.get("schedule", {})

    default_formats = {
        name: _parse_format(fmt_raw)
        for name, fmt_raw in default_formats_raw.items()
    }

    defaults = DefaultsSpec(
        formats=default_formats,
        schedule=ScheduleDefaultsSpec(
            touch_mtime_to_range_end=bool(
                default_schedule_raw.get("touch_mtime_to_range_end", False)
            )
        ),
    )

    datasets_raw = payload.get("datasets", {})
    datasets: dict[str, DatasetSpec] = {}

    for name, ds_raw in datasets_raw.items():
        ds_formats_raw = ds_raw.get("formats", {})
        merged_formats: dict[str, FormatSpec] = {}

        format_names = set(defaults.formats) | set(ds_formats_raw)
        for fmt_name in format_names:
            merged_formats[fmt_name] = _parse_format(
                ds_formats_raw.get(fmt_name),
                fallback=defaults.formats.get(fmt_name, FormatSpec()),
            )

        schedules = tuple(_parse_schedule(s) for s in ds_raw.get("schedules", []))
        if not schedules:
            raise ValueError(f"Dataset {name} has no schedules[]")

        datasets[name] = DatasetSpec(
            name=name,
            out_stem=ds_raw.get("out_stem"),
            schedules=schedules,
            formats=merged_formats,
        )

    return AppConfig(defaults=defaults, datasets=datasets)
