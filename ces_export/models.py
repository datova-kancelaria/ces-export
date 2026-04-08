from __future__ import annotations

from dataclasses import dataclass, field
from datetime import date
from pathlib import Path
from typing import Any, Literal

WindowMode = Literal["none", "days", "calendar_month", "calendar_quarter", "calendar_year"]
MergeStrategy = Literal["concat", "csv_header", "rdfxml_graph", "keep_chunks", "skip_if_chunked"]
ScheduleKind = Literal[
    "snapshot_halfyear_start",
    "previous_year_full",
    "current_year_to_previous_month_end",
    "yearly_archive_with_current_ytd",
    "previous_quarter",
    "quarterly_archive",
    "custom_range",
    "annual_snapshot_on_date",
]


@dataclass(frozen=True)
class Credentials:
    apikey: str
    user: str
    password: str


@dataclass(frozen=True)
class AppSettings:
    od001: str
    od002: str
    od003: str


@dataclass(frozen=True)
class WindowSpec:
    mode: WindowMode = "none"
    size: int = 1


@dataclass(frozen=True)
class FormatSpec:
    enabled: bool = True
    window: WindowSpec = field(default_factory=WindowSpec)
    merge_strategy: MergeStrategy = "concat"
    postprocess: tuple[str, ...] = ()
    keep_chunks: bool = False


@dataclass(frozen=True)
class ScheduleDefaultsSpec:
    touch_mtime_to_range_end: bool = False


@dataclass(frozen=True)
class ScheduleSpec:
    kind: ScheduleKind
    out_dir_template: str = "."
    start_year: int | None = None
    end_year: int | None = None
    date_from: str | None = None
    date_to: str | None = None
    month: int | None = None
    day: int | None = None

    # None = not specified here, fall back to global defaults.schedule
    touch_mtime_to_range_end: bool | None = None


@dataclass(frozen=True)
class DatasetSpec:
    name: str
    out_stem: str | None
    schedules: tuple[ScheduleSpec, ...]
    formats: dict[str, FormatSpec]


@dataclass(frozen=True)
class DefaultsSpec:
    out_dir: Path | None = None
    formats: dict[str, FormatSpec] = field(default_factory=dict)
    schedule: ScheduleDefaultsSpec = field(default_factory=ScheduleDefaultsSpec)


@dataclass(frozen=True)
class AppConfig:
    defaults: DefaultsSpec
    datasets: dict[str, DatasetSpec]


@dataclass(frozen=True)
class ExportJob:
    dataset: str
    fmt: str
    d_from: date
    d_to: date
    out_path: Path
    meta_path: Path
    window: WindowSpec
    merge_strategy: MergeStrategy
    postprocess: tuple[str, ...]
    keep_chunks: bool = False

    # final resolved value after applying defaults + local override
    touch_mtime_to_range_end: bool = False


@dataclass(frozen=True)
class PlannedRange:
    d_from: date
    d_to: date
    context: dict[str, Any]


@dataclass(frozen=True)
class RunResult:
    dataset: str
    fmt: str
    requested_range: tuple[date, date]
    main_output: Path | None
    meta_output: Path | None
    chunk_outputs: tuple[Path, ...]
    merged: bool
    skipped: bool
    message: str
