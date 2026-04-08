from __future__ import annotations

from datetime import date
from pathlib import Path

from .date_rules import halfyear_start, last_day_prev_month, previous_quarter_range
from .models import AppConfig, ExportJob, PlannedRange, ScheduleSpec


def render_out_dir(base_out_dir: Path, template: str, context: dict[str, object]) -> Path:
    rendered = template.format(**context)
    if rendered in {"", "."}:
        return base_out_dir
    return base_out_dir / rendered


def _schedule_ranges(today: date, schedule: ScheduleSpec) -> list[PlannedRange]:
    kind = schedule.kind

    if kind == "snapshot_halfyear_start":
        d = halfyear_start(today)
        return [PlannedRange(d, d, {"year": d.year, "quarter": ((d.month - 1) // 3) + 1})]

    if kind == "previous_year_full":
        y = today.year - 1
        return [PlannedRange(date(y, 1, 1), date(y, 12, 31), {"year": y})]

    if kind == "current_year_to_previous_month_end":
        end = last_day_prev_month(today)
        if end.year != today.year:
            return []
        return [PlannedRange(date(today.year, 1, 1), end, {"year": today.year})]

    if kind == "yearly_archive_with_current_ytd":
        start_year = schedule.start_year
        if start_year is None:
            raise ValueError("yearly_archive_with_current_ytd requires start_year")
        end_year = schedule.end_year or today.year
        out: list[PlannedRange] = []

        for year in range(start_year, end_year + 1):
            y0 = date(year, 1, 1)
            if year < today.year:
                y1 = date(year, 12, 31)
            else:
                y1 = last_day_prev_month(today)
                if y1.year != today.year:
                    continue
            if y1 >= y0:
                out.append(PlannedRange(y0, y1, {"year": year}))
        return out

    if kind == "previous_quarter":
        d0, d1 = previous_quarter_range(today)
        return [PlannedRange(d0, d1, {"year": d0.year, "quarter": ((d0.month - 1) // 3) + 1})]

    if kind == "quarterly_archive":
        start_year = schedule.start_year
        if start_year is None:
            raise ValueError("quarterly_archive requires start_year")
        end_year = schedule.end_year or today.year
        out: list[PlannedRange] = []

        for year in range(start_year, end_year + 1):
            for q in range(1, 5):
                d0 = date(year, 3 * (q - 1) + 1, 1)
                if q == 4:
                    d1 = date(year, 12, 31)
                else:
                    from datetime import timedelta
                    d1 = date(year, 3 * (q - 1) + 4, 1) - timedelta(days=1)

                if year == today.year:
                    cutoff = last_day_prev_month(today)
                    if cutoff.year != today.year or d0 > cutoff:
                        continue
                    d1 = min(d1, cutoff)

                out.append(PlannedRange(d0, d1, {"year": year, "quarter": q}))
        return out

    if kind == "annual_snapshot_on_date":
        if schedule.start_year is None or schedule.month is None or schedule.day is None:
            raise ValueError("annual_snapshot_on_date requires start_year, month, and day")

        end_year = schedule.end_year or today.year
        out: list[PlannedRange] = []

        for year in range(schedule.start_year, end_year + 1):
            d = date(year, schedule.month, schedule.day)

            # past years always included
            # current year only if that date has already passed
            if year < today.year or d <= today:
                out.append(PlannedRange(d, d, {"year": year}))

        return out

    if kind == "custom_range":
        if not schedule.date_from or not schedule.date_to:
            raise ValueError("custom_range requires date_from and date_to")
        d0 = date.fromisoformat(schedule.date_from)
        d1 = date.fromisoformat(schedule.date_to)
        return [PlannedRange(d0, d1, {"year": d0.year})]

    raise ValueError(f"Unsupported schedule kind: {kind}")


def build_jobs(
    config: AppConfig,
    *,
    today: date,
    start_year_override: int | None = None,
    end_year_override: int | None = None,
    include_datasets: set[str] | None = None,
    exclude_datasets: set[str] | None = None,
) -> list[ExportJob]:
    jobs: list[ExportJob] = []
    include_datasets = include_datasets or set()
    exclude_datasets = exclude_datasets or set()

    for dataset_name, ds in config.datasets.items():
        if include_datasets and dataset_name not in include_datasets:
            continue
        if dataset_name in exclude_datasets:
            continue

        for schedule in ds.schedules:
            effective_schedule = schedule
            if start_year_override is not None or end_year_override is not None:
                effective_schedule = ScheduleSpec(
                    kind=schedule.kind,
                    out_dir_template=schedule.out_dir_template,
                    start_year=start_year_override if start_year_override is not None else schedule.start_year,
                    end_year=end_year_override if end_year_override is not None else schedule.end_year,
                    date_from=schedule.date_from,
                    date_to=schedule.date_to,
                    month=schedule.month,
                    day=schedule.day,
                    touch_mtime_to_range_end=schedule.touch_mtime_to_range_end,
                )

            for pr in _schedule_ranges(today, effective_schedule):
                out_dir = render_out_dir(config.defaults.out_dir, effective_schedule.out_dir_template, pr.context)
                stem = ds.out_stem or dataset_name

                for fmt, fmt_spec in ds.formats.items():
                    if not fmt_spec.enabled:
                        continue

                    out_path = out_dir / f"{stem}.{fmt}"
                    meta_path = out_dir / f"{stem}.{fmt}.meta.json"

                    effective_touch_mtime = (
                        effective_schedule.touch_mtime_to_range_end
                        if effective_schedule.touch_mtime_to_range_end is not None
                        else config.defaults.schedule.touch_mtime_to_range_end
                    )

                    jobs.append(
                        ExportJob(
                            dataset=dataset_name,
                            fmt=fmt,
                            d_from=pr.d_from,
                            d_to=pr.d_to,
                            out_path=out_path,
                            meta_path=meta_path,
                            window=fmt_spec.window,
                            merge_strategy=fmt_spec.merge_strategy,
                            postprocess=fmt_spec.postprocess,
                            keep_chunks=fmt_spec.keep_chunks,
                            touch_mtime_to_range_end=effective_touch_mtime,
                        )
                    )
    return jobs
