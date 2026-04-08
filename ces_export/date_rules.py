from __future__ import annotations

from datetime import date, timedelta

from .models import WindowSpec


def last_day_prev_month(today: date) -> date:
    first = date(today.year, today.month, 1)
    return first - timedelta(days=1)


def halfyear_start(today: date) -> date:
    jun1 = date(today.year, 6, 1)
    return date(today.year, 1, 1) if today < jun1 else jun1


def quarter_start(d: date) -> date:
    q_month = ((d.month - 1) // 3) * 3 + 1
    return date(d.year, q_month, 1)


def add_months(d: date, months: int) -> date:
    month0 = d.month - 1 + months
    year = d.year + month0 // 12
    month = month0 % 12 + 1
    first_this = date(d.year, d.month, 1)
    first_next = date(year + (1 if month == 12 else 0), 1 if month == 12 else month + 1, 1)
    last_day = (first_next - timedelta(days=1)).day
    return date(year, month, min(d.day, last_day))


def month_start(d: date) -> date:
    return date(d.year, d.month, 1)


def month_end(d: date) -> date:
    return add_months(month_start(d), 1) - timedelta(days=1)


def previous_quarter_range(today: date) -> tuple[date, date]:
    q0 = quarter_start(today)
    prev_q_end = q0 - timedelta(days=1)
    prev_q_start = quarter_start(prev_q_end)
    return prev_q_start, prev_q_end


def daterange_chunks(d0: date, d1: date, chunk_days: int) -> list[tuple[date, date]]:
    if d1 < d0:
        raise ValueError(f"bad range: {d0} -> {d1}")
    if chunk_days <= 0:
        return [(d0, d1)]
    out: list[tuple[date, date]] = []
    cur = d0
    while cur <= d1:
        end = min(d1, cur + timedelta(days=chunk_days - 1))
        out.append((cur, end))
        cur = end + timedelta(days=1)
    return out


def split_range(d_from: date, d_to: date, window: WindowSpec) -> list[tuple[date, date]]:
    if d_to < d_from:
        raise ValueError(f"bad range: {d_from} -> {d_to}")

    if window.mode == "none":
        return [(d_from, d_to)]

    if window.mode == "days":
        return daterange_chunks(d_from, d_to, window.size)

    out: list[tuple[date, date]] = []
    cur = d_from

    if window.mode == "calendar_month":
        while cur <= d_to:
            start = cur
            raw_end = add_months(month_start(cur), window.size) - timedelta(days=1)
            end = min(d_to, raw_end)
            out.append((start, end))
            cur = end + timedelta(days=1)
        return out

    if window.mode == "calendar_quarter":
        months = 3 * max(window.size, 1)
        while cur <= d_to:
            start = cur
            q0 = quarter_start(cur)
            raw_end = add_months(q0, months) - timedelta(days=1)
            end = min(d_to, raw_end)
            out.append((start, end))
            cur = end + timedelta(days=1)
        return out

    if window.mode == "calendar_year":
        years = max(window.size, 1)
        while cur <= d_to:
            start = cur
            raw_end = date(cur.year + years - 1, 12, 31)
            end = min(d_to, raw_end)
            out.append((start, end))
            cur = end + timedelta(days=1)
        return out

    raise ValueError(f"Unsupported window mode: {window.mode}")
