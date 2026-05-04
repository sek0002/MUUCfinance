from __future__ import annotations

import argparse
import html
import io
import os
import re
import shutil
import uuid
from datetime import date
from pathlib import Path
from typing import Any, Optional
from urllib.parse import urlencode

import pandas as pd
import pyotp
import uvicorn
from fastapi import FastAPI, Form, HTTPException, Query, Request, UploadFile
from fastapi.responses import FileResponse, HTMLResponse, JSONResponse, RedirectResponse, StreamingResponse
from fastapi.staticfiles import StaticFiles
from fastapi.templating import Jinja2Templates
from starlette.middleware.sessions import SessionMiddleware

from muuc_finance_core import (
    APP_NAME,
    EXPENSE_CATEGORIES,
    INCOME_CATEGORIES,
    AnalysisBundle,
    RUNTIME_DIR,
    SETTINGS_DIR,
    SOURCE_FILENAMES,
    currency,
    filter_frame,
    latest_entry_label,
    load_rule_table,
    load_analysis,
    period_range,
    save_rule_table,
    summarize_categories,
)


BASE_DIR = Path(__file__).resolve().parent
WEB_DIR = BASE_DIR / "webapp"
TEMPLATES_DIR = WEB_DIR / "templates"
STATIC_DIR = WEB_DIR / "static"
SOURCE_KEYS = ("stripe", "teamapp", "everyday")
RULE_FILE_MAP = {
    "income": ("income_rules.csv", INCOME_CATEGORIES),
    "expense": ("expense_rules.csv", EXPENSE_CATEGORIES),
}
WEB_DATA_DIR = Path(os.getenv("MUUC_WEB_DATA_DIR", str(SETTINGS_DIR / "web")))
WEB_SOURCE_DIR = WEB_DATA_DIR / "source"
WEB_RULES_DIR = WEB_DATA_DIR / "config"
WEB_SESSION_DIR = WEB_DATA_DIR / "sessions"
PERIOD_OPTIONS = [
    "All Dates",
    "Custom",
    "Month To Date",
    "Year To Date",
    "Financial Year To Date",
    "Last 30 Days",
    "Current Month",
    "Current Year",
    "Selected Year",
    "Current Financial Year",
]
PURCHASE_PREFIX_RE = re.compile(r"^MUUC\s+(?:Ticketing\s+)?Purchase\s+Id:\s*\d+\s*-\s*", flags=re.IGNORECASE)
DEMO_PIN = "6882"

app = FastAPI(title=f"{APP_NAME} Web")
app.add_middleware(
    SessionMiddleware,
    secret_key=os.getenv("MUUC_SESSION_SECRET", "change-me-in-production"),
    same_site="lax",
    https_only=False,
)
app.mount("/static", StaticFiles(directory=str(STATIC_DIR)), name="static")
templates = Jinja2Templates(directory=str(TEMPLATES_DIR))
MANIFEST_PATH = STATIC_DIR / "manifest.webmanifest"
SERVICE_WORKER_PATH = STATIC_DIR / "sw.js"
ASSET_VERSION_PATHS = [
    TEMPLATES_DIR / "base.html",
    TEMPLATES_DIR / "dashboard.html",
    TEMPLATES_DIR / "files.html",
    TEMPLATES_DIR / "rules.html",
    TEMPLATES_DIR / "login.html",
    STATIC_DIR / "app.css",
    STATIC_DIR / "logo.png",
    STATIC_DIR / "sw.js",
    STATIC_DIR / "manifest.webmanifest",
    STATIC_DIR / "icons" / "favicon-32.png",
    STATIC_DIR / "icons" / "apple-touch-icon.png",
    STATIC_DIR / "icons" / "icon-192.png",
    STATIC_DIR / "icons" / "icon-512.png",
]
ASSET_VERSION = str(
    int(
        max(path.stat().st_mtime for path in ASSET_VERSION_PATHS if path.exists())
    )
)


def auth_config() -> dict[str, str]:
    return {
        "username": os.getenv("MUUC_WEB_USERNAME", "admin"),
        "totp_secret": os.getenv("MUUC_TOTP_SECRET", ""),
        "issuer": os.getenv("MUUC_TOTP_ISSUER", APP_NAME),
    }


def auth_config_error() -> Optional[str]:
    cfg = auth_config()
    if not cfg["totp_secret"]:
        return "Missing auth configuration: MUUC_TOTP_SECRET"
    return None


def verify_totp(code: str) -> bool:
    secret = auth_config()["totp_secret"]
    if not secret:
        return False
    try:
        return pyotp.TOTP(secret).verify(code.strip(), valid_window=1)
    except Exception:
        return False


def ensure_web_source_dir() -> Path:
    WEB_SOURCE_DIR.mkdir(parents=True, exist_ok=True)
    return WEB_SOURCE_DIR


def is_demo_session(request: Request) -> bool:
    return request.session.get("session_mode") == "demo"


def ensure_demo_session(request: Request) -> Path:
    session_id = request.session.get("demo_session_id")
    if not session_id:
        session_id = uuid.uuid4().hex
        request.session["demo_session_id"] = session_id
    root = WEB_SESSION_DIR / session_id
    source_dir = root / "source"
    config_dir = root / "config"
    source_dir.mkdir(parents=True, exist_ok=True)
    config_dir.mkdir(parents=True, exist_ok=True)

    primary_sources = current_source_paths(None)
    primary_income_rules, primary_expense_rules = current_rule_paths(None)
    for key, path in primary_sources.items():
        target = source_dir / SOURCE_FILENAMES[key]
        if not target.exists() and path.exists():
            shutil.copyfile(path, target)
    for filename, source_path in {
        "income_rules.csv": primary_income_rules,
        "expense_rules.csv": primary_expense_rules,
    }.items():
        target = config_dir / filename
        if not target.exists() and source_path.exists():
            shutil.copyfile(source_path, target)
    return root


def current_source_paths(request: Optional[Request] = None) -> dict[str, Path]:
    if request is not None and is_demo_session(request):
        source_dir = ensure_demo_session(request) / "source"
    else:
        source_dir = ensure_web_source_dir()
    return {key: source_dir / SOURCE_FILENAMES[key] for key in SOURCE_KEYS}


def ensure_web_rule_file(filename: str) -> Path:
    WEB_RULES_DIR.mkdir(parents=True, exist_ok=True)
    destination = WEB_RULES_DIR / filename
    bundled_path = RUNTIME_DIR / "config" / filename
    if not destination.exists() and bundled_path.exists():
        destination.write_bytes(bundled_path.read_bytes())
    return destination


def current_rule_paths(request: Optional[Request] = None) -> tuple[Path, Path]:
    if request is not None and is_demo_session(request):
        root = ensure_demo_session(request) / "config"
        return root / "income_rules.csv", root / "expense_rules.csv"
    return ensure_web_rule_file("income_rules.csv"), ensure_web_rule_file("expense_rules.csv")


def empty_income_frame() -> pd.DataFrame:
    return pd.DataFrame(
        columns=["date", "description", "category", "matched", "subgroup", "amount", "source", "reference", "refunded_amount", "name", "email"]
    )


def empty_expense_frame() -> pd.DataFrame:
    return pd.DataFrame(
        columns=["date", "description", "category", "matched", "subgroup", "amount", "source", "reference", "name", "email"]
    )


def empty_bundle() -> AnalysisBundle:
    return AnalysisBundle(
        income=empty_income_frame(),
        expenses=empty_expense_frame(),
        misc_income=empty_income_frame(),
        misc_expenses=empty_expense_frame(),
    )


def missing_source_keys(request: Optional[Request] = None) -> list[str]:
    paths = current_source_paths(request)
    return [key for key in SOURCE_KEYS if not paths[key].exists()]


def load_bundle(request: Optional[Request] = None) -> AnalysisBundle:
    source_paths = current_source_paths(request)
    income_rules_path, expense_rules_path = current_rule_paths(request)
    return load_analysis(
        source_paths["stripe"],
        source_paths["teamapp"],
        source_paths["everyday"],
        income_rules_path,
        expense_rules_path,
    )


def load_bundle_safe(request: Optional[Request] = None) -> tuple[AnalysisBundle, list[str]]:
    missing = missing_source_keys(request)
    if missing:
        return empty_bundle(), missing
    try:
        return load_bundle(request), []
    except FileNotFoundError:
        return empty_bundle(), missing_source_keys(request)


def merge_csv_bytes(existing_path: Path, uploaded_bytes: bytes) -> tuple[int, int]:
    uploaded_df = pd.read_csv(io.BytesIO(uploaded_bytes))
    if not existing_path.exists():
        existing_path.parent.mkdir(parents=True, exist_ok=True)
        uploaded_df.to_csv(existing_path, index=False)
        return len(uploaded_df.index), 0

    existing_df = pd.read_csv(existing_path)
    all_columns = list(dict.fromkeys(list(existing_df.columns) + list(uploaded_df.columns)))
    existing_aligned = existing_df.reindex(columns=all_columns)
    uploaded_aligned = uploaded_df.reindex(columns=all_columns)
    combined = pd.concat([existing_aligned, uploaded_aligned], ignore_index=True, sort=False)
    dedupe_keys = combined.fillna("").astype(str)
    unique_mask = ~dedupe_keys.duplicated(keep="first")
    merged = combined.loc[unique_mask].copy()
    added_rows = int(len(merged.index) - len(existing_aligned.index))
    skipped_rows = int(len(uploaded_aligned.index) - added_rows)
    merged.to_csv(existing_path, index=False)
    return added_rows, skipped_rows


def preview_csv(path: Path, rows: int = 12) -> dict[str, Any]:
    if not path.exists():
        return {"headers": [], "rows": [], "row_count": 0}
    try:
        df = pd.read_csv(path).fillna("")
    except Exception:
        return {"headers": [], "rows": [], "row_count": 0}
    preview = df.head(rows)
    return {
        "headers": [str(column) for column in preview.columns.tolist()],
        "rows": [[str(value) for value in row] for row in preview.values.tolist()],
        "row_count": int(len(df.index)),
    }


def preview_frame_page(path: Path, offset: int, limit: int) -> dict[str, Any]:
    if not path.exists():
        return {"headers": [], "rows": [], "row_count": 0, "next_offset": None}
    try:
        df = pd.read_csv(path).fillna("")
    except Exception:
        return {"headers": [], "rows": [], "row_count": 0, "next_offset": None}
    offset = max(offset, 0)
    limit = max(min(limit, 100), 1)
    page = df.iloc[offset : offset + limit]
    next_offset = offset + len(page.index)
    return {
        "headers": [str(column) for column in df.columns.tolist()],
        "rows": [[str(value) for value in row] for row in page.values.tolist()],
        "row_count": int(len(df.index)),
        "next_offset": next_offset if next_offset < len(df.index) else None,
    }


def editable_rule_table(rule_key: str, request: Optional[Request] = None) -> dict[str, Any]:
    filename, categories = RULE_FILE_MAP[rule_key]
    path = current_rule_paths(request)[0] if rule_key == "income" else current_rule_paths(request)[1]
    df = load_rule_table(path, categories)
    row_count = max(len(df.index), 8)
    padded = df.reindex(range(row_count), fill_value="")
    rows = []
    for row_index in range(row_count):
        rows.append(
            {
                "index": row_index,
                "cells": [str(padded.iloc[row_index][category]) for category in categories],
            }
        )
    return {
        "key": rule_key,
        "label": "Income Rules" if rule_key == "income" else "Expense Rules",
        "columns": categories,
        "rows": rows,
    }


VIEW_OPTIONS = ["All", "Income", "Expenses", "Income Misc", "Expense Misc"]


def transaction_view_frame(bundle: AnalysisBundle, view: str) -> pd.DataFrame:
    if view == "Income":
        return bundle.income.copy()
    if view == "Expenses":
        return bundle.expenses.copy()
    if view == "Income Misc":
        return bundle.misc_income.copy()
    if view == "Expense Misc":
        return bundle.misc_expenses.copy()
    combined = pd.concat([bundle.income, bundle.expenses], ignore_index=True, sort=False)
    if "date" in combined.columns:
        return combined.sort_values("date", ascending=False)
    return combined


def require_auth(request: Request) -> Optional[RedirectResponse]:
    if not request.session.get("authenticated"):
        return RedirectResponse(url="/login", status_code=303)
    return None


def requested_categories(categories: list[str]) -> list[str]:
    allowed = set(INCOME_CATEGORIES + EXPENSE_CATEGORIES)
    return [category for category in categories if category in allowed]


WINDOW_OPTIONS = ["day", "week", "month", "year"]


def dashboard_base_params(
    period: str,
    start_text: str,
    end_text: str,
    selected_year: int,
    graph_mode: str,
    chart_style: str,
    line_categories: list[str],
    window_scale: int,
    pie_categories: list[str],
) -> list[tuple[str, str]]:
    params = [
        ("period", period),
        ("start", start_text),
        ("end", end_text),
        ("selected_year", str(selected_year)),
        ("graph_mode", graph_mode),
        ("chart_style", chart_style),
        ("window_scale", str(window_scale)),
    ]
    params.extend([("line_category", category) for category in line_categories])
    params.extend([("pie_categories", category) for category in pie_categories])
    return params


def dashboard_url(
    period: str,
    start_text: str,
    end_text: str,
    selected_year: int,
    graph_mode: str,
    chart_style: str,
    line_categories: list[str],
    window_scale: int,
    pie_categories: list[str],
) -> str:
    params = dashboard_base_params(period, start_text, end_text, selected_year, graph_mode, chart_style, line_categories, window_scale, pie_categories)
    return f"/dashboard?{urlencode(params)}"


def strip_purchase_prefix(value: str) -> str:
    return PURCHASE_PREFIX_RE.sub("", value or "")


def frame_for_view(
    bundle: AnalysisBundle,
    income: pd.DataFrame,
    expenses: pd.DataFrame,
    view: str,
    start: Optional[date],
    end: Optional[date],
) -> pd.DataFrame:
    if view == "Income":
        return income.copy()
    if view == "Expenses":
        return expenses.copy()
    if view == "Income Misc":
        return filter_frame(bundle.misc_income, start, end)
    if view == "Expense Misc":
        return filter_frame(bundle.misc_expenses, start, end)
    combined = pd.concat([income, expenses], ignore_index=True, sort=False)
    if "date" in combined.columns:
        return combined.sort_values("date")
    return combined


def apply_focus(frame: pd.DataFrame, category: str) -> pd.DataFrame:
    if frame.empty or not category or category == "all" or "category" not in frame.columns:
        return frame.copy()
    return frame[frame["category"] == category].copy()


def aggregate_series(frame: pd.DataFrame, window_key: str) -> pd.Series:
    if frame.empty:
        return pd.Series(dtype="float64")
    out = frame.copy()
    dates = pd.to_datetime(out["date"], errors="coerce")
    if window_key == "day":
        out["bucket"] = dates.dt.strftime("%Y-%m-%d")
    elif window_key == "week":
        out["bucket"] = dates.dt.to_period("W").astype(str)
    elif window_key == "year":
        out["bucket"] = dates.dt.strftime("%Y")
    else:
        out["bucket"] = dates.dt.to_period("M").astype(str)
    out = out[out["bucket"].notna()].copy()
    if out.empty:
        return pd.Series(dtype="float64")
    return out.groupby("bucket")["amount"].sum().sort_index()


def format_bucket_label(bucket: str, window_key: str) -> str:
    try:
        if window_key == "day":
            return pd.to_datetime(bucket, errors="raise").strftime("%d-%m-%Y")
        if window_key == "week":
            start_text = str(bucket).split("/")[0]
            return pd.to_datetime(start_text, errors="raise").strftime("%d-%m-%Y")
        if window_key == "month":
            return pd.Period(bucket, freq="M").start_time.strftime("%b %Y")
        if window_key == "year":
            dt = pd.to_datetime(f"{bucket}-01-01", errors="raise")
            return dt.strftime("%d-%m-%Y")
    except Exception:
        return str(bucket)
    return str(bucket)


def bucket_year_label(bucket: str, window_key: str) -> str:
    try:
        if window_key == "day":
            return pd.to_datetime(bucket, errors="raise").strftime("%Y")
        if window_key == "week":
            start_text = str(bucket).split("/")[0]
            return pd.to_datetime(start_text, errors="raise").strftime("%Y")
        if window_key == "month":
            return str(pd.Period(bucket, freq="M").year)
        if window_key == "year":
            return str(bucket)
    except Exception:
        return str(bucket)[:4]
    return str(bucket)[:4]


def svg_tooltip_script() -> str:
    return """
    <script>
    document.addEventListener('DOMContentLoaded', function () {
      const tip = document.createElement('div');
      tip.className = 'svg-tooltip';
      tip.style.display = 'none';
      document.body.appendChild(tip);
      document.querySelectorAll('[data-tooltip]').forEach(function (node) {
        node.addEventListener('mouseenter', function (event) {
          tip.textContent = node.getAttribute('data-tooltip');
          tip.style.display = 'block';
        });
        node.addEventListener('mousemove', function (event) {
          tip.style.left = (event.pageX + 14) + 'px';
          tip.style.top = (event.pageY - 12) + 'px';
        });
        node.addEventListener('mouseleave', function () {
          tip.style.display = 'none';
        });
      });
    });
    </script>
    """


def build_line_chart_svg(series_map: dict[str, pd.Series], title: str, window_key: str) -> str:
    width = 1160
    height = 360
    margin_left = 64
    margin_right = 32
    margin_top = 42
    margin_bottom = 58
    colors = ["#00a67e", "#db5b7b", "#635bff", "#0ea5e9"]
    active_series = {label: series for label, series in series_map.items() if not series.empty}
    if not active_series:
        return '<div class="chart-empty">No data in the selected range.</div>'

    labels = []
    for series in active_series.values():
        for label in series.index.tolist():
            if label not in labels:
                labels.append(label)
    labels = sorted(labels)
    max_value = max(float(series.max()) for series in active_series.values()) if active_series else 1.0
    max_value = max(max_value, 1.0)
    plot_width = width - margin_left - margin_right
    plot_height = height - margin_top - margin_bottom

    parts = [
        f'<svg class="chart-svg line-svg" viewBox="0 0 {width} {height}" role="img" aria-label="{html.escape(title)}">',
        f'<line x1="{margin_left}" y1="{margin_top}" x2="{margin_left}" y2="{margin_top + plot_height}" class="axis-line" />',
        f'<line x1="{margin_left}" y1="{margin_top + plot_height}" x2="{width - margin_right}" y2="{margin_top + plot_height}" class="axis-line" />',
    ]

    for tick in range(5):
        ratio = tick / 4
        y = margin_top + plot_height - (plot_height * ratio)
        value = max_value * ratio
        parts.append(f'<line x1="{margin_left}" y1="{y:.1f}" x2="{width - margin_right}" y2="{y:.1f}" class="grid-line" />')
        parts.append(f'<text x="{margin_left - 12}" y="{y + 4:.1f}" text-anchor="end" class="axis-label">{html.escape(currency(value))}</text>')

    x_step = plot_width / max(len(labels) - 1, 1)
    for index, label in enumerate(labels):
        x = margin_left + (index * x_step if len(labels) > 1 else plot_width / 2)
        display_label = format_bucket_label(label, window_key)
        parts.append(
            f'<text x="{x:.1f}" y="{height - 14}" text-anchor="end" transform="rotate(-45 {x:.1f} {height - 14})" class="axis-label">{html.escape(display_label)}</text>'
        )

    if window_key != "year" and labels:
        year_groups: list[tuple[str, int, int]] = []
        current_year = bucket_year_label(labels[0], window_key)
        group_start = 0
        for index, label in enumerate(labels[1:], start=1):
            year = bucket_year_label(label, window_key)
            if year != current_year:
                year_groups.append((current_year, group_start, index - 1))
                current_year = year
                group_start = index
        year_groups.append((current_year, group_start, len(labels) - 1))

        inset_y = margin_top + plot_height + 10
        inset_height = 18
        for year, start_index, end_index in year_groups:
            start_x = margin_left + (start_index * x_step if len(labels) > 1 else plot_width / 2)
            end_x = margin_left + (end_index * x_step if len(labels) > 1 else plot_width / 2)
            inset_x = max(margin_left, start_x - 18)
            inset_width = max((end_x - start_x) + 36, 58)
            parts.append(
                f'<rect x="{inset_x:.1f}" y="{inset_y:.1f}" width="{inset_width:.1f}" height="{inset_height}" rx="9" class="year-inset" />'
            )
            parts.append(
                f'<text x="{(inset_x + inset_width / 2):.1f}" y="{inset_y + 12:.1f}" text-anchor="middle" class="year-inset-label">{html.escape(year)}</text>'
            )

    legend_x = width - margin_right - (150 * len(active_series))
    legend_x = max(legend_x, margin_left + 280)
    legend_y = 22
    for idx, (series_name, series) in enumerate(active_series.items()):
        color = colors[idx % len(colors)]
        points = []
        for label_idx, label in enumerate(labels):
            value = float(series.get(label, 0.0))
            x = margin_left + (label_idx * x_step if len(labels) > 1 else plot_width / 2)
            y = margin_top + plot_height - ((value / max_value) * plot_height)
            points.append((x, y, value, label))
        path_data = " ".join([f"{'M' if i == 0 else 'L'} {x:.1f} {y:.1f}" for i, (x, y, _value, _label) in enumerate(points)])
        parts.append(f'<path d="{path_data}" fill="none" stroke="{color}" stroke-width="3" stroke-linecap="round" stroke-linejoin="round" />')
        for x, y, value, label in points:
            tooltip = f"{series_name} | {label} | {currency(value)}"
            parts.append(f'<circle cx="{x:.1f}" cy="{y:.1f}" r="4.5" fill="{color}" data-tooltip="{html.escape(tooltip)}" />')
        parts.append(f'<circle cx="{legend_x}" cy="{legend_y}" r="5" fill="{color}" />')
        parts.append(f'<text x="{legend_x + 12}" y="{legend_y + 4}" class="legend-label">{html.escape(series_name)}</text>')
        legend_x += 150

    parts.append("</svg>")
    return "".join(parts)


def build_bar_chart_svg(series_map: dict[str, pd.Series], title: str, window_key: str) -> str:
    width = 1160
    height = 360
    margin_left = 64
    margin_right = 32
    margin_top = 42
    margin_bottom = 58
    colors = ["#00a67e", "#db5b7b", "#635bff", "#0ea5e9"]
    active_series = {label: series for label, series in series_map.items() if not series.empty}
    if not active_series:
        return '<div class="chart-empty">No data in the selected range.</div>'

    labels = []
    for series in active_series.values():
        for label in series.index.tolist():
            if label not in labels:
                labels.append(label)
    labels = sorted(labels)
    max_value = max(float(series.max()) for series in active_series.values()) if active_series else 1.0
    max_value = max(max_value, 1.0)
    plot_width = width - margin_left - margin_right
    plot_height = height - margin_top - margin_bottom
    group_width = plot_width / max(len(labels), 1)
    inner_width = min(group_width * 0.78, 64)
    series_count = max(len(active_series), 1)
    bar_width = max(inner_width / series_count, 10)

    parts = [
        f'<svg class="chart-svg line-svg" viewBox="0 0 {width} {height}" role="img" aria-label="{html.escape(title)}">',
        f'<line x1="{margin_left}" y1="{margin_top}" x2="{margin_left}" y2="{margin_top + plot_height}" class="axis-line" />',
        f'<line x1="{margin_left}" y1="{margin_top + plot_height}" x2="{width - margin_right}" y2="{margin_top + plot_height}" class="axis-line" />',
    ]

    for tick in range(5):
        ratio = tick / 4
        y = margin_top + plot_height - (plot_height * ratio)
        value = max_value * ratio
        parts.append(f'<line x1="{margin_left}" y1="{y:.1f}" x2="{width - margin_right}" y2="{y:.1f}" class="grid-line" />')
        parts.append(f'<text x="{margin_left - 12}" y="{y + 4:.1f}" text-anchor="end" class="axis-label">{html.escape(currency(value))}</text>')

    for index, label in enumerate(labels):
        group_start = margin_left + (index * group_width)
        center_x = group_start + group_width / 2
        display_label = format_bucket_label(label, window_key)
        parts.append(
            f'<text x="{center_x:.1f}" y="{height - 14}" text-anchor="end" transform="rotate(-45 {center_x:.1f} {height - 14})" class="axis-label">{html.escape(display_label)}</text>'
        )

    if window_key != "year" and labels:
        year_groups: list[tuple[str, int, int]] = []
        current_year = bucket_year_label(labels[0], window_key)
        group_start_index = 0
        for index, label in enumerate(labels[1:], start=1):
            year = bucket_year_label(label, window_key)
            if year != current_year:
                year_groups.append((current_year, group_start_index, index - 1))
                current_year = year
                group_start_index = index
        year_groups.append((current_year, group_start_index, len(labels) - 1))

        inset_y = margin_top + plot_height + 10
        inset_height = 18
        for year, start_index, end_index in year_groups:
            start_x = margin_left + (start_index * group_width)
            end_x = margin_left + ((end_index + 1) * group_width)
            inset_x = max(margin_left, start_x + 2)
            inset_width = max((end_x - start_x) - 4, 58)
            parts.append(f'<rect x="{inset_x:.1f}" y="{inset_y:.1f}" width="{inset_width:.1f}" height="{inset_height}" rx="9" class="year-inset" />')
            parts.append(f'<text x="{(inset_x + inset_width / 2):.1f}" y="{inset_y + 12:.1f}" text-anchor="middle" class="year-inset-label">{html.escape(year)}</text>')

    legend_x = width - margin_right - (150 * len(active_series))
    legend_x = max(legend_x, margin_left + 280)
    legend_y = 22
    for series_idx, (series_name, series) in enumerate(active_series.items()):
        color = colors[series_idx % len(colors)]
        for label_idx, label in enumerate(labels):
            value = float(series.get(label, 0.0))
            group_start = margin_left + (label_idx * group_width)
            bars_total_width = bar_width * series_count
            start_x = group_start + (group_width - bars_total_width) / 2
            x = start_x + (series_idx * bar_width)
            bar_height = 0 if max_value <= 0 else (value / max_value) * plot_height
            y = margin_top + plot_height - bar_height
            tooltip = f"{series_name} | {label} | {currency(value)}"
            parts.append(
                f'<rect x="{x:.1f}" y="{y:.1f}" width="{max(bar_width - 3, 6):.1f}" height="{max(bar_height, 1):.1f}" rx="4" fill="{color}" data-tooltip="{html.escape(tooltip)}" />'
            )
        parts.append(f'<circle cx="{legend_x}" cy="{legend_y}" r="5" fill="{color}" />')
        parts.append(f'<text x="{legend_x + 12}" y="{legend_y + 4}" class="legend-label">{html.escape(series_name)}</text>')
        legend_x += 150

    parts.append("</svg>")
    return "".join(parts)


def build_category_summary_line_chart_svg(income_totals: pd.Series, expense_totals: pd.Series, title: str) -> str:
    width = 1160
    height = 360
    margin_left = 64
    margin_right = 32
    margin_top = 42
    margin_bottom = 58
    colors = {"Income": "#00a67e", "Expenses": "#db5b7b"}
    labels = list(dict.fromkeys(list(income_totals.index) + list(expense_totals.index)))
    if not labels:
        return '<div class="chart-empty">No data in the selected range.</div>'

    max_value = max(
        float(income_totals.max()) if not income_totals.empty else 0.0,
        float(expense_totals.max()) if not expense_totals.empty else 0.0,
        1.0,
    )
    plot_width = width - margin_left - margin_right
    plot_height = height - margin_top - margin_bottom
    x_step = plot_width / max(len(labels) - 1, 1)

    parts = [
        f'<svg class="chart-svg line-svg" viewBox="0 0 {width} {height}" role="img" aria-label="{html.escape(title)}">',
        f'<line x1="{margin_left}" y1="{margin_top}" x2="{margin_left}" y2="{margin_top + plot_height}" class="axis-line" />',
        f'<line x1="{margin_left}" y1="{margin_top + plot_height}" x2="{width - margin_right}" y2="{margin_top + plot_height}" class="axis-line" />',
    ]

    for tick in range(5):
        ratio = tick / 4
        y = margin_top + plot_height - (plot_height * ratio)
        value = max_value * ratio
        parts.append(f'<line x1="{margin_left}" y1="{y:.1f}" x2="{width - margin_right}" y2="{y:.1f}" class="grid-line" />')
        parts.append(f'<text x="{margin_left - 12}" y="{y + 4:.1f}" text-anchor="end" class="axis-label">{html.escape(currency(value))}</text>')

    def add_series(series_name: str, series: pd.Series) -> None:
        color = colors[series_name]
        points = []
        for label_idx, label in enumerate(labels):
            value = float(series.get(label, 0.0))
            x = margin_left + (label_idx * x_step if len(labels) > 1 else plot_width / 2)
            y = margin_top + plot_height - ((value / max_value) * plot_height)
            points.append((x, y, value, label))
        path_data = " ".join([f"{'M' if i == 0 else 'L'} {x:.1f} {y:.1f}" for i, (x, y, _value, _label) in enumerate(points)])
        parts.append(f'<path d="{path_data}" fill="none" stroke="{color}" stroke-width="3" stroke-linecap="round" stroke-linejoin="round" />')
        for x, y, value, label in points:
            tooltip = f"{series_name} | {label} | {currency(value)}"
            parts.append(f'<circle cx="{x:.1f}" cy="{y:.1f}" r="4.5" fill="{color}" data-tooltip="{html.escape(tooltip)}" />')

    for index, label in enumerate(labels):
        x = margin_left + (index * x_step if len(labels) > 1 else plot_width / 2)
        parts.append(
            f'<text x="{x:.1f}" y="{height - 14}" text-anchor="end" transform="rotate(-45 {x:.1f} {height - 14})" class="axis-label">{html.escape(str(label))}</text>'
        )

    add_series("Income", income_totals)
    add_series("Expenses", expense_totals)

    legend_x = width - margin_right - 300
    legend_x = max(legend_x, margin_left + 280)
    legend_y = 22
    for series_name in ["Income", "Expenses"]:
        color = colors[series_name]
        parts.append(f'<circle cx="{legend_x}" cy="{legend_y}" r="5" fill="{color}" />')
        parts.append(f'<text x="{legend_x + 12}" y="{legend_y + 4}" class="legend-label">{html.escape(series_name)}</text>')
        legend_x += 150

    parts.append("</svg>")
    return "".join(parts)


def build_category_stacked_bar_svg(income_totals: pd.Series, expense_totals: pd.Series, title: str) -> str:
    width = 1160
    height = 360
    margin_left = 64
    margin_right = 32
    margin_top = 42
    margin_bottom = 58
    colors = ["#00a67e", "#db5b7b", "#635bff", "#0ea5e9", "#f59e0b", "#a855f7", "#14b8a6", "#ef4444"]

    income_segments = [(label, float(income_totals.get(label, 0.0))) for label in income_totals.index.tolist() if float(income_totals.get(label, 0.0)) > 0]
    expense_segments = [(label, float(expense_totals.get(label, 0.0))) for label in expense_totals.index.tolist() if float(expense_totals.get(label, 0.0)) > 0]
    if not income_segments and not expense_segments:
        return '<div class="chart-empty">No data in the selected range.</div>'

    income_total = sum(value for _label, value in income_segments)
    expense_total = sum(value for _label, value in expense_segments)
    max_value = max(income_total, expense_total, 1.0)
    plot_width = width - margin_left - margin_right
    plot_height = height - margin_top - margin_bottom
    group_labels = ["Income", "Expenses"]
    group_width = plot_width / len(group_labels)
    bar_width = min(group_width * 0.4, 120)

    parts = [
        f'<svg class="chart-svg line-svg" viewBox="0 0 {width} {height}" role="img" aria-label="{html.escape(title)}">',
        f'<line x1="{margin_left}" y1="{margin_top}" x2="{margin_left}" y2="{margin_top + plot_height}" class="axis-line" />',
        f'<line x1="{margin_left}" y1="{margin_top + plot_height}" x2="{width - margin_right}" y2="{margin_top + plot_height}" class="axis-line" />',
    ]

    for tick in range(5):
        ratio = tick / 4
        y = margin_top + plot_height - (plot_height * ratio)
        value = max_value * ratio
        parts.append(f'<line x1="{margin_left}" y1="{y:.1f}" x2="{width - margin_right}" y2="{y:.1f}" class="grid-line" />')
        parts.append(f'<text x="{margin_left - 12}" y="{y + 4:.1f}" text-anchor="end" class="axis-label">{html.escape(currency(value))}</text>')

    def draw_stack(group_index: int, group_name: str, segments: list[tuple[str, float]]) -> None:
        center_x = margin_left + (group_width * group_index) + (group_width / 2)
        x = center_x - (bar_width / 2)
        running_height = 0.0
        for idx, (label, value) in enumerate(segments):
            segment_height = 0 if max_value <= 0 else (value / max_value) * plot_height
            y = margin_top + plot_height - running_height - segment_height
            color = colors[idx % len(colors)]
            tooltip = f"{group_name} | {label} | {currency(value)}"
            parts.append(
                f'<rect x="{x:.1f}" y="{y:.1f}" width="{bar_width:.1f}" height="{max(segment_height, 1):.1f}" rx="6" fill="{color}" data-tooltip="{html.escape(tooltip)}" />'
            )
            running_height += segment_height
        parts.append(f'<text x="{center_x:.1f}" y="{height - 14}" text-anchor="middle" class="axis-label">{html.escape(group_name)}</text>')

    draw_stack(0, "Income", income_segments)
    draw_stack(1, "Expenses", expense_segments)

    legend_x = width - margin_right - 180
    legend_x = max(legend_x, margin_left + 280)
    legend_y = 18
    legend_step = 20
    legend_items = income_segments if income_segments else expense_segments
    for idx, (label, _value) in enumerate(legend_items):
        color = colors[idx % len(colors)]
        row_y = legend_y + (idx * legend_step)
        parts.append(f'<rect x="{legend_x}" y="{row_y - 8}" width="10" height="10" rx="2" fill="{color}" />')
        parts.append(f'<text x="{legend_x + 16}" y="{row_y + 1}" class="legend-label">{html.escape(label)}</text>')

    parts.append("</svg>")
    return "".join(parts)


def build_time_stacked_category_bar_svg(
    income_series_map: dict[str, pd.Series],
    expense_series_map: dict[str, pd.Series],
    title: str,
    window_key: str,
) -> str:
    width = 1160
    height = 360
    margin_left = 64
    margin_right = 32
    margin_top = 42
    margin_bottom = 58
    colors = ["#00a67e", "#db5b7b", "#635bff", "#0ea5e9", "#f59e0b", "#a855f7", "#14b8a6", "#ef4444"]

    labels = []
    for series in list(income_series_map.values()) + list(expense_series_map.values()):
        for label in series.index.tolist():
            if label not in labels:
                labels.append(label)
    labels = sorted(labels)
    if not labels:
        return '<div class="chart-empty">No data in the selected range.</div>'

    income_totals = {label: sum(float(series.get(label, 0.0)) for series in income_series_map.values()) for label in labels}
    expense_totals = {label: sum(float(series.get(label, 0.0)) for series in expense_series_map.values()) for label in labels}
    max_value = max(
        max(income_totals.values()) if income_totals else 0.0,
        max(expense_totals.values()) if expense_totals else 0.0,
        1.0,
    )
    plot_width = width - margin_left - margin_right
    plot_height = height - margin_top - margin_bottom
    group_width = plot_width / max(len(labels), 1)
    bar_width = min(group_width * 0.26, 34)
    pair_gap = min(group_width * 0.1, 10)

    parts = [
        f'<svg class="chart-svg line-svg" viewBox="0 0 {width} {height}" role="img" aria-label="{html.escape(title)}">',
        f'<line x1="{margin_left}" y1="{margin_top}" x2="{margin_left}" y2="{margin_top + plot_height}" class="axis-line" />',
        f'<line x1="{margin_left}" y1="{margin_top + plot_height}" x2="{width - margin_right}" y2="{margin_top + plot_height}" class="axis-line" />',
    ]

    for tick in range(5):
        ratio = tick / 4
        y = margin_top + plot_height - (plot_height * ratio)
        value = max_value * ratio
        parts.append(f'<line x1="{margin_left}" y1="{y:.1f}" x2="{width - margin_right}" y2="{y:.1f}" class="grid-line" />')
        parts.append(f'<text x="{margin_left - 12}" y="{y + 4:.1f}" text-anchor="end" class="axis-label">{html.escape(currency(value))}</text>')

    for index, label in enumerate(labels):
        group_start = margin_left + (index * group_width)
        center_x = group_start + group_width / 2
        display_label = format_bucket_label(label, window_key)
        parts.append(
            f'<text x="{center_x:.1f}" y="{height - 14}" text-anchor="end" transform="rotate(-45 {center_x:.1f} {height - 14})" class="axis-label">{html.escape(display_label)}</text>'
        )

    if window_key != "year" and labels:
        year_groups: list[tuple[str, int, int]] = []
        current_year = bucket_year_label(labels[0], window_key)
        group_start_index = 0
        for index, label in enumerate(labels[1:], start=1):
            year = bucket_year_label(label, window_key)
            if year != current_year:
                year_groups.append((current_year, group_start_index, index - 1))
                current_year = year
                group_start_index = index
        year_groups.append((current_year, group_start_index, len(labels) - 1))

        inset_y = margin_top + plot_height + 10
        inset_height = 18
        for year, start_index, end_index in year_groups:
            start_x = margin_left + (start_index * group_width)
            end_x = margin_left + ((end_index + 1) * group_width)
            inset_x = max(margin_left, start_x + 2)
            inset_width = max((end_x - start_x) - 4, 58)
            parts.append(f'<rect x="{inset_x:.1f}" y="{inset_y:.1f}" width="{inset_width:.1f}" height="{inset_height}" rx="9" class="year-inset" />')
            parts.append(f'<text x="{(inset_x + inset_width / 2):.1f}" y="{inset_y + 12:.1f}" text-anchor="middle" class="year-inset-label">{html.escape(year)}</text>')

    def category_name(series_name: str) -> str:
        return series_name.split("·", 1)[1].strip() if "·" in series_name else series_name

    category_order: list[str] = []
    for series_name in list(income_series_map.keys()) + list(expense_series_map.keys()):
        category = category_name(series_name)
        if category not in category_order:
            category_order.append(category)
    color_map = {category: colors[idx % len(colors)] for idx, category in enumerate(category_order)}

    income_items = [(category_name(name), series) for name, series in income_series_map.items()]
    expense_items = [(category_name(name), series) for name, series in expense_series_map.items()]

    for label_index, label in enumerate(labels):
        group_start = margin_left + (label_index * group_width)
        center_x = group_start + group_width / 2
        income_x = center_x - bar_width - (pair_gap / 2)
        expense_x = center_x + (pair_gap / 2)

        running_height = 0.0
        for series_name, series in income_items:
            value = float(series.get(label, 0.0))
            if value <= 0:
                continue
            segment_height = (value / max_value) * plot_height
            y = margin_top + plot_height - running_height - segment_height
            color = color_map.get(series_name, colors[0])
            tooltip = f"{series_name} | {label} | {currency(value)}"
            parts.append(
                f'<rect x="{income_x:.1f}" y="{y:.1f}" width="{bar_width:.1f}" height="{max(segment_height, 1):.1f}" rx="4" fill="{color}" data-tooltip="{html.escape(tooltip)}" />'
            )
            running_height += segment_height

        running_height = 0.0
        for series_name, series in expense_items:
            value = float(series.get(label, 0.0))
            if value <= 0:
                continue
            segment_height = (value / max_value) * plot_height
            y = margin_top + plot_height - running_height - segment_height
            color = color_map.get(series_name, colors[0])
            tooltip = f"{series_name} | {label} | {currency(value)}"
            parts.append(
                f'<rect x="{expense_x:.1f}" y="{y:.1f}" width="{bar_width:.1f}" height="{max(segment_height, 1):.1f}" rx="4" fill="{color}" data-tooltip="{html.escape(tooltip)}" />'
            )
            running_height += segment_height

        parts.append(f'<text x="{income_x + (bar_width / 2):.1f}" y="{margin_top + plot_height - 6:.1f}" text-anchor="middle" class="axis-label">I</text>')
        parts.append(f'<text x="{expense_x + (bar_width / 2):.1f}" y="{margin_top + plot_height - 6:.1f}" text-anchor="middle" class="axis-label">E</text>')

    legend_x = width - margin_right - 220
    legend_x = max(legend_x, margin_left + 280)
    legend_y = 18
    legend_step = 18
    legend_items = [(category, color_map[category]) for category in category_order]
    for idx, (label, color) in enumerate(legend_items):
        row_y = legend_y + (idx * legend_step)
        parts.append(f'<rect x="{legend_x}" y="{row_y - 8}" width="10" height="10" rx="2" fill="{color}" />')
        parts.append(f'<text x="{legend_x + 16}" y="{row_y + 1}" class="legend-label">{html.escape(label)}</text>')

    parts.append("</svg>")
    return "".join(parts)


def pie_arc(cx: float, cy: float, radius: float, start_angle: float, end_angle: float) -> str:
    import math

    start_x = cx + radius * math.cos(start_angle)
    start_y = cy + radius * math.sin(start_angle)
    end_x = cx + radius * math.cos(end_angle)
    end_y = cy + radius * math.sin(end_angle)
    large_arc = 1 if end_angle - start_angle > math.pi else 0
    return f"M {cx:.1f} {cy:.1f} L {start_x:.1f} {start_y:.1f} A {radius:.1f} {radius:.1f} 0 {large_arc} 1 {end_x:.1f} {end_y:.1f} Z"


def build_pie_svg(income_series: pd.Series, expense_series: pd.Series) -> str:
    import math

    width = 1240
    legend_count = max(len([value for value in income_series.values if float(value) > 0]), len([value for value in expense_series.values if float(value) > 0]), 1)
    height = max(420, 92 + (legend_count * 24))
    colors = ["#00a67e", "#3b82f6", "#635bff", "#f59e0b", "#db5b7b", "#0ea5e9", "#a855f7", "#14b8a6", "#64748b"]
    sections = [("Income", income_series, 56), ("Expenses", expense_series, 650)]
    parts = [f'<svg class="chart-svg pie-svg" viewBox="0 0 {width} {height}" role="img" aria-label="Income and expense pie charts">']
    for title, series, panel_x in sections:
        total = float(series.sum()) if not series.empty else 0.0
        center_x = panel_x + 150
        legend_x = panel_x + 330
        center_y = min(max(height / 2, 186), 236)
        radius = 122
        parts.append(f'<text x="{panel_x}" y="30" class="chart-title">{html.escape(title)}</text>')
        if total <= 0:
            parts.append(f'<text x="{panel_x}" y="64" class="axis-label">No data</text>')
            continue
        start = -math.pi / 2
        for idx, (category, amount) in enumerate(series.items()):
            if float(amount) <= 0:
                continue
            sweep = (float(amount) / total) * math.tau
            end = start + sweep
            color = colors[idx % len(colors)]
            tooltip = f"{title} | {category} | {currency(float(amount))} ({(float(amount)/total)*100:.1f}%)"
            path = pie_arc(center_x, center_y, radius, start, end)
            parts.append(f'<path d="{path}" fill="{color}" stroke="#ffffff" stroke-width="2" data-tooltip="{html.escape(tooltip)}" />')
            start = end
        legend_y = 58
        visible_items = [(category, amount) for category, amount in series.items() if float(amount) > 0]
        for idx, (category, amount) in enumerate(visible_items):
            if float(amount) <= 0:
                continue
            color = colors[idx % len(colors)]
            tooltip = f"{title} | {category} | {currency(float(amount))}"
            y = legend_y + idx * 24
            parts.append(f'<rect x="{legend_x}" y="{y}" width="12" height="12" rx="3" fill="{color}" data-tooltip="{html.escape(tooltip)}" />')
            parts.append(f'<text x="{legend_x + 18}" y="{y + 10}" class="legend-label">{html.escape(category)} {html.escape(currency(float(amount)))}</text>')
    parts.append("</svg>")
    return "".join(parts)


def category_rows(series: pd.Series) -> list[dict[str, Any]]:
    if series.empty:
        return []
    max_amount = max(float(series.max()), 1.0)
    total = float(series.sum())
    rows: list[dict[str, Any]] = []
    for category, amount in series.items():
        amount_value = float(amount)
        rows.append(
            {
                "category": category,
                "amount": amount_value,
                "amount_label": currency(amount_value),
                "width_pct": 0 if max_amount <= 0 else (amount_value / max_amount) * 100,
                "share_pct": 0 if total <= 0 else (amount_value / total) * 100,
            }
        )
    return rows


def transaction_rows(frame: pd.DataFrame, include_contact: bool) -> list[dict[str, Any]]:
    if frame.empty:
        return []
    ordered = frame.sort_values(["date", "category", "amount"], ascending=[False, True, False]).head(250)
    rows: list[dict[str, Any]] = []
    for _, row in ordered.iterrows():
        dt = pd.to_datetime(row.get("date"), errors="coerce")
        rows.append(
            {
                "date": "" if pd.isna(dt) else dt.strftime("%Y-%m-%d"),
                "category": row.get("category", ""),
                "subgroup": row.get("subgroup", "") or "Unmatched",
                "identifier": strip_purchase_prefix(str(row.get("description") or row.get("reference") or "")),
                "name": row.get("name", "") if include_contact else "",
                "email": row.get("email", "") if include_contact else "",
                "amount": currency(float(row.get("amount", 0.0))),
                "source": row.get("source", ""),
                "matched": "Yes" if bool(row.get("matched")) else "No",
            }
        )
    return rows


def category_subgroup_rows(income: pd.DataFrame, expenses: pd.DataFrame) -> list[dict[str, Any]]:
    combined = pd.concat([income, expenses], ignore_index=True, sort=False)
    if combined.empty:
        return []
    summary = (
        combined.assign(subgroup=combined.get("subgroup", "").fillna("").replace("", "Unmatched"))
        .groupby(["category", "subgroup"], dropna=False)
        .agg(transaction_count=("amount", "size"), total_amount=("amount", "sum"))
        .reset_index()
        .sort_values(["category", "total_amount"], ascending=[True, False])
    )
    rows: list[dict[str, Any]] = []
    for _, row in summary.iterrows():
        rows.append(
            {
                "category": str(row["category"]),
                "subgroup": str(row["subgroup"]),
                "transaction_count": int(row["transaction_count"]),
                "total_amount": currency(float(row["total_amount"])),
            }
        )
    return rows


def chart_totals_detail_rows(income: pd.DataFrame, expenses: pd.DataFrame, window_key: str) -> list[dict[str, Any]]:
    income_series = aggregate_series(income, window_key)
    expense_series = aggregate_series(expenses, window_key)
    labels = list(dict.fromkeys(list(income_series.index) + list(expense_series.index)))
    labels = sorted(labels)
    rows: list[dict[str, Any]] = []
    for label in labels:
        income_value = float(income_series.get(label, 0.0))
        expense_value = float(expense_series.get(label, 0.0))
        rows.append(
            {
                "period": format_bucket_label(label, window_key),
                "income": currency(income_value),
                "expenses": currency(expense_value),
                "net": currency(income_value - expense_value),
                "_sort_period": label,
                "_sort_income": income_value,
                "_sort_expenses": expense_value,
                "_sort_net": income_value - expense_value,
            }
        )
    return rows


def chart_category_detail_rows(income: pd.DataFrame, expenses: pd.DataFrame, selected_categories: list[str]) -> list[dict[str, Any]]:
    allowed = requested_categories(selected_categories)
    income_allowed = [category for category in allowed if category in INCOME_CATEGORIES]
    expense_allowed = [category for category in allowed if category in EXPENSE_CATEGORIES]
    income_frame = income[income["category"].isin(income_allowed)].copy() if income_allowed else pd.DataFrame(columns=income.columns)
    expense_frame = expenses[expenses["category"].isin(expense_allowed)].copy() if expense_allowed else pd.DataFrame(columns=expenses.columns)
    rows: list[dict[str, Any]] = []
    combined = pd.concat([income_frame, expense_frame], ignore_index=True, sort=False)
    if combined.empty:
        return rows
    ordered = combined.sort_values(["date", "category", "amount"], ascending=[False, True, False]).head(300)
    for _, row in ordered.iterrows():
        dt = pd.to_datetime(row.get("date"), errors="coerce")
        amount_value = float(row.get("amount", 0.0))
        rows.append(
            {
                "date": "" if pd.isna(dt) else dt.strftime("%Y-%m-%d"),
                "flow": "Income" if row.get("category", "") in INCOME_CATEGORIES else "Expense",
                "category": row.get("category", ""),
                "line_item": strip_purchase_prefix(str(row.get("description") or row.get("reference") or "")),
                "amount": currency(amount_value),
                "_sort_date": "" if pd.isna(dt) else dt.strftime("%Y-%m-%d"),
                "_sort_flow": "0" if row.get("category", "") in INCOME_CATEGORIES else "1",
                "_sort_category": str(row.get("category", "")),
                "_sort_line_item": strip_purchase_prefix(str(row.get("description") or row.get("reference") or "")),
                "_sort_amount": amount_value,
            }
        )
    return rows


def monthly_rows(income: pd.DataFrame, expenses: pd.DataFrame) -> list[dict[str, Any]]:
    if not income.empty:
        income = income.copy()
        income["month"] = pd.to_datetime(income["date"], errors="coerce").dt.to_period("M").astype(str)
        income_monthly = income.groupby("month")["amount"].sum().sort_index()
    else:
        income_monthly = pd.Series(dtype="float64")

    if not expenses.empty:
        expenses = expenses.copy()
        expenses["month"] = pd.to_datetime(expenses["date"], errors="coerce").dt.to_period("M").astype(str)
        expense_monthly = expenses.groupby("month")["amount"].sum().sort_index()
    else:
        expense_monthly = pd.Series(dtype="float64")

    labels = list(dict.fromkeys(list(income_monthly.index) + list(expense_monthly.index)))
    max_value = max(
        float(income_monthly.max()) if not income_monthly.empty else 0.0,
        float(expense_monthly.max()) if not expense_monthly.empty else 0.0,
        1.0,
    )

    rows: list[dict[str, Any]] = []
    for label in labels:
        income_value = float(income_monthly.get(label, 0.0))
        expense_value = float(expense_monthly.get(label, 0.0))
        rows.append(
            {
                "month": label,
                "income": currency(income_value),
                "expenses": currency(expense_value),
                "income_width": (income_value / max_value) * 100,
                "expense_width": (expense_value / max_value) * 100,
            }
        )
    return rows


def source_rows(request: Optional[Request] = None) -> list[dict[str, Any]]:
    paths = current_source_paths(request)
    labels = {
        "stripe": "Stripe CSV",
        "teamapp": "TeamApp CSV",
        "everyday": "Everyday CSV",
    }
    return [
        {
            "key": key,
            "label": labels[key],
            "path": str(paths[key]),
            "exists": "1" if paths[key].exists() else "",
            "latest": latest_entry_label(key, paths[key]),
            "preview": preview_csv(paths[key]),
        }
        for key in SOURCE_KEYS
    ]


def rule_rows(request: Optional[Request] = None) -> list[dict[str, Any]]:
    income_rules_path, expense_rules_path = current_rule_paths(request)
    mapping = {
        "income": ("Income Rules", income_rules_path),
        "expense": ("Expense Rules", expense_rules_path),
    }
    rows: list[dict[str, str]] = []
    for key, (label, path) in mapping.items():
        df = load_rule_table(path, RULE_FILE_MAP[key][1])
        rows.append(
            {
                "key": key,
                "label": label,
                "path": str(path),
                "rows": str(len(df.index)),
                "preview": preview_csv(path),
            }
        )
    return rows


def base_template_context(request: Request) -> dict[str, Any]:
    return {
        "request": request,
        "app_name": APP_NAME,
        "auth_error": auth_config_error(),
        "asset_version": ASSET_VERSION,
        "session_mode": request.session.get("session_mode", "admin"),
        "is_demo_session": is_demo_session(request),
    }


def dashboard_context(
    request: Request,
    period: str,
    start_text: str,
    end_text: str,
    selected_year: int,
    graph_mode: str,
    chart_style: str,
    line_category: list[str],
    window_scale: int,
    pie_categories: list[str],
    message: Optional[str],
) -> dict[str, Any]:
    bundle, missing_sources = load_bundle_safe(request)
    start, end = period_range(period, start_text, end_text, selected_year)
    filtered_income = filter_frame(bundle.income, start, end)
    filtered_expenses = filter_frame(bundle.expenses, start, end)
    all_categories = list(dict.fromkeys(INCOME_CATEGORIES + EXPENSE_CATEGORIES))
    graph_mode_value = graph_mode if graph_mode in {"totals", "category", "pie"} else "totals"
    chart_style_value = chart_style if chart_style in {"line", "bar"} else "line"
    line_selected = requested_categories(line_category)
    if not line_selected and all_categories:
        line_selected = [all_categories[0]]
    window_scale_value = window_scale if 0 <= window_scale < len(WINDOW_OPTIONS) else 2
    window_key = WINDOW_OPTIONS[window_scale_value]
    pie_selected = [category for category in pie_categories if category in all_categories]
    if not pie_selected:
        pie_selected = all_categories[:]

    income_summary = summarize_categories(filtered_income, INCOME_CATEGORIES, [])
    expense_summary = summarize_categories(filtered_expenses, EXPENSE_CATEGORIES, [])
    recent_transactions = frame_for_view(bundle, filtered_income, filtered_expenses, "All", start, end)

    chart_title = "Total Income & Total Expenses"
    chart_detail_title: Optional[str] = None
    chart_detail_columns: list[dict[str, str]] = []
    chart_detail_rows: list[dict[str, Any]] = []
    if graph_mode_value == "totals":
        totals_series = {
            "Income": aggregate_series(filtered_income, window_key),
            "Expenses": aggregate_series(filtered_expenses, window_key),
        }
        chart_svg = build_bar_chart_svg(totals_series, chart_title, window_key) if chart_style_value == "bar" else build_line_chart_svg(totals_series, chart_title, window_key)
        chart_detail_title = "Data Points"
        chart_detail_columns = [
            {"key": "period", "label": "Period", "sort_key": "_sort_period", "numeric": ""},
            {"key": "income", "label": "Income", "sort_key": "_sort_income", "numeric": "1"},
            {"key": "expenses", "label": "Expenses", "sort_key": "_sort_expenses", "numeric": "1"},
            {"key": "net", "label": "Net", "sort_key": "_sort_net", "numeric": "1"},
        ]
        chart_detail_rows = chart_totals_detail_rows(filtered_income, filtered_expenses, window_key)
    elif graph_mode_value == "category":
        chart_title = "Income & Expenses by Category"
        income_series_map: dict[str, pd.Series] = {}
        expense_series_map: dict[str, pd.Series] = {}
        for category in line_selected:
            if category in INCOME_CATEGORIES:
                income_series_map[f"Income · {category}"] = aggregate_series(apply_focus(filtered_income, category), window_key)
            if category in EXPENSE_CATEGORIES:
                expense_series_map[f"Expenses · {category}"] = aggregate_series(apply_focus(filtered_expenses, category), window_key)
        if chart_style_value == "bar":
            chart_svg = build_time_stacked_category_bar_svg(income_series_map, expense_series_map, chart_title, window_key)
        else:
            category_series = {**income_series_map, **expense_series_map}
            chart_svg = build_line_chart_svg(category_series, chart_title, window_key)
        chart_detail_title = "Line Items"
        chart_detail_columns = [
            {"key": "date", "label": "Date", "sort_key": "_sort_date", "numeric": ""},
            {"key": "flow", "label": "Flow", "sort_key": "_sort_flow", "numeric": ""},
            {"key": "category", "label": "Category", "sort_key": "_sort_category", "numeric": ""},
            {"key": "line_item", "label": "Line Item", "sort_key": "_sort_line_item", "numeric": ""},
            {"key": "amount", "label": "Amount", "sort_key": "_sort_amount", "numeric": "1"},
        ]
        chart_detail_rows = chart_category_detail_rows(filtered_income, filtered_expenses, line_selected)
    else:
        chart_title = "Pie Breakdown"
        pie_income = summarize_categories(filtered_income[filtered_income["category"].isin(pie_selected)], INCOME_CATEGORIES, [])
        pie_expense = summarize_categories(filtered_expenses[filtered_expenses["category"].isin(pie_selected)], EXPENSE_CATEGORIES, [])
        chart_svg = build_pie_svg(pie_income, pie_expense)

    export_query = urlencode(dashboard_base_params(period, start_text, end_text, selected_year, graph_mode_value, chart_style_value, line_selected, window_scale_value, pie_selected))

    return {
        **base_template_context(request),
        "message": message,
        "active_page": "dashboard",
        "source_rows": source_rows(request),
        "period": period,
        "period_options": PERIOD_OPTIONS,
        "start": start_text,
        "end": end_text,
        "selected_year": selected_year,
        "show_custom_dates": period == "Custom",
        "missing_sources": missing_sources,
        "show_upload_prompt": bool(missing_sources),
        "income_total": currency(float(filtered_income["amount"].sum())) if not filtered_income.empty else currency(0.0),
        "expense_total": currency(float(filtered_expenses["amount"].sum())) if not filtered_expenses.empty else currency(0.0),
        "net_total": currency(float(filtered_income["amount"].sum()) - float(filtered_expenses["amount"].sum())),
        "net_is_positive": (float(filtered_income["amount"].sum()) - float(filtered_expenses["amount"].sum())) >= 0,
        "misc_count": len(filter_frame(bundle.misc_income, start, end)) + len(filter_frame(bundle.misc_expenses, start, end)),
        "income_rows": category_rows(income_summary),
        "expense_rows": category_rows(expense_summary),
        "category_subgroup_rows": category_subgroup_rows(filtered_income, filtered_expenses),
        "recent_transactions": transaction_rows(recent_transactions, include_contact=True)[:24],
        "export_query": export_query,
        "graph_mode": graph_mode_value,
        "chart_style": chart_style_value,
        "graph_mode_links": {
            mode: dashboard_url(period, start_text, end_text, selected_year, mode, chart_style_value, line_selected, window_scale_value, pie_selected)
            for mode in ["totals", "category", "pie"]
        },
        "line_categories_selected": set(line_selected),
        "all_categories": all_categories,
        "pie_categories_selected": set(pie_selected),
        "window_scale": window_scale_value,
        "window_key": window_key,
        "window_label": WINDOW_OPTIONS[window_scale_value].title(),
        "chart_title": chart_title,
        "show_chart_detail_table": graph_mode_value != "pie",
        "chart_detail_title": chart_detail_title,
        "chart_detail_columns": chart_detail_columns,
        "chart_detail_rows": chart_detail_rows,
        "chart_svg": chart_svg,
        "chart_tooltip_script": svg_tooltip_script(),
    }


def files_context(request: Request, message: Optional[str]) -> dict[str, Any]:
    return {
        **base_template_context(request),
        "message": message,
        "active_page": "files",
        "source_rows": source_rows(request),
        "rule_rows": rule_rows(request),
        "missing_sources": missing_source_keys(request),
    }


def rules_context(request: Request, message: Optional[str], view: str = "All") -> dict[str, Any]:
    bundle, _missing_sources = load_bundle_safe(request)
    active_view = view if view in VIEW_OPTIONS else "All"
    return {
        **base_template_context(request),
        "message": message,
        "active_page": "rules",
        "income_rule_table": editable_rule_table("income", request),
        "expense_rule_table": editable_rule_table("expense", request),
        "view_options": VIEW_OPTIONS,
        "active_view": active_view,
        "review_rows": transaction_rows(transaction_view_frame(bundle, active_view), include_contact=True),
    }


@app.get("/", response_class=HTMLResponse)
def home(request: Request) -> RedirectResponse:
    if request.session.get("authenticated"):
        return RedirectResponse(url="/dashboard", status_code=303)
    return RedirectResponse(url="/login", status_code=303)


@app.get("/manifest.webmanifest")
def manifest_file() -> FileResponse:
    return FileResponse(MANIFEST_PATH, media_type="application/manifest+json")


@app.get("/sw.js")
def service_worker() -> FileResponse:
    return FileResponse(SERVICE_WORKER_PATH, media_type="application/javascript", headers={"Service-Worker-Allowed": "/"})


@app.get("/login", response_class=HTMLResponse)
def login_page(request: Request, message: Optional[str] = None) -> HTMLResponse:
    return templates.TemplateResponse(
        "login.html",
        {
            **base_template_context(request),
            "login_error": bool(message),
            "login_page": True,
            "message": "Invalid login code." if message else None,
        },
    )


@app.post("/login/admin")
async def admin_login(
    request: Request,
    totp_code: str = Form(...),
) -> RedirectResponse:
    config_error = auth_config_error()
    if config_error:
        return RedirectResponse(url=f"/login?message={config_error}", status_code=303)

    if not verify_totp(totp_code):
        return RedirectResponse(url="/login?message=1", status_code=303)

    request.session["authenticated"] = True
    request.session["username"] = auth_config()["username"]
    request.session["session_mode"] = "admin"
    request.session.pop("demo_session_id", None)
    return RedirectResponse(url="/dashboard", status_code=303)


@app.post("/login/pin")
async def pin_login(
    request: Request,
    pin_code: str = Form(...),
) -> RedirectResponse:
    if pin_code.strip() != DEMO_PIN:
        return RedirectResponse(url="/login?message=1", status_code=303)

    request.session["authenticated"] = True
    request.session["username"] = "demo"
    request.session["session_mode"] = "demo"
    request.session.pop("demo_session_id", None)
    ensure_demo_session(request)
    return RedirectResponse(url="/dashboard", status_code=303)


@app.post("/logout")
def logout(request: Request) -> RedirectResponse:
    request.session.clear()
    return RedirectResponse(url="/login", status_code=303)


@app.get("/dashboard", response_class=HTMLResponse)
def dashboard(
    request: Request,
    period: str = Query("All Dates"),
    start: str = Query(""),
    end: str = Query(""),
    selected_year: int = Query(date.today().year),
    graph_mode: str = Query("totals"),
    chart_style: str = Query("line"),
    line_category: list[str] = Query(default=[]),
    window_scale: int = Query(2),
    pie_categories: list[str] = Query(default=[]),
    message: Optional[str] = Query(None),
) -> HTMLResponse:
    auth_redirect = require_auth(request)
    if auth_redirect:
        return auth_redirect
    context = dashboard_context(
        request,
        period,
        start,
        end,
        selected_year,
        graph_mode,
        chart_style,
        line_category,
        window_scale,
        pie_categories,
        message,
    )
    return templates.TemplateResponse("dashboard.html", context)


@app.get("/files", response_class=HTMLResponse)
def files_page(request: Request, message: Optional[str] = Query(None)) -> HTMLResponse:
    auth_redirect = require_auth(request)
    if auth_redirect:
        return auth_redirect
    return templates.TemplateResponse("files.html", files_context(request, message))


@app.get("/rules", response_class=HTMLResponse)
def rules_page(request: Request, message: Optional[str] = Query(None), view: str = Query("All")) -> HTMLResponse:
    auth_redirect = require_auth(request)
    if auth_redirect:
        return auth_redirect
    return templates.TemplateResponse("rules.html", rules_context(request, message, view))


@app.post("/upload/{source_key}")
async def upload_source(request: Request, source_key: str, file: UploadFile) -> RedirectResponse:
    auth_redirect = require_auth(request)
    if auth_redirect:
        return auth_redirect
    if source_key not in SOURCE_KEYS:
        raise HTTPException(status_code=404, detail="Unknown source key")
    if not file.filename or not file.filename.lower().endswith(".csv"):
        return RedirectResponse(url="/files?message=Please upload a CSV file.", status_code=303)

    destination = current_source_paths(request)[source_key]
    contents = await file.read()
    try:
        added_rows, skipped_rows = merge_csv_bytes(destination, contents)
    except Exception as exc:
        return RedirectResponse(url=f"/files?message=Failed to merge uploaded CSV: {exc}", status_code=303)
    return RedirectResponse(
        url=f"/files?message=Updated {source_key} source file. Added {added_rows} new rows, skipped {skipped_rows} overlapping rows.",
        status_code=303,
    )


@app.get("/download/source/{source_key}")
def download_source(request: Request, source_key: str) -> StreamingResponse:
    auth_redirect = require_auth(request)
    if auth_redirect:
        return auth_redirect
    if source_key not in SOURCE_KEYS:
        raise HTTPException(status_code=404, detail="Unknown source key")
    path = current_source_paths(request)[source_key]
    if not path.exists():
        raise HTTPException(status_code=404, detail="File not found")
    return StreamingResponse(
        io.BytesIO(path.read_bytes()),
        media_type="text/csv",
        headers={"Content-Disposition": f'attachment; filename="{path.name}"'},
    )


@app.post("/upload/rules/{rule_key}")
async def upload_rules(request: Request, rule_key: str, file: UploadFile) -> RedirectResponse:
    auth_redirect = require_auth(request)
    if auth_redirect:
        return auth_redirect
    if rule_key not in RULE_FILE_MAP:
        raise HTTPException(status_code=404, detail="Unknown rule key")
    if not file.filename or not file.filename.lower().endswith(".csv"):
        return RedirectResponse(url="/files?message=Please upload a CSV file.", status_code=303)

    filename, categories = RULE_FILE_MAP[rule_key]
    destination = current_rule_paths(request)[0] if rule_key == "income" else current_rule_paths(request)[1]
    contents = await file.read()
    destination.write_bytes(contents)
    try:
        validated = load_rule_table(destination, categories)
        save_rule_table(destination, validated, categories)
    except Exception as exc:
        return RedirectResponse(url=f"/files?message=Failed to load uploaded rules: {exc}", status_code=303)
    return RedirectResponse(url=f"/files?message=Updated {rule_key} rules file.", status_code=303)


@app.post("/rules/save/{rule_key}")
async def save_rules_table(request: Request, rule_key: str) -> JSONResponse:
    auth_redirect = require_auth(request)
    if auth_redirect:
        return JSONResponse({"ok": False, "error": "Unauthorized"}, status_code=401)
    if rule_key not in RULE_FILE_MAP:
        raise HTTPException(status_code=404, detail="Unknown rule key")

    payload = await request.json()
    columns = payload.get("columns", [])
    rows = payload.get("rows", [])
    filename, expected_categories = RULE_FILE_MAP[rule_key]
    if columns != expected_categories:
        return JSONResponse({"ok": False, "error": "Rule columns do not match expected categories."}, status_code=400)

    frame = pd.DataFrame(rows, columns=expected_categories).fillna("").astype(str)
    destination = current_rule_paths(request)[0] if rule_key == "income" else current_rule_paths(request)[1]
    save_rule_table(destination, frame, expected_categories)
    return JSONResponse({"ok": True, "saved_rows": int(len(frame.index))})


@app.get("/download/rules/{rule_key}")
def download_rules(request: Request, rule_key: str) -> StreamingResponse:
    auth_redirect = require_auth(request)
    if auth_redirect:
        return auth_redirect
    if rule_key not in RULE_FILE_MAP:
        raise HTTPException(status_code=404, detail="Unknown rule key")
    filename, _categories = RULE_FILE_MAP[rule_key]
    path = current_rule_paths(request)[0] if rule_key == "income" else current_rule_paths(request)[1]
    if not path.exists():
        raise HTTPException(status_code=404, detail="File not found")
    return StreamingResponse(
        io.BytesIO(path.read_bytes()),
        media_type="text/csv",
        headers={"Content-Disposition": f'attachment; filename="{path.name}"'},
    )


@app.get("/preview/{kind}/{item_key}")
def preview_rows(
    request: Request,
    kind: str,
    item_key: str,
    offset: int = Query(0),
    limit: int = Query(20),
) -> JSONResponse:
    auth_redirect = require_auth(request)
    if auth_redirect:
        return JSONResponse({"ok": False, "error": "Unauthorized"}, status_code=401)

    if kind == "source":
        if item_key not in SOURCE_KEYS:
            raise HTTPException(status_code=404, detail="Unknown source key")
        path = current_source_paths(request)[item_key]
    elif kind == "rules":
        if item_key not in RULE_FILE_MAP:
            raise HTTPException(status_code=404, detail="Unknown rule key")
        path = current_rule_paths(request)[0] if item_key == "income" else current_rule_paths(request)[1]
    else:
        raise HTTPException(status_code=404, detail="Unknown preview kind")

    payload = preview_frame_page(path, offset, limit)
    return JSONResponse({"ok": True, **payload})


@app.get("/transactions/export")
def export_transactions(
    request: Request,
    period: str = Query("All Dates"),
    start: str = Query(""),
    end: str = Query(""),
    selected_year: int = Query(date.today().year),
    graph_mode: str = Query("totals"),
    chart_style: str = Query("line"),
    line_category: list[str] = Query(default=[]),
    window_scale: int = Query(2),
    pie_categories: list[str] = Query(default=[]),
) -> StreamingResponse:
    auth_redirect = require_auth(request)
    if auth_redirect:
        return auth_redirect
    bundle, _missing_sources = load_bundle_safe(request)
    start_date, end_date = period_range(period, start, end, selected_year)
    filtered_income = filter_frame(bundle.income, start_date, end_date)
    filtered_expenses = filter_frame(bundle.expenses, start_date, end_date)
    if graph_mode == "category" and line_category:
        allowed = requested_categories(line_category)
        income_frame = filtered_income[filtered_income["category"].isin([category for category in allowed if category in INCOME_CATEGORIES])]
        expense_frame = filtered_expenses[filtered_expenses["category"].isin([category for category in allowed if category in EXPENSE_CATEGORIES])]
    elif graph_mode == "pie" and pie_categories:
        allowed = requested_categories(pie_categories)
        income_frame = filtered_income[filtered_income["category"].isin(allowed)]
        expense_frame = filtered_expenses[filtered_expenses["category"].isin(allowed)]
    else:
        income_frame = filtered_income
        expense_frame = filtered_expenses
    frame = frame_for_view(bundle, income_frame, expense_frame, "All", start_date, end_date).copy()
    if not frame.empty:
        frame["date"] = pd.to_datetime(frame["date"], errors="coerce").dt.strftime("%Y-%m-%d").fillna("")
    csv_bytes = frame.to_csv(index=False).encode("utf-8")
    return StreamingResponse(
        io.BytesIO(csv_bytes),
        media_type="text/csv",
        headers={"Content-Disposition": 'attachment; filename="transactions_export.csv"'},
    )


def init_auth_secret() -> int:
    secret = pyotp.random_base32()
    username = os.getenv("MUUC_WEB_USERNAME", "admin")
    issuer = os.getenv("MUUC_TOTP_ISSUER", APP_NAME)
    uri = pyotp.TOTP(secret).provisioning_uri(name=username, issuer_name=issuer)
    print(f"MUUC_WEB_USERNAME={username}")
    print(f"MUUC_TOTP_SECRET={secret}")
    print("MUUC_SESSION_SECRET=choose-a-long-random-session-secret")
    print()
    print("Authenticator URI:")
    print(uri)
    return 0


def main() -> None:
    parser = argparse.ArgumentParser(description="MUUC Finance Analyzer web app")
    parser.add_argument("--host", default="127.0.0.1")
    parser.add_argument("--port", type=int, default=8000)
    parser.add_argument("--reload", action="store_true")
    parser.add_argument("--init-auth", action="store_true", help="Print a fresh TOTP secret and provisioning URI.")
    args = parser.parse_args()

    if args.init_auth:
        raise SystemExit(init_auth_secret())

    uvicorn.run("muuc_finance_web:app", host=args.host, port=args.port, reload=args.reload)


if __name__ == "__main__":
    main()
