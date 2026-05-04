from __future__ import annotations

import argparse
import html
import io
import os
import re
from datetime import date
from pathlib import Path
from typing import Any, Optional
from urllib.parse import urlencode

import pandas as pd
import pyotp
import uvicorn
from fastapi import FastAPI, Form, HTTPException, Query, Request, UploadFile
from fastapi.responses import FileResponse, HTMLResponse, RedirectResponse, StreamingResponse
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


def current_source_paths() -> dict[str, Path]:
    source_dir = ensure_web_source_dir()
    return {key: source_dir / SOURCE_FILENAMES[key] for key in SOURCE_KEYS}


def ensure_web_rule_file(filename: str) -> Path:
    WEB_RULES_DIR.mkdir(parents=True, exist_ok=True)
    destination = WEB_RULES_DIR / filename
    bundled_path = RUNTIME_DIR / "config" / filename
    if not destination.exists() and bundled_path.exists():
        destination.write_bytes(bundled_path.read_bytes())
    return destination


def current_rule_paths() -> tuple[Path, Path]:
    return ensure_web_rule_file("income_rules.csv"), ensure_web_rule_file("expense_rules.csv")


def empty_income_frame() -> pd.DataFrame:
    return pd.DataFrame(
        columns=["date", "description", "category", "matched", "amount", "source", "reference", "refunded_amount", "name", "email"]
    )


def empty_expense_frame() -> pd.DataFrame:
    return pd.DataFrame(
        columns=["date", "description", "category", "matched", "amount", "source", "reference", "name", "email"]
    )


def empty_bundle() -> AnalysisBundle:
    return AnalysisBundle(
        income=empty_income_frame(),
        expenses=empty_expense_frame(),
        misc_income=empty_income_frame(),
        misc_expenses=empty_expense_frame(),
    )


def missing_source_keys() -> list[str]:
    paths = current_source_paths()
    return [key for key in SOURCE_KEYS if not paths[key].exists()]


def load_bundle() -> AnalysisBundle:
    source_paths = current_source_paths()
    income_rules_path, expense_rules_path = current_rule_paths()
    return load_analysis(
        source_paths["stripe"],
        source_paths["teamapp"],
        source_paths["everyday"],
        income_rules_path,
        expense_rules_path,
    )


def load_bundle_safe() -> tuple[AnalysisBundle, list[str]]:
    missing = missing_source_keys()
    if missing:
        return empty_bundle(), missing
    try:
        return load_bundle(), []
    except FileNotFoundError:
        return empty_bundle(), missing_source_keys()


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
    line_category: str,
    window_scale: int,
    pie_categories: list[str],
) -> list[tuple[str, str]]:
    params = [
        ("period", period),
        ("start", start_text),
        ("end", end_text),
        ("selected_year", str(selected_year)),
        ("graph_mode", graph_mode),
        ("line_category", line_category),
        ("window_scale", str(window_scale)),
    ]
    params.extend([("pie_categories", category) for category in pie_categories])
    return params


def dashboard_url(
    period: str,
    start_text: str,
    end_text: str,
    selected_year: int,
    graph_mode: str,
    line_category: str,
    window_scale: int,
    pie_categories: list[str],
) -> str:
    params = dashboard_base_params(period, start_text, end_text, selected_year, graph_mode, line_category, window_scale, pie_categories)
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


def build_line_chart_svg(series_map: dict[str, pd.Series], title: str) -> str:
    width = 1160
    height = 380
    margin_left = 64
    margin_right = 32
    margin_top = 28
    margin_bottom = 54
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
        f'<svg class="chart-svg" viewBox="0 0 {width} {height}" role="img" aria-label="{html.escape(title)}">',
        f'<text x="{margin_left}" y="18" class="chart-title">{html.escape(title)}</text>',
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
        if index < 8 or index == len(labels) - 1 or index % max(len(labels) // 6, 1) == 0:
            parts.append(f'<text x="{x:.1f}" y="{height - 18}" text-anchor="middle" class="axis-label">{html.escape(label)}</text>')

    legend_x = margin_left
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
        parts.append(f'<circle cx="{legend_x}" cy="{height - 8}" r="5" fill="{color}" />')
        parts.append(f'<text x="{legend_x + 12}" y="{height - 4}" class="legend-label">{html.escape(series_name)}</text>')
        legend_x += 150

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

    width = 1160
    height = 360
    colors = ["#00a67e", "#3b82f6", "#635bff", "#f59e0b", "#db5b7b", "#0ea5e9", "#a855f7", "#14b8a6", "#64748b"]
    sections = [("Income", income_series, 280), ("Expenses", expense_series, 820)]
    parts = [f'<svg class="chart-svg pie-svg" viewBox="0 0 {width} {height}" role="img" aria-label="Income and expense pie charts">']
    for title, series, center_x in sections:
        total = float(series.sum()) if not series.empty else 0.0
        parts.append(f'<text x="{center_x}" y="28" text-anchor="middle" class="chart-title">{html.escape(title)}</text>')
        if total <= 0:
            parts.append(f'<text x="{center_x}" y="180" text-anchor="middle" class="axis-label">No data</text>')
            continue
        start = -math.pi / 2
        center_y = 160
        radius = 88
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
        legend_y = 280
        for idx, (category, amount) in enumerate(series.items()):
            if float(amount) <= 0:
                continue
            color = colors[idx % len(colors)]
            tooltip = f"{title} | {category} | {currency(float(amount))}"
            parts.append(f'<rect x="{center_x - 108}" y="{legend_y + idx*20}" width="12" height="12" rx="3" fill="{color}" data-tooltip="{html.escape(tooltip)}" />')
            parts.append(f'<text x="{center_x - 90}" y="{legend_y + idx*20 + 10}" class="legend-label">{html.escape(category)} {html.escape(currency(float(amount)))}</text>')
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
                "identifier": strip_purchase_prefix(str(row.get("description") or row.get("reference") or "")),
                "name": row.get("name", "") if include_contact else "",
                "email": row.get("email", "") if include_contact else "",
                "amount": currency(float(row.get("amount", 0.0))),
                "source": row.get("source", ""),
                "matched": "Yes" if bool(row.get("matched")) else "No",
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


def source_rows() -> list[dict[str, str]]:
    paths = current_source_paths()
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
        }
        for key in SOURCE_KEYS
    ]


def rule_rows() -> list[dict[str, str]]:
    income_rules_path, expense_rules_path = current_rule_paths()
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
            }
        )
    return rows


def base_template_context(request: Request) -> dict[str, Any]:
    return {
        "request": request,
        "app_name": APP_NAME,
        "auth_error": auth_config_error(),
        "asset_version": ASSET_VERSION,
    }


def dashboard_context(
    request: Request,
    period: str,
    start_text: str,
    end_text: str,
    selected_year: int,
    graph_mode: str,
    line_category: str,
    window_scale: int,
    pie_categories: list[str],
    message: Optional[str],
) -> dict[str, Any]:
    bundle, missing_sources = load_bundle_safe()
    start, end = period_range(period, start_text, end_text, selected_year)
    filtered_income = filter_frame(bundle.income, start, end)
    filtered_expenses = filter_frame(bundle.expenses, start, end)
    all_categories = list(dict.fromkeys(INCOME_CATEGORIES + EXPENSE_CATEGORIES))
    graph_mode_value = graph_mode if graph_mode in {"totals", "category", "pie"} else "totals"
    line_category_value = line_category if line_category in all_categories else (all_categories[0] if all_categories else "")
    window_scale_value = window_scale if 0 <= window_scale < len(WINDOW_OPTIONS) else 2
    window_key = WINDOW_OPTIONS[window_scale_value]
    pie_selected = [category for category in pie_categories if category in all_categories]
    if not pie_selected:
        pie_selected = all_categories[:]

    income_summary = summarize_categories(filtered_income, INCOME_CATEGORIES, [])
    expense_summary = summarize_categories(filtered_expenses, EXPENSE_CATEGORIES, [])
    recent_transactions = frame_for_view(bundle, filtered_income, filtered_expenses, "All", start, end)

    chart_title = "Total Income & Total Expenses"
    if graph_mode_value == "totals":
        chart_svg = build_line_chart_svg(
            {
                "Income": aggregate_series(filtered_income, window_key),
                "Expenses": aggregate_series(filtered_expenses, window_key),
            },
            chart_title,
        )
    elif graph_mode_value == "category":
        income_category_frame = apply_focus(filtered_income, line_category_value if line_category_value in INCOME_CATEGORIES else "all")
        expense_category_frame = apply_focus(filtered_expenses, line_category_value if line_category_value in EXPENSE_CATEGORIES else "all")
        chart_title = f"Income & Expenses for {line_category_value}"
        chart_svg = build_line_chart_svg(
            {
                "Income": aggregate_series(income_category_frame, window_key),
                "Expenses": aggregate_series(expense_category_frame, window_key),
            },
            chart_title,
        )
    else:
        chart_title = "Pie Breakdown"
        pie_income = summarize_categories(filtered_income[filtered_income["category"].isin(pie_selected)], INCOME_CATEGORIES, [])
        pie_expense = summarize_categories(filtered_expenses[filtered_expenses["category"].isin(pie_selected)], EXPENSE_CATEGORIES, [])
        chart_svg = build_pie_svg(pie_income, pie_expense)

    export_query = urlencode(dashboard_base_params(period, start_text, end_text, selected_year, graph_mode_value, line_category_value, window_scale_value, pie_selected))

    return {
        **base_template_context(request),
        "message": message,
        "active_page": "dashboard",
        "source_rows": source_rows(),
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
        "misc_count": len(filter_frame(bundle.misc_income, start, end)) + len(filter_frame(bundle.misc_expenses, start, end)),
        "income_rows": category_rows(income_summary),
        "expense_rows": category_rows(expense_summary),
        "recent_transactions": transaction_rows(recent_transactions, include_contact=True)[:24],
        "export_query": export_query,
        "graph_mode": graph_mode_value,
        "graph_mode_links": {
            mode: dashboard_url(period, start_text, end_text, selected_year, mode, line_category_value, window_scale_value, pie_selected)
            for mode in ["totals", "category", "pie"]
        },
        "line_category": line_category_value,
        "all_categories": all_categories,
        "pie_categories_selected": set(pie_selected),
        "window_scale": window_scale_value,
        "window_key": window_key,
        "window_label": WINDOW_OPTIONS[window_scale_value].title(),
        "chart_title": chart_title,
        "chart_svg": chart_svg,
        "chart_tooltip_script": svg_tooltip_script(),
    }


def files_context(request: Request, message: Optional[str]) -> dict[str, Any]:
    return {
        **base_template_context(request),
        "message": message,
        "active_page": "files",
        "source_rows": source_rows(),
        "rule_rows": rule_rows(),
        "missing_sources": missing_source_keys(),
    }


def rules_context(request: Request, message: Optional[str]) -> dict[str, Any]:
    return {
        **base_template_context(request),
        "message": message,
        "active_page": "rules",
        "rule_rows": rule_rows(),
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
        },
    )


@app.post("/login")
async def login(
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
    line_category: str = Query(""),
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
def rules_page(request: Request, message: Optional[str] = Query(None)) -> HTMLResponse:
    auth_redirect = require_auth(request)
    if auth_redirect:
        return auth_redirect
    return templates.TemplateResponse("rules.html", rules_context(request, message))


@app.post("/upload/{source_key}")
async def upload_source(request: Request, source_key: str, file: UploadFile) -> RedirectResponse:
    auth_redirect = require_auth(request)
    if auth_redirect:
        return auth_redirect
    if source_key not in SOURCE_KEYS:
        raise HTTPException(status_code=404, detail="Unknown source key")
    if not file.filename or not file.filename.lower().endswith(".csv"):
        return RedirectResponse(url="/files?message=Please upload a CSV file.", status_code=303)

    destination = current_source_paths()[source_key]
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
    path = current_source_paths()[source_key]
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
    destination = ensure_web_rule_file(filename)
    contents = await file.read()
    destination.write_bytes(contents)
    try:
        validated = load_rule_table(destination, categories)
        save_rule_table(destination, validated, categories)
    except Exception as exc:
        return RedirectResponse(url=f"/files?message=Failed to load uploaded rules: {exc}", status_code=303)
    return RedirectResponse(url=f"/files?message=Updated {rule_key} rules file.", status_code=303)


@app.get("/download/rules/{rule_key}")
def download_rules(request: Request, rule_key: str) -> StreamingResponse:
    auth_redirect = require_auth(request)
    if auth_redirect:
        return auth_redirect
    if rule_key not in RULE_FILE_MAP:
        raise HTTPException(status_code=404, detail="Unknown rule key")
    filename, _categories = RULE_FILE_MAP[rule_key]
    path = ensure_web_rule_file(filename)
    if not path.exists():
        raise HTTPException(status_code=404, detail="File not found")
    return StreamingResponse(
        io.BytesIO(path.read_bytes()),
        media_type="text/csv",
        headers={"Content-Disposition": f'attachment; filename="{path.name}"'},
    )


@app.get("/transactions/export")
def export_transactions(
    request: Request,
    period: str = Query("All Dates"),
    start: str = Query(""),
    end: str = Query(""),
    selected_year: int = Query(date.today().year),
    graph_mode: str = Query("totals"),
    line_category: str = Query(""),
    window_scale: int = Query(2),
    pie_categories: list[str] = Query(default=[]),
) -> StreamingResponse:
    auth_redirect = require_auth(request)
    if auth_redirect:
        return auth_redirect
    bundle, _missing_sources = load_bundle_safe()
    start_date, end_date = period_range(period, start, end, selected_year)
    filtered_income = filter_frame(bundle.income, start_date, end_date)
    filtered_expenses = filter_frame(bundle.expenses, start_date, end_date)
    if graph_mode == "category" and line_category:
        income_frame = apply_focus(filtered_income, line_category if line_category in INCOME_CATEGORIES else "all")
        expense_frame = apply_focus(filtered_expenses, line_category if line_category in EXPENSE_CATEGORIES else "all")
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
