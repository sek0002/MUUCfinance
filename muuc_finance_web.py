from __future__ import annotations

import argparse
import base64
import io
import os
import re
from datetime import date
from pathlib import Path
from typing import Any, Optional
from urllib.parse import urlencode

import pandas as pd
import pyotp
import qrcode
import qrcode.image.svg
import uvicorn
from fastapi import FastAPI, Form, HTTPException, Query, Request, UploadFile
from fastapi.responses import HTMLResponse, RedirectResponse, StreamingResponse
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
    for key in SOURCE_KEYS:
        filename = SOURCE_FILENAMES[key]
        destination = WEB_SOURCE_DIR / filename
        bundled_path = RUNTIME_DIR / "source" / filename
        if not destination.exists() and bundled_path.exists():
            destination.write_bytes(bundled_path.read_bytes())
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


def require_auth(request: Request) -> Optional[RedirectResponse]:
    if not request.session.get("authenticated"):
        return RedirectResponse(url="/login", status_code=303)
    return None


def requested_categories(categories: list[str]) -> list[str]:
    allowed = set(INCOME_CATEGORIES + EXPENSE_CATEGORIES)
    return [category for category in categories if category in allowed]


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
    return pd.concat([income, expenses], ignore_index=True, sort=False).sort_values("date")


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
    }


def otpauth_uri() -> str:
    cfg = auth_config()
    return pyotp.TOTP(cfg["totp_secret"]).provisioning_uri(name=cfg["username"], issuer_name=cfg["issuer"])


def otpauth_qr_svg_data_uri() -> str:
    image = qrcode.make(otpauth_uri(), image_factory=qrcode.image.svg.SvgPathImage)
    buffer = io.BytesIO()
    image.save(buffer)
    encoded = base64.b64encode(buffer.getvalue()).decode("ascii")
    return f"data:image/svg+xml;base64,{encoded}"


def dashboard_context(
    request: Request,
    period: str,
    start_text: str,
    end_text: str,
    selected_year: int,
    categories: list[str],
    transaction_view: str,
    message: Optional[str],
) -> dict[str, Any]:
    bundle = load_bundle()
    active_categories = requested_categories(categories)
    start, end = period_range(period, start_text, end_text, selected_year)

    filtered_income = filter_frame(bundle.income, start, end)
    filtered_expenses = filter_frame(bundle.expenses, start, end)
    if active_categories:
        filtered_income = filtered_income[filtered_income["category"].isin(active_categories)]
        filtered_expenses = filtered_expenses[filtered_expenses["category"].isin(active_categories)]

    income_summary = summarize_categories(filtered_income, INCOME_CATEGORIES, [c for c in active_categories if c in INCOME_CATEGORIES])
    expense_summary = summarize_categories(filtered_expenses, EXPENSE_CATEGORIES, [c for c in active_categories if c in EXPENSE_CATEGORIES])
    transaction_frame = frame_for_view(bundle, filtered_income, filtered_expenses, transaction_view, start, end)

    all_query = urlencode(
        [
            ("period", period),
            ("start", start_text),
            ("end", end_text),
            ("selected_year", str(selected_year)),
            ("transaction_view", transaction_view),
            *[("categories", category) for category in active_categories],
        ]
    )

    return {
        **base_template_context(request),
        "message": message,
        "period": period,
        "period_options": PERIOD_OPTIONS,
        "start": start_text,
        "end": end_text,
        "selected_year": selected_year,
        "show_custom_dates": period == "Custom",
        "selected_categories": set(active_categories),
        "all_categories": INCOME_CATEGORIES + [c for c in EXPENSE_CATEGORIES if c not in INCOME_CATEGORIES],
        "transaction_view": transaction_view,
        "transaction_views": ["All", "Income", "Expenses", "Income Misc", "Expense Misc"],
        "source_rows": source_rows(),
        "rule_rows": rule_rows(),
        "income_total": currency(float(filtered_income["amount"].sum())) if not filtered_income.empty else currency(0.0),
        "expense_total": currency(float(filtered_expenses["amount"].sum())) if not filtered_expenses.empty else currency(0.0),
        "net_total": currency(float(filtered_income["amount"].sum()) - float(filtered_expenses["amount"].sum())),
        "misc_count": len(filter_frame(bundle.misc_income, start, end)) + len(filter_frame(bundle.misc_expenses, start, end)),
        "income_rows": category_rows(income_summary),
        "expense_rows": category_rows(expense_summary),
        "monthly_rows": monthly_rows(filtered_income, filtered_expenses),
        "income_transactions": transaction_rows(filtered_income, include_contact=True),
        "expense_transactions": transaction_rows(filtered_expenses, include_contact=False),
        "review_transactions": transaction_rows(transaction_frame, include_contact=True),
        "export_query": all_query,
    }


@app.get("/", response_class=HTMLResponse)
def home(request: Request) -> RedirectResponse:
    if request.session.get("authenticated"):
        return RedirectResponse(url="/dashboard", status_code=303)
    return RedirectResponse(url="/login", status_code=303)


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
    categories: list[str] = Query(default=[]),
    transaction_view: str = Query("All"),
    message: Optional[str] = Query(None),
) -> HTMLResponse:
    auth_redirect = require_auth(request)
    if auth_redirect:
        return auth_redirect
    context = dashboard_context(request, period, start, end, selected_year, categories, transaction_view, message)
    return templates.TemplateResponse("dashboard.html", context)


@app.post("/upload/{source_key}")
async def upload_source(request: Request, source_key: str, file: UploadFile) -> RedirectResponse:
    auth_redirect = require_auth(request)
    if auth_redirect:
        return auth_redirect
    if source_key not in SOURCE_KEYS:
        raise HTTPException(status_code=404, detail="Unknown source key")
    if not file.filename or not file.filename.lower().endswith(".csv"):
        return RedirectResponse(url="/dashboard?message=Please upload a CSV file.", status_code=303)

    destination = current_source_paths()[source_key]
    destination.parent.mkdir(parents=True, exist_ok=True)
    contents = await file.read()
    destination.write_bytes(contents)
    return RedirectResponse(url=f"/dashboard?message=Updated {source_key} source file.", status_code=303)


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
        return RedirectResponse(url="/dashboard?message=Please upload a CSV file.", status_code=303)

    filename, categories = RULE_FILE_MAP[rule_key]
    destination = ensure_web_rule_file(filename)
    contents = await file.read()
    destination.write_bytes(contents)
    try:
        validated = load_rule_table(destination, categories)
        save_rule_table(destination, validated, categories)
    except Exception as exc:
        return RedirectResponse(url=f"/dashboard?message=Failed to load uploaded rules: {exc}", status_code=303)
    return RedirectResponse(url=f"/dashboard?message=Updated {rule_key} rules file.", status_code=303)


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
    categories: list[str] = Query(default=[]),
    transaction_view: str = Query("All"),
) -> StreamingResponse:
    auth_redirect = require_auth(request)
    if auth_redirect:
        return auth_redirect
    bundle = load_bundle()
    active_categories = requested_categories(categories)
    start_date, end_date = period_range(period, start, end, selected_year)
    filtered_income = filter_frame(bundle.income, start_date, end_date)
    filtered_expenses = filter_frame(bundle.expenses, start_date, end_date)
    if active_categories:
        filtered_income = filtered_income[filtered_income["category"].isin(active_categories)]
        filtered_expenses = filtered_expenses[filtered_expenses["category"].isin(active_categories)]
    frame = frame_for_view(bundle, filtered_income, filtered_expenses, transaction_view, start_date, end_date).copy()
    if not frame.empty:
        frame["date"] = pd.to_datetime(frame["date"], errors="coerce").dt.strftime("%Y-%m-%d").fillna("")
    csv_bytes = frame.to_csv(index=False).encode("utf-8")
    return StreamingResponse(
        io.BytesIO(csv_bytes),
        media_type="text/csv",
        headers={"Content-Disposition": 'attachment; filename="transactions_export.csv"'},
    )


@app.get("/auth/setup", response_class=HTMLResponse)
def authenticator_setup(request: Request) -> HTMLResponse:
    auth_redirect = require_auth(request)
    if auth_redirect:
        return auth_redirect
    config_error = auth_config_error()
    context = {
        **base_template_context(request),
        "config_error": config_error,
        "username": auth_config()["username"],
        "totp_secret": auth_config()["totp_secret"],
        "otpauth_uri": "" if config_error else otpauth_uri(),
        "qr_data_uri": "" if config_error else otpauth_qr_svg_data_uri(),
    }
    return templates.TemplateResponse("setup.html", context)


def init_auth_secret() -> int:
    secret = pyotp.random_base32()
    username = os.getenv("MUUC_WEB_USERNAME", "admin")
    issuer = os.getenv("MUUC_TOTP_ISSUER", APP_NAME)
    uri = pyotp.TOTP(secret).provisioning_uri(name=username, issuer_name=issuer)
    print(f"MUUC_WEB_USERNAME={username}")
    print("MUUC_WEB_PASSWORD=choose-a-strong-password")
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
