from __future__ import annotations

import calendar
import hashlib
import json
import re
import shutil
import sys
from dataclasses import dataclass
from datetime import date, timedelta
from pathlib import Path

import pandas as pd


APP_NAME = "MUUC Finance Analyzer"
BASE_DIR = Path(__file__).resolve().parent
RUNTIME_DIR = Path(getattr(sys, "_MEIPASS", str(BASE_DIR)))
CONFIG_DIR = RUNTIME_DIR / "config"
SETTINGS_DIR = Path.home() / ".muuc_finance_analyzer"
USER_CONFIG_DIR = SETTINGS_DIR / "config"
USER_SOURCE_DIR = SETTINGS_DIR / "source"
BUNDLED_RULES_STATE_FILE = USER_CONFIG_DIR / "bundled_rules_state.json"
SOURCE_FILENAMES = {
    "stripe": "stripe.csv",
    "teamapp": "teamapp.csv",
    "everyday": "everyday.csv",
}

BG_PRIMARY = "#16181c"
BG_PANEL = "#1f232a"
BG_INPUT = "#252b33"
BG_ACCENT = "#2c3440"
BORDER = "#3b4552"
TEXT_PRIMARY = "#eef2f7"
TEXT_MUTED = "#aeb8c5"
SELECT_BG = "#35506b"
SELECT_FG = "#ffffff"

INCOME_CATEGORIES = [
    "air",
    "courses",
    "gear hire",
    "memberships",
    "social",
    "merch",
    "trips",
    "specialtrips",
    "misc",
]

EXPENSE_CATEGORIES = [
    "fees",
    "air",
    "car/boat",
    "compressor",
    "courses",
    "gear",
    "gear servicing",
    "refunds",
    "social",
    "specialtrips",
    "trips",
    "misc",
]

CATEGORY_COLORS = {
    "air": "#4c78a8",
    "courses": "#f58518",
    "gear hire": "#54a24b",
    "memberships": "#e45756",
    "social": "#72b7b2",
    "merch": "#5d8cae",
    "trips": "#eeca3b",
    "specialtrips": "#b279a2",
    "misc": "#9d755d",
    "fees": "#bab0ab",
    "car/boat": "#5f8dd3",
    "compressor": "#7a9e9f",
    "gear": "#89a54e",
    "gear servicing": "#4f7f6f",
    "refunds": "#d94f70",
}


@dataclass
class AnalysisBundle:
    income: pd.DataFrame
    expenses: pd.DataFrame
    misc_income: pd.DataFrame
    misc_expenses: pd.DataFrame


def default_file_paths() -> dict[str, str]:
    source_dir = USER_SOURCE_DIR
    if BASE_DIR == RUNTIME_DIR:
        source_dir = BASE_DIR / "source"
    source_dir.mkdir(parents=True, exist_ok=True)
    paths: dict[str, str] = {}
    for key, filename in SOURCE_FILENAMES.items():
        user_path = source_dir / filename
        bundled_path = RUNTIME_DIR / "source" / filename
        if not user_path.exists() and bundled_path.exists():
            shutil.copyfile(bundled_path, user_path)
        paths[key] = str(user_path if user_path.exists() else bundled_path)
    return paths


def bundled_rules_fingerprint() -> str:
    hasher = hashlib.sha256()
    for filename in ("income_rules.csv", "expense_rules.csv"):
        bundled_path = CONFIG_DIR / filename
        hasher.update(filename.encode("utf-8"))
        if bundled_path.exists():
            hasher.update(bundled_path.read_bytes())
    return hasher.hexdigest()


def sync_bundled_rules_if_updated() -> None:
    USER_CONFIG_DIR.mkdir(parents=True, exist_ok=True)
    current_fingerprint = bundled_rules_fingerprint()
    stored_fingerprint = ""
    if BUNDLED_RULES_STATE_FILE.exists():
        try:
            payload = json.loads(BUNDLED_RULES_STATE_FILE.read_text(encoding="utf-8"))
            stored_fingerprint = str(payload.get("fingerprint", ""))
        except (json.JSONDecodeError, OSError):
            stored_fingerprint = ""

    if stored_fingerprint == current_fingerprint:
        return

    for filename in ("income_rules.csv", "expense_rules.csv"):
        bundled_path = CONFIG_DIR / filename
        user_path = USER_CONFIG_DIR / filename
        if bundled_path.exists():
            shutil.copyfile(bundled_path, user_path)

    BUNDLED_RULES_STATE_FILE.write_text(
        json.dumps({"fingerprint": current_fingerprint}, indent=2),
        encoding="utf-8",
    )


def ensure_user_rule_file(filename: str) -> Path:
    sync_bundled_rules_if_updated()
    USER_CONFIG_DIR.mkdir(parents=True, exist_ok=True)
    user_path = USER_CONFIG_DIR / filename
    if user_path.exists():
        return user_path

    bundled_path = CONFIG_DIR / filename
    if bundled_path.exists():
        shutil.copyfile(bundled_path, user_path)
    else:
        user_path.write_text("", encoding="utf-8")
    return user_path


def load_rule_table(path: Path, categories: list[str]) -> pd.DataFrame:
    if path.exists():
        df = pd.read_csv(path).fillna("")
    else:
        df = pd.DataFrame(columns=categories)
    for column in categories:
        if column not in df.columns:
            df[column] = ""
    df = df[categories].fillna("")
    return df.astype(str)


def save_rule_table(path: Path, df: pd.DataFrame, categories: list[str]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    clean = df[categories].fillna("")
    clean.to_csv(path, index=False)
    if path.parent == USER_CONFIG_DIR and BASE_DIR == RUNTIME_DIR:
        project_path = BASE_DIR / "config" / path.name
        project_path.parent.mkdir(parents=True, exist_ok=True)
        clean.to_csv(project_path, index=False)


def compile_rule_map(df: pd.DataFrame, categories: list[str]) -> dict[str, list[re.Pattern[str]]]:
    compiled: dict[str, list[tuple[str, re.Pattern[str]]]] = {category: [] for category in categories}
    for category in categories:
        for value in df.get(category, pd.Series(dtype="object")).tolist():
            text = str(value).strip()
            if not text:
                continue
            try:
                compiled[category].append((text, re.compile(text, re.IGNORECASE)))
            except re.error:
                continue
    return compiled


def match_category(
    text: str,
    compiled_rules: dict[str, list[tuple[str, re.Pattern[str]]]],
    categories: list[str],
) -> tuple[str, bool, str]:
    haystack = text or ""
    for category in categories:
        for pattern_text, pattern in compiled_rules.get(category, []):
            if pattern.search(haystack):
                return category, True, pattern_text
    return "misc", False, ""


def parse_stripe_income(stripe_path: Path) -> tuple[pd.DataFrame, pd.DataFrame]:
    raw = pd.read_csv(stripe_path)
    df = raw.copy()
    df["date"] = pd.to_datetime(df["Created date (UTC)"], errors="coerce")
    df["status_normalized"] = df["Status"].fillna("").astype(str).str.strip().str.lower()
    df["description"] = df["Description"].fillna("")
    df["gross_amount"] = pd.to_numeric(df["Converted Amount"], errors="coerce").fillna(0.0)
    df["refunded_amount"] = pd.to_numeric(df["Converted Amount Refunded"], errors="coerce").fillna(0.0)
    df["fee_amount"] = pd.to_numeric(df["Fee"], errors="coerce").fillna(0.0)
    df = df[~df["status_normalized"].isin({"failed", "requires_payment_method"})].copy()
    refund_mask = df["status_normalized"].isin({"refunded", "refund"}) | (df["refunded_amount"] > 0)

    fee_expenses = df[df["fee_amount"] > 0][["date", "description", "fee_amount", "id"]].copy()
    fee_expenses["category"] = "fees"
    fee_expenses["matched"] = True
    fee_expenses["subgroup"] = "Fee column"
    fee_expenses["source"] = "stripe fee"
    fee_expenses.rename(columns={"fee_amount": "amount", "id": "reference"}, inplace=True)
    refund_expenses = df[refund_mask][["date", "description", "refunded_amount", "id"]].copy()
    refund_expenses = refund_expenses[refund_expenses["refunded_amount"] > 0].copy()
    refund_expenses["category"] = "refunds"
    refund_expenses["matched"] = True
    refund_expenses["subgroup"] = "Refund status"
    refund_expenses["source"] = "stripe refund"
    refund_expenses.rename(columns={"refunded_amount": "amount", "id": "reference"}, inplace=True)
    expenses = pd.concat([fee_expenses, refund_expenses], ignore_index=True, sort=False)
    expenses["name"] = ""
    expenses["email"] = ""
    expenses = expenses[["date", "description", "category", "matched", "subgroup", "amount", "source", "reference", "name", "email"]]
    income = pd.DataFrame(
        columns=["date", "description", "category", "matched", "subgroup", "amount", "source", "reference", "refunded_amount", "name", "email"]
    )
    return income, expenses


def parse_teamapp_income(teamapp_path: Path, rule_df: pd.DataFrame) -> pd.DataFrame:
    raw = pd.read_csv(teamapp_path)
    df = raw.copy()
    df["date"] = pd.to_datetime(df["date"], format="%Y-%b-%d", errors="coerce")
    df["paid_normalized"] = df["paid"].fillna("").astype(str).str.strip().str.upper()
    df = df[df["paid_normalized"] == "YES"].copy()
    df = df[df["date"] >= pd.Timestamp("2025-01-01")].copy()
    df["description"] = df["items"].fillna("")
    df["amount"] = (
        df["total"]
        .fillna("")
        .astype(str)
        .str.replace("$", "", regex=False)
        .str.replace(",", "", regex=False)
    )
    df["amount"] = pd.to_numeric(df["amount"], errors="coerce").fillna(0.0)
    df["name"] = df["name"].fillna("").astype(str)
    df["email"] = df["email"].fillna("").astype(str)
    compiled = compile_rule_map(rule_df, INCOME_CATEGORIES)
    if df.empty:
        return pd.DataFrame(
            columns=["date", "description", "category", "matched", "subgroup", "amount", "source", "reference", "refunded_amount", "name", "email"]
        )
    categories, matched, subgroups = zip(*df["description"].map(lambda value: match_category(value, compiled, INCOME_CATEGORIES)))
    df["category"] = list(categories)
    df["matched"] = list(matched)
    df["subgroup"] = list(subgroups)
    df["source"] = "teamapp income"
    df["reference"] = df["purchase_id"].fillna("").astype(str)
    df["refunded_amount"] = 0.0
    return df[["date", "description", "category", "matched", "subgroup", "amount", "source", "reference", "refunded_amount", "name", "email"]]


def parse_everyday_expenses(everyday_path: Path, rule_df: pd.DataFrame) -> pd.DataFrame:
    raw = pd.read_csv(everyday_path)
    df = raw.copy()
    df["date"] = pd.to_datetime(df["Date"], format="%d %b %y", errors="coerce")
    missing_dates = df["date"].isna()
    if missing_dates.any():
        df.loc[missing_dates, "date"] = pd.to_datetime(df.loc[missing_dates, "Date"], format="%d %B %y", errors="coerce")
    missing_dates = df["date"].isna()
    if missing_dates.any():
        normalized_dates = df.loc[missing_dates, "Date"].astype(str).str.replace("Sept", "Sep", regex=False)
        df.loc[missing_dates, "date"] = pd.to_datetime(normalized_dates, format="%d %b %y", errors="coerce")
    df["raw_amount"] = pd.to_numeric(df["Amount"], errors="coerce").fillna(0.0)
    df = df[df["raw_amount"] < 0].copy()
    df = df[df["Category"].fillna("").str.strip().str.lower() != "internal transfers"].copy()
    df["amount"] = df["raw_amount"].abs()
    detail_text = df["Transaction Details"].fillna("") + " " + df["Merchant Name"].fillna("")
    df["description"] = detail_text.str.strip()
    compiled = compile_rule_map(rule_df, EXPENSE_CATEGORIES)
    categories, matched, subgroups = zip(*df["description"].map(lambda value: match_category(value, compiled, EXPENSE_CATEGORIES)))
    df["category"] = list(categories)
    df["matched"] = list(matched)
    df["subgroup"] = list(subgroups)
    df["source"] = "everyday expense"
    df["reference"] = ""
    df["name"] = ""
    df["email"] = ""
    return df[["date", "description", "category", "matched", "subgroup", "amount", "source", "reference", "name", "email"]]


def load_analysis(
    stripe_path: Path,
    teamapp_path: Path,
    everyday_path: Path,
    income_rules_path: Path,
    expense_rules_path: Path,
) -> AnalysisBundle:
    income_rules = load_rule_table(income_rules_path, INCOME_CATEGORIES)
    expense_rules = load_rule_table(expense_rules_path, EXPENSE_CATEGORIES)
    _, stripe_expenses = parse_stripe_income(stripe_path)
    income = parse_teamapp_income(teamapp_path, income_rules)
    everyday_expenses = parse_everyday_expenses(everyday_path, expense_rules)
    expenses = pd.concat([everyday_expenses, stripe_expenses], ignore_index=True, sort=False)
    misc_income = income[income["category"] == "misc"].copy()
    misc_expenses = expenses[expenses["category"] == "misc"].copy()
    return AnalysisBundle(
        income=income.sort_values("date"),
        expenses=expenses.sort_values("date"),
        misc_income=misc_income.sort_values("date"),
        misc_expenses=misc_expenses.sort_values("date"),
    )


def start_of_financial_year(anchor: date) -> date:
    year = anchor.year if anchor.month >= 7 else anchor.year - 1
    return date(year, 7, 1)


def end_of_financial_year(anchor: date) -> date:
    start = start_of_financial_year(anchor)
    return date(start.year + 1, 6, 30)


def parse_date_input(value: str) -> date | None:
    text = value.strip()
    if not text:
        return None
    try:
        return date.fromisoformat(text)
    except ValueError:
        return None


def month_range(year: int, month: int) -> tuple[date, date]:
    last_day = calendar.monthrange(year, month)[1]
    return date(year, month, 1), date(year, month, last_day)


def period_range(mode: str, start_text: str, end_text: str, selected_year: int) -> tuple[date | None, date | None]:
    today = date.today()
    custom_start = parse_date_input(start_text)
    custom_end = parse_date_input(end_text)

    if mode == "All Dates":
        return None, None
    if mode == "Custom":
        return custom_start, custom_end
    if mode == "Month To Date":
        return date(today.year, today.month, 1), today
    if mode == "Year To Date":
        return date(today.year, 1, 1), today
    if mode == "Financial Year To Date":
        return start_of_financial_year(today), today
    if mode == "Last 30 Days":
        return today - timedelta(days=29), today
    if mode == "Current Month":
        return month_range(today.year, today.month)
    if mode == "Current Year":
        return date(today.year, 1, 1), date(today.year, 12, 31)
    if mode == "Selected Year":
        return date(selected_year, 1, 1), date(selected_year, 12, 31)
    if mode == "Current Financial Year":
        start = start_of_financial_year(today)
        return start, end_of_financial_year(today)
    return custom_start, custom_end


def filter_frame(df: pd.DataFrame, start: date | None, end: date | None) -> pd.DataFrame:
    if df.empty:
        return df.copy()
    out = df.copy()
    out["date_only"] = pd.to_datetime(out["date"], errors="coerce").dt.date
    if start:
        out = out[out["date_only"] >= start]
    if end:
        out = out[out["date_only"] <= end]
    return out.drop(columns=["date_only"])


def summarize_categories(df: pd.DataFrame, categories: list[str], selected_categories: list[str]) -> pd.Series:
    if df.empty:
        return pd.Series(0.0, index=selected_categories or categories)
    active = selected_categories or categories
    grouped = df.groupby("category")["amount"].sum()
    return grouped.reindex(active, fill_value=0.0).sort_values(ascending=False)


def currency(value: float) -> str:
    return f"${value:,.2f}"


def latest_entry_label(csv_key: str, csv_path: Path) -> str:
    if not csv_path.exists():
        return "latest: unavailable"

    try:
        df = pd.read_csv(csv_path)
        if csv_key == "stripe":
            dates = pd.to_datetime(df.get("Created date (UTC)"), errors="coerce")
        elif csv_key == "teamapp":
            dates = pd.to_datetime(df.get("date"), format="%Y-%b-%d", errors="coerce")
        elif csv_key == "everyday":
            dates = pd.to_datetime(df.get("Date"), format="%d %b %y", errors="coerce")
        else:
            return "latest: unavailable"

        if dates.isna().all():
            return "latest: unavailable"
        return f"latest: {dates.max().strftime('%Y-%m-%d')}"
    except Exception:
        return "latest: unavailable"
