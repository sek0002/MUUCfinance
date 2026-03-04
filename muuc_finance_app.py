from __future__ import annotations

import calendar
import hashlib
import json
import re
import shutil
import sys
import tkinter as tk
import webbrowser
from dataclasses import dataclass
from datetime import date, timedelta
from pathlib import Path
from tkinter import messagebox, simpledialog, ttk

import pandas as pd


APP_NAME = "MUUC Finance Analyzer"
BASE_DIR = Path(__file__).resolve().parent
RUNTIME_DIR = Path(getattr(sys, "_MEIPASS", str(BASE_DIR)))
CONFIG_DIR = RUNTIME_DIR / "config"
SETTINGS_DIR = Path.home() / ".muuc_finance_analyzer"
USER_CONFIG_DIR = SETTINGS_DIR / "config"
BUNDLED_RULES_STATE_FILE = USER_CONFIG_DIR / "bundled_rules_state.json"

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
    return {
        "stripe": str(RUNTIME_DIR / "source" / "stripe.csv"),
        "everyday": str(RUNTIME_DIR / "source" / "everyday.csv"),
    }


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
        if BASE_DIR == RUNTIME_DIR:
            project_path = BASE_DIR / "config" / filename
            project_path.parent.mkdir(parents=True, exist_ok=True)
            shutil.copyfile(user_path, project_path)
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
    compiled: dict[str, list[re.Pattern[str]]] = {category: [] for category in categories}
    for category in categories:
        for value in df.get(category, pd.Series(dtype="object")).tolist():
            text = str(value).strip()
            if not text:
                continue
            try:
                compiled[category].append(re.compile(text, re.IGNORECASE))
            except re.error:
                continue
    return compiled


def match_category(text: str, compiled_rules: dict[str, list[re.Pattern[str]]], categories: list[str]) -> tuple[str, bool]:
    haystack = text or ""
    for category in categories:
        for pattern in compiled_rules.get(category, []):
            if pattern.search(haystack):
                return category, True
    return "misc", False


def parse_stripe_income(
    stripe_path: Path,
    rule_df: pd.DataFrame,
) -> tuple[pd.DataFrame, pd.DataFrame]:
    raw = pd.read_csv(stripe_path)
    df = raw.copy()
    df["date"] = pd.to_datetime(df["Created date (UTC)"], errors="coerce")
    df["status_normalized"] = df["Status"].fillna("").astype(str).str.strip().str.lower()
    df["description"] = df["Description"].fillna("")
    df["gross_amount"] = pd.to_numeric(df["Converted Amount"], errors="coerce").fillna(0.0)
    df["refunded_amount"] = pd.to_numeric(df["Converted Amount Refunded"], errors="coerce").fillna(0.0)
    df["fee_amount"] = pd.to_numeric(df["Fee"], errors="coerce").fillna(0.0)
    df = df[~df["status_normalized"].isin({"failed", "requires_payment_method"})].copy()

    refund_mask = df["status_normalized"].isin({"refunded", "refund"})
    income_df = df[~refund_mask].copy()

    compiled = compile_rule_map(rule_df, INCOME_CATEGORIES)
    if income_df.empty:
        income = pd.DataFrame(columns=["date", "description", "category", "matched", "amount", "source", "reference", "refunded_amount"])
    else:
        categories, matched = zip(*income_df["description"].map(lambda value: match_category(value, compiled, INCOME_CATEGORIES)))
        income_df["category"] = list(categories)
        income_df["matched"] = list(matched)
        income = income_df[["date", "description", "category", "matched", "gross_amount", "refunded_amount", "id"]].rename(
            columns={"gross_amount": "amount", "id": "reference"}
        )
        income["source"] = "stripe income"
        income = income[["date", "description", "category", "matched", "amount", "source", "reference", "refunded_amount"]]

    fee_expenses = df[df["fee_amount"] > 0][["date", "description", "fee_amount", "id"]].copy()
    fee_expenses["category"] = "fees"
    fee_expenses["matched"] = True
    fee_expenses["source"] = "stripe fee"
    fee_expenses.rename(columns={"fee_amount": "amount", "id": "reference"}, inplace=True)
    refund_expenses = df[refund_mask & (df["refunded_amount"] > 0)][["date", "description", "refunded_amount", "id"]].copy()
    refund_expenses["category"] = "refunds"
    refund_expenses["matched"] = True
    refund_expenses["source"] = "stripe refund"
    refund_expenses.rename(columns={"refunded_amount": "amount", "id": "reference"}, inplace=True)
    expenses = pd.concat([fee_expenses, refund_expenses], ignore_index=True, sort=False)
    expenses = expenses[["date", "description", "category", "matched", "amount", "source", "reference"]]
    return income, expenses


def parse_everyday_expenses(
    everyday_path: Path,
    rule_df: pd.DataFrame,
) -> pd.DataFrame:
    raw = pd.read_csv(everyday_path)
    df = raw.copy()
    df["date"] = pd.to_datetime(df["Date"], format="%d %b %y", errors="coerce")
    df["raw_amount"] = pd.to_numeric(df["Amount"], errors="coerce").fillna(0.0)
    # Everyday exports encode debits as negative values; only those rows are used.
    df = df[df["raw_amount"] < 0].copy()
    # Ignore bank rows explicitly marked as internal transfers.
    df = df[df["Category"].fillna("").str.strip().str.lower() != "internal transfers"].copy()
    df["amount"] = df["raw_amount"].abs()
    detail_text = df["Transaction Details"].fillna("") + " " + df["Merchant Name"].fillna("")
    df["description"] = detail_text.str.strip()
    compiled = compile_rule_map(rule_df, EXPENSE_CATEGORIES)
    categories, matched = zip(*df["description"].map(lambda value: match_category(value, compiled, EXPENSE_CATEGORIES)))
    df["category"] = list(categories)
    df["matched"] = list(matched)
    df["source"] = "everyday expense"
    df["reference"] = ""
    return df[["date", "description", "category", "matched", "amount", "source", "reference"]]


def load_analysis(
    stripe_path: Path,
    everyday_path: Path,
    income_rules_path: Path,
    expense_rules_path: Path,
) -> AnalysisBundle:
    income_rules = load_rule_table(income_rules_path, INCOME_CATEGORIES)
    expense_rules = load_rule_table(expense_rules_path, EXPENSE_CATEGORIES)
    income, stripe_expenses = parse_stripe_income(stripe_path, income_rules)
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


def period_range(
    mode: str,
    start_text: str,
    end_text: str,
    selected_year: int,
) -> tuple[date | None, date | None]:
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
        elif csv_key == "everyday":
            dates = pd.to_datetime(df.get("Date"), format="%d %b %y", errors="coerce")
        else:
            return "latest: unavailable"

        if dates.isna().all():
            return "latest: unavailable"
        return f"latest: {dates.max().strftime('%Y-%m-%d')}"
    except Exception:
        return "latest: unavailable"


class RuleTableEditor(ttk.LabelFrame):
    def __init__(self, parent: tk.Widget, title: str, file_path: Path, categories: list[str], on_save):
        super().__init__(parent, text=title, padding=8)
        self.file_path = file_path
        self.categories = categories
        self.on_save = on_save
        self.df = load_rule_table(file_path, categories)

        self.tree = ttk.Treeview(self, columns=categories, show="headings", height=8)
        for category in categories:
            self.tree.heading(category, text=category)
            self.tree.column(category, width=145, anchor="w")
        self.tree.grid(row=0, column=0, columnspan=4, sticky="nsew")
        self.tree.bind("<Double-1>", self.edit_selected_cell)

        yscroll = ttk.Scrollbar(self, orient="vertical", command=self.tree.yview)
        self.tree.configure(yscrollcommand=yscroll.set)
        yscroll.grid(row=0, column=4, sticky="ns")

        ttk.Button(self, text="Add Row", command=self.add_row).grid(row=1, column=0, sticky="w", pady=(8, 0))
        ttk.Button(self, text="Delete Row", command=self.delete_row).grid(row=1, column=1, sticky="w", padx=(8, 0), pady=(8, 0))
        ttk.Button(self, text="Save Rules", command=self.save).grid(row=1, column=2, sticky="w", padx=(8, 0), pady=(8, 0))
        ttk.Label(self, text="Double-click a cell to edit its regex. Blank cells are ignored.").grid(
            row=1, column=3, sticky="e", padx=(8, 0), pady=(8, 0)
        )

        self.columnconfigure(0, weight=1)
        self.rowconfigure(0, weight=1)
        self.refresh()

    def refresh(self) -> None:
        self.tree.delete(*self.tree.get_children())
        for idx, row in self.df.fillna("").iterrows():
            values = [row.get(category, "") for category in self.categories]
            self.tree.insert("", "end", iid=str(idx), values=values)

    def add_row(self) -> None:
        self.df.loc[len(self.df)] = {category: "" for category in self.categories}
        self.refresh()

    def delete_row(self) -> None:
        selection = self.tree.selection()
        if not selection:
            return
        row_index = int(selection[0])
        self.df = self.df.drop(index=row_index).reset_index(drop=True)
        self.refresh()

    def edit_selected_cell(self, event) -> None:
        row_id = self.tree.identify_row(event.y)
        column_id = self.tree.identify_column(event.x)
        if not row_id or not column_id:
            return
        col_index = int(column_id.replace("#", "")) - 1
        category = self.categories[col_index]
        current_value = str(self.df.at[int(row_id), category])
        updated = simpledialog.askstring("Edit regex", f"Pattern for {category}", initialvalue=current_value, parent=self)
        if updated is None:
            return
        self.df.at[int(row_id), category] = updated.strip()
        self.refresh()

    def save(self) -> None:
        save_rule_table(self.file_path, self.df, self.categories)
        self.on_save()


class ChartCanvas(tk.Canvas):
    def __init__(self, parent: tk.Widget, width: int = 420, height: int = 320):
        super().__init__(
            parent,
            width=width,
            height=height,
            bg=BG_PANEL,
            highlightthickness=1,
            highlightbackground=BORDER,
        )
        self.width = width
        self.height = height
        self._render_state = ("empty", {"message": "No data loaded."})
        self._tooltip_bg = None
        self._tooltip_text = None
        self.bind("<Configure>", self._handle_configure)
        self.bind("<Leave>", lambda _event: self._hide_tooltip())

    def _handle_configure(self, event) -> None:
        self.width = max(int(event.width), 80)
        self.height = max(int(event.height), 80)
        self._rerender()

    def _rerender(self) -> None:
        mode, payload = self._render_state
        if mode == "pie":
            self._render_pie(payload["values"], payload["title"])
        elif mode == "monthly":
            self._render_monthly_bars(payload["income"], payload["expenses"], payload["title"])
        else:
            self._render_empty(payload["message"])

    def clear(self) -> None:
        self.delete("all")
        self._tooltip_bg = None
        self._tooltip_text = None

    def _show_tooltip(self, x: int, y: int, text: str) -> None:
        if self._tooltip_bg is None or self._tooltip_text is None:
            self._tooltip_text = self.create_text(
                0,
                0,
                text=text,
                anchor="nw",
                font=("Helvetica", 9),
                fill=TEXT_PRIMARY,
                state="hidden",
            )
            self._tooltip_bg = self.create_rectangle(
                0,
                0,
                0,
                0,
                fill=BG_ACCENT,
                outline=BORDER,
                state="hidden",
            )
            self.tag_raise(self._tooltip_text, self._tooltip_bg)

        pad = 6
        tx = min(max(x + 14, 8), max(self.width - 180, 8))
        ty = min(max(y + 14, 8), max(self.height - 48, 8))
        self.itemconfigure(self._tooltip_text, text=text, state="normal")
        self.coords(self._tooltip_text, tx + pad, ty + pad)
        bbox = self.bbox(self._tooltip_text)
        if bbox:
            self.coords(self._tooltip_bg, bbox[0] - pad, bbox[1] - pad, bbox[2] + pad, bbox[3] + pad)
            self.itemconfigure(self._tooltip_bg, state="normal")
            self.tag_raise(self._tooltip_bg)
            self.tag_raise(self._tooltip_text)

    def _hide_tooltip(self) -> None:
        if self._tooltip_bg is not None:
            self.itemconfigure(self._tooltip_bg, state="hidden")
        if self._tooltip_text is not None:
            self.itemconfigure(self._tooltip_text, state="hidden")

    def _bind_hover(self, tag: str, tooltip_text: str) -> None:
        self.tag_bind(tag, "<Enter>", lambda event, text=tooltip_text: self._show_tooltip(event.x, event.y, text))
        self.tag_bind(tag, "<Motion>", lambda event, text=tooltip_text: self._show_tooltip(event.x, event.y, text))
        self.tag_bind(tag, "<Leave>", lambda _event: self._hide_tooltip())

    def draw_empty(self, message: str) -> None:
        self._render_state = ("empty", {"message": message})
        self._render_empty(message)

    def _render_empty(self, message: str) -> None:
        self.clear()
        self.create_text(self.width / 2, self.height / 2, text=message, fill=TEXT_MUTED, font=("Helvetica", 11))

    def draw_pie(self, values: pd.Series, title: str) -> None:
        self._render_state = ("pie", {"values": values.copy(), "title": title})
        self._render_pie(values, title)

    def _render_pie(self, values: pd.Series, title: str) -> None:
        self.clear()
        total = float(values.sum())
        self.create_text(self.width / 2, 18, text=title, font=("Helvetica", 12, "bold"), fill=TEXT_PRIMARY)
        if total <= 0:
            self._render_empty(f"{title}\nNo data in the selected range.")
            return

        chart_diameter = min(max(self.height - 90, 160), max((self.width * 0.44), 160))
        chart_diameter = min(chart_diameter, max(self.width - 220, 140))
        x1, y1 = 20, 40
        x2, y2 = x1 + chart_diameter, y1 + chart_diameter
        start = 0.0
        for idx, (category, amount) in enumerate(values.items()):
            if amount <= 0:
                continue
            extent = (amount / total) * 360.0
            color = CATEGORY_COLORS.get(category, "#cccccc")
            pct = 0 if total == 0 else (amount / total) * 100
            hover_tag = f"pie_hover_{idx}"
            tooltip_text = f"{category}\n{currency(float(amount))} ({pct:.1f}%)"
            self.create_arc(x1, y1, x2, y2, start=start, extent=extent, fill=color, outline=BG_PANEL, tags=(hover_tag,))
            self._bind_hover(hover_tag, tooltip_text)
            start += extent

        legend_x = x2 + 24
        legend_y = 50
        for idx, (category, amount) in enumerate(values.items()):
            color = CATEGORY_COLORS.get(category, "#cccccc")
            y = legend_y + idx * 24
            pct = 0 if total == 0 else (amount / total) * 100
            hover_tag = f"pie_hover_{idx}"
            tooltip_text = f"{category}\n{currency(float(amount))} ({pct:.1f}%)"
            self.create_rectangle(legend_x, y, legend_x + 14, y + 14, fill=color, outline=color, tags=(hover_tag,))
            self.create_text(
                legend_x + 22,
                y + 7,
                anchor="w",
                text=f"{category}: {currency(float(amount))} ({pct:.1f}%)",
                font=("Helvetica", 10),
                fill=TEXT_PRIMARY,
                tags=(hover_tag,),
            )
            self._bind_hover(hover_tag, tooltip_text)

    def draw_monthly_bars(self, income: pd.Series, expenses: pd.Series, title: str) -> None:
        self._render_state = ("monthly", {"income": income.copy(), "expenses": expenses.copy(), "title": title})
        self._render_monthly_bars(income, expenses, title)

    def _render_monthly_bars(self, income: pd.Series, expenses: pd.Series, title: str) -> None:
        self.clear()
        self.create_text(self.width / 2, 18, text=title, font=("Helvetica", 12, "bold"), fill=TEXT_PRIMARY)

        month_labels = list(dict.fromkeys(list(income.index) + list(expenses.index)))
        if not month_labels:
            self._render_empty(f"{title}\nNo data in the selected range.")
            return

        income = income.reindex(month_labels, fill_value=0.0)
        expenses = expenses.reindex(month_labels, fill_value=0.0)
        max_value = max(float(income.max()), float(expenses.max()), 0.0)
        if max_value <= 0:
            self._render_empty(f"{title}\nNo data in the selected range.")
            return

        left, right, top, bottom = 60, self.width - 20, 50, self.height - 55
        group_width = (right - left) / max(len(month_labels), 1)
        bar_width = min(28, max(group_width / 3, 10))

        ticks = 4
        self.create_line(left, top, left, bottom, fill=BORDER)
        self.create_line(left, bottom, right, bottom, fill=BORDER)
        for tick in range(ticks + 1):
            ratio = tick / ticks
            y = bottom - ((bottom - top - 20) * ratio)
            value = max_value * ratio
            self.create_line(left - 5, y, left, y, fill=BORDER)
            self.create_text(left - 10, y, text=currency(value), fill=TEXT_MUTED, font=("Helvetica", 8), anchor="e")

        self.create_rectangle(self.width - 180, 18, self.width - 166, 32, fill="#4c78a8", outline="#4c78a8")
        self.create_text(self.width - 160, 25, anchor="w", text="Income", fill=TEXT_PRIMARY, font=("Helvetica", 10))
        self.create_rectangle(self.width - 90, 18, self.width - 76, 32, fill="#e45756", outline="#e45756")
        self.create_text(self.width - 70, 25, anchor="w", text="Expenses", fill=TEXT_PRIMARY, font=("Helvetica", 10))

        for idx, label in enumerate(month_labels):
            center_x = left + idx * group_width + (group_width / 2)
            income_value = float(income.loc[label])
            expense_value = float(expenses.loc[label])
            income_height = ((bottom - top) - 20) * (income_value / max_value)
            expense_height = ((bottom - top) - 20) * (expense_value / max_value)

            income_tag = f"monthly_income_{idx}"
            self.create_rectangle(
                center_x - bar_width - 2,
                bottom - income_height,
                center_x - 2,
                bottom,
                fill="#4c78a8",
                outline="",
                tags=(income_tag,),
            )
            self._bind_hover(income_tag, f"{label}\nIncome: {currency(income_value)}")
            expense_tag = f"monthly_expense_{idx}"
            self.create_rectangle(
                center_x + 2,
                bottom - expense_height,
                center_x + bar_width + 2,
                bottom,
                fill="#e45756",
                outline="",
                tags=(expense_tag,),
            )
            self._bind_hover(expense_tag, f"{label}\nExpenses: {currency(expense_value)}")
            self.create_text(center_x, bottom + 14, text=label, fill=TEXT_PRIMARY, font=("Helvetica", 9))


def classify_income_subgroup(category: str, description: str) -> str:
    text = (description or "").lower()
    if category == "trips":
        if "car fee" in text:
            return "1. Car fee"
        return "2. Other trips"
    if category == "courses":
        if "pool session" in text:
            return "1. Pool session"
        if any(token in text for token in ["advanced", "aow", "rescue"]):
            return "2. Advanced / AOW / Rescue"
        return "3. Other courses"
    if category == "gear hire":
        if "gear deposit" in text:
            return "1. Gear deposit"
        if any(token in text for token in ["1 year full gear", "1/2 year full gear", "1/2 year full gear", "half year full gear"]):
            return "2. 1 year + 1/2 year gear hire"
        return "3. Other gear hire"
    return "All items"


def pretty_rule_label(pattern: str) -> str:
    label = str(pattern)
    label = label.replace("\\b", "")
    label = label.replace("\\s*", " ")
    label = label.replace("\\", "")
    label = re.sub(r"\s+", " ", label).strip()
    return label or pattern

class FinanceAnalyzerApp:
    def __init__(self, root: tk.Tk):
        self.root = root
        self.root.title(APP_NAME)
        self.root.geometry("1700x1100")
        self.root.configure(bg=BG_PRIMARY)

        self.settings = default_file_paths()
        self.analysis = AnalysisBundle(
            income=pd.DataFrame(),
            expenses=pd.DataFrame(),
            misc_income=pd.DataFrame(),
            misc_expenses=pd.DataFrame(),
        )
        self.filtered_income = pd.DataFrame()
        self.filtered_expenses = pd.DataFrame()

        self.income_rules_path = ensure_user_rule_file("income_rules.csv")
        self.expense_rules_path = ensure_user_rule_file("expense_rules.csv")
        self.period_var = tk.StringVar(value="All Dates")
        start_default = date(date.today().year, date.today().month, 1)
        end_default = date.today()
        self.start_var = tk.StringVar(value=start_default.isoformat())
        self.end_var = tk.StringVar(value=end_default.isoformat())
        self.selected_filter_year = date.today().year
        self.last_non_year_period = "All Dates"
        self.status_var = tk.StringVar(value="Load the source CSVs to begin.")
        self.transaction_view_var = tk.StringVar(value="All")
        self.stripe_latest_var = tk.StringVar(value=latest_entry_label("stripe", Path(self.settings["stripe"])))
        self.everyday_latest_var = tk.StringVar(value=latest_entry_label("everyday", Path(self.settings["everyday"])))

        self.income_total_var = tk.StringVar(value="$0.00")
        self.expense_total_var = tk.StringVar(value="$0.00")
        self.net_total_var = tk.StringVar(value="$0.00")
        self.misc_var = tk.StringVar(value="Misc review items: 0")

        self.build_ui()
        self.load_data()

    def configure_dark_theme(self) -> None:
        style = ttk.Style(self.root)
        if "clam" in style.theme_names():
            style.theme_use("clam")

        style.configure(".", background=BG_PRIMARY, foreground=TEXT_PRIMARY, fieldbackground=BG_INPUT)
        style.configure("TFrame", background=BG_PRIMARY)
        style.configure("TLabel", background=BG_PRIMARY, foreground=TEXT_PRIMARY)
        style.configure("TLabelframe", background=BG_PANEL, bordercolor=BORDER)
        style.configure("TLabelframe.Label", background=BG_PANEL, foreground=TEXT_PRIMARY)
        style.configure("TButton", background=BG_ACCENT, foreground=TEXT_PRIMARY, bordercolor=BORDER)
        style.map("TButton", background=[("active", SELECT_BG), ("pressed", SELECT_BG)])
        style.configure("TEntry", fieldbackground=BG_INPUT, foreground=TEXT_PRIMARY, insertcolor=TEXT_PRIMARY, bordercolor=BORDER)
        style.configure(
            "TCombobox",
            fieldbackground=BG_INPUT,
            background=BG_INPUT,
            foreground=TEXT_PRIMARY,
            arrowcolor=TEXT_PRIMARY,
            bordercolor=BORDER,
        )
        style.map("TCombobox", fieldbackground=[("readonly", BG_INPUT)], foreground=[("readonly", TEXT_PRIMARY)])
        style.configure("TSpinbox", fieldbackground=BG_INPUT, foreground=TEXT_PRIMARY, bordercolor=BORDER, arrowcolor=TEXT_PRIMARY)
        style.configure("TNotebook", background=BG_PRIMARY, borderwidth=0)
        style.configure("TNotebook.Tab", background=BG_ACCENT, foreground=TEXT_MUTED, padding=(10, 6))
        style.map("TNotebook.Tab", background=[("selected", BG_PANEL)], foreground=[("selected", TEXT_PRIMARY)])
        style.configure("Treeview", background=BG_INPUT, fieldbackground=BG_INPUT, foreground=TEXT_PRIMARY, bordercolor=BORDER, rowheight=24)
        style.configure("Treeview.Heading", background=BG_ACCENT, foreground=TEXT_PRIMARY, bordercolor=BORDER)
        style.map("Treeview", background=[("selected", SELECT_BG)], foreground=[("selected", SELECT_FG)])

        self.root.option_add("*TCombobox*Listbox.background", BG_INPUT)
        self.root.option_add("*TCombobox*Listbox.foreground", TEXT_PRIMARY)
        self.root.option_add("*TCombobox*Listbox.selectBackground", SELECT_BG)
        self.root.option_add("*TCombobox*Listbox.selectForeground", SELECT_FG)

    def build_ui(self) -> None:
        self.root.columnconfigure(0, weight=1)
        self.root.rowconfigure(0, weight=1)

        main = ttk.Frame(self.root, padding=12)
        main.grid(row=0, column=0, sticky="nsew")
        main.columnconfigure(0, weight=1)
        main.rowconfigure(1, weight=1)

        controls = ttk.LabelFrame(main, text="Filters", padding=10)
        controls.grid(row=0, column=0, sticky="ew")
        controls.columnconfigure(8, weight=1)

        ttk.Label(controls, text="Period").grid(row=0, column=0, sticky="w")
        self.period_combo = ttk.Combobox(
            controls,
            textvariable=self.period_var,
            values=[
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
            ],
            state="readonly",
            width=22,
        )
        self.period_combo.grid(row=0, column=1, sticky="w")
        self.period_combo.bind("<<ComboboxSelected>>", lambda _event: self.on_period_change())
        ttk.Button(controls, text="Apply Filter", command=self.apply_filters).grid(row=0, column=2, sticky="w", padx=(8, 0))

        self.custom_date_frame = ttk.Frame(controls)
        self.custom_date_frame.grid(row=0, column=3, columnspan=5, sticky="w", padx=(10, 0))
        ttk.Label(self.custom_date_frame, text="Start").grid(row=0, column=0, sticky="w")
        self.start_entry = ttk.Entry(self.custom_date_frame, textvariable=self.start_var, width=12)
        self.start_entry.grid(row=0, column=1, sticky="w", padx=(6, 0))
        ttk.Label(self.custom_date_frame, text="End").grid(row=0, column=2, sticky="w", padx=(10, 0))
        self.end_entry = ttk.Entry(self.custom_date_frame, textvariable=self.end_var, width=12)
        self.end_entry.grid(row=0, column=3, sticky="w", padx=(6, 0))

        links_frame = ttk.Frame(controls)
        links_frame.grid(row=0, column=9, sticky="e")
        stripe_link = tk.Label(
            links_frame,
            text="Open Stripe CSV",
            fg="#7db7ff",
            bg=BG_PANEL,
            cursor="hand2",
            font=("Helvetica", 10, "underline"),
        )
        stripe_link.grid(row=0, column=0, sticky="e")
        stripe_link.bind("<Button-1>", lambda _event: self.open_source_file("stripe"))
        stripe_meta = ttk.Label(links_frame, textvariable=self.stripe_latest_var)
        stripe_meta.grid(row=1, column=0, sticky="e")
        everyday_link = tk.Label(
            links_frame,
            text="Open Everyday CSV",
            fg="#7db7ff",
            bg=BG_PANEL,
            cursor="hand2",
            font=("Helvetica", 10, "underline"),
        )
        everyday_link.grid(row=0, column=1, sticky="e", padx=(12, 0))
        everyday_link.bind("<Button-1>", lambda _event: self.open_source_file("everyday"))
        everyday_meta = ttk.Label(links_frame, textvariable=self.everyday_latest_var)
        everyday_meta.grid(row=1, column=1, sticky="e", padx=(12, 0))

        notebook = ttk.Notebook(main)
        notebook.grid(row=1, column=0, sticky="nsew", pady=(10, 0))

        dashboard = ttk.Frame(notebook, padding=10)
        income_summary = ttk.Frame(notebook, padding=10)
        transactions = ttk.Frame(notebook, padding=10)
        rules = ttk.Frame(notebook, padding=10)

        notebook.add(dashboard, text="Dashboard")
        notebook.add(income_summary, text="Income Summary")
        notebook.add(transactions, text="Transactions")
        notebook.add(rules, text="Rule Tables")

        self.build_dashboard(dashboard)
        self.build_income_summary(income_summary)
        self.build_transactions(transactions)
        self.build_rules(rules)

        ttk.Label(main, textvariable=self.status_var).grid(row=2, column=0, sticky="w", pady=(8, 0))
        self.on_period_change()

    def build_dashboard(self, parent: ttk.Frame) -> None:
        parent.columnconfigure(0, weight=1)
        parent.rowconfigure(1, weight=1)

        summary = ttk.LabelFrame(parent, text="Summary", padding=10)
        summary.grid(row=0, column=0, sticky="ew")
        for idx in range(4):
            summary.columnconfigure(idx, weight=1)
        self._summary_metric(summary, 0, "Income", self.income_total_var)
        self._summary_metric(summary, 1, "Expenses", self.expense_total_var)
        self._summary_metric(summary, 2, "Net", self.net_total_var)
        self._summary_metric(summary, 3, "Misc Review", self.misc_var)

        content_pane = ttk.Panedwindow(parent, orient=tk.VERTICAL)
        content_pane.grid(row=1, column=0, sticky="nsew", pady=(10, 0))
        self.dashboard_pane = content_pane

        charts = ttk.Frame(content_pane)
        charts.rowconfigure(0, weight=1)
        charts.columnconfigure(0, weight=1)

        self.chart_notebook = ttk.Notebook(charts)
        self.chart_notebook.grid(row=0, column=0, sticky="nsew")

        pie_tab = ttk.Frame(self.chart_notebook, padding=4)
        pie_tab.columnconfigure(0, weight=1)
        pie_tab.columnconfigure(1, weight=1)
        pie_tab.rowconfigure(0, weight=1)

        self.income_chart = ChartCanvas(pie_tab, width=820, height=460)
        self.income_chart.grid(row=0, column=0, sticky="nsew", padx=(0, 6), pady=(0, 6))
        self.expense_chart = ChartCanvas(pie_tab, width=820, height=460)
        self.expense_chart.grid(row=0, column=1, sticky="nsew", padx=(6, 0), pady=(0, 6))

        monthly_tab = ttk.Frame(self.chart_notebook, padding=4)
        monthly_tab.columnconfigure(0, weight=1)
        monthly_tab.rowconfigure(0, weight=1)
        self.monthly_chart = ChartCanvas(monthly_tab, width=1600, height=460)
        self.monthly_chart.grid(row=0, column=0, sticky="nsew")

        self.chart_notebook.add(pie_tab, text="Pie Charts")
        self.chart_notebook.add(monthly_tab, text="Monthly Breakdown")

        lower = ttk.Frame(content_pane)
        lower.columnconfigure(0, weight=1)
        lower.columnconfigure(1, weight=1)
        lower.columnconfigure(2, weight=0)
        lower.rowconfigure(0, weight=1)

        income_breakdown = ttk.LabelFrame(lower, text="Filtered Income by Category", padding=10)
        income_breakdown.grid(row=0, column=0, sticky="nsew")
        income_breakdown.columnconfigure(0, weight=1)
        income_breakdown.rowconfigure(0, weight=1)
        self.income_breakdown_tree = ttk.Treeview(
            income_breakdown,
            columns=("date", "category", "identifier", "amount"),
            show="headings",
            height=14,
        )
        for name, width in (("date", 100), ("category", 110), ("identifier", 380), ("amount", 120)):
            self.income_breakdown_tree.heading(name, text=name.title())
            self.income_breakdown_tree.column(name, width=width, anchor="w" if name != "amount" else "e")
        self.income_breakdown_tree.grid(row=0, column=0, sticky="nsew")
        income_scroll = ttk.Scrollbar(income_breakdown, orient="vertical", command=self.income_breakdown_tree.yview)
        self.income_breakdown_tree.configure(yscrollcommand=income_scroll.set)
        income_scroll.grid(row=0, column=1, sticky="ns")

        expense_breakdown = ttk.LabelFrame(lower, text="Filtered Expenses by Category", padding=10)
        expense_breakdown.grid(row=0, column=1, sticky="nsew", padx=(12, 0))
        expense_breakdown.columnconfigure(0, weight=1)
        expense_breakdown.rowconfigure(0, weight=1)
        self.expense_breakdown_tree = ttk.Treeview(
            expense_breakdown,
            columns=("date", "category", "identifier", "amount"),
            show="headings",
            height=14,
        )
        for name, width in (("date", 100), ("category", 110), ("identifier", 380), ("amount", 120)):
            self.expense_breakdown_tree.heading(name, text=name.title())
            self.expense_breakdown_tree.column(name, width=width, anchor="w" if name != "amount" else "e")
        self.expense_breakdown_tree.grid(row=0, column=0, sticky="nsew")
        expense_scroll = ttk.Scrollbar(expense_breakdown, orient="vertical", command=self.expense_breakdown_tree.yview)
        self.expense_breakdown_tree.configure(yscrollcommand=expense_scroll.set)
        expense_scroll.grid(row=0, column=1, sticky="ns")

        category_frame = ttk.LabelFrame(lower, text="Category Selection", padding=10)
        category_frame.grid(row=0, column=2, sticky="nsew", padx=(12, 0))
        category_frame.columnconfigure(0, weight=1)
        category_frame.rowconfigure(1, weight=1)
        ttk.Label(category_frame, text="Select one or more categories to focus charts and tables.").grid(row=0, column=0, sticky="w")

        self.category_listbox = tk.Listbox(category_frame, selectmode=tk.MULTIPLE, exportselection=False, height=14)
        self.category_listbox.configure(
            bg=BG_INPUT,
            fg=TEXT_PRIMARY,
            selectbackground=SELECT_BG,
            selectforeground=SELECT_FG,
            highlightthickness=1,
            highlightbackground=BORDER,
            highlightcolor=BORDER,
            relief="flat",
        )
        for category in dict.fromkeys(INCOME_CATEGORIES + EXPENSE_CATEGORIES):
            self.category_listbox.insert(tk.END, category)
        self.category_listbox.grid(row=1, column=0, sticky="nsew", pady=(8, 0))
        self.category_listbox.bind("<<ListboxSelect>>", lambda _event: self.apply_filters())
        ttk.Button(category_frame, text="Clear Selection", command=self.clear_category_selection).grid(row=2, column=0, sticky="w", pady=(8, 0))

        content_pane.add(charts)
        content_pane.add(lower)
        parent.after(0, self.set_default_dashboard_split)

    def _summary_metric(self, parent: ttk.LabelFrame, column: int, label: str, variable: tk.StringVar) -> None:
        block = ttk.Frame(parent)
        block.grid(row=0, column=column, sticky="ew", padx=(0 if column == 0 else 8, 0))
        ttk.Label(block, text=label, font=("Helvetica", 10, "bold")).grid(row=0, column=0, sticky="w")
        ttk.Label(block, textvariable=variable).grid(row=1, column=0, sticky="w", pady=(4, 0))

    def build_income_summary(self, parent: ttk.Frame) -> None:
        parent.columnconfigure(0, weight=1)
        parent.rowconfigure(1, weight=1)

        info = ttk.LabelFrame(parent, text="Income Category Totals", padding=10)
        info.grid(row=0, column=0, sticky="ew")
        ttk.Label(
            info,
            text="Counts and totals follow the current filters. Trips and gear hire include extra subgroup breakdowns.",
        ).grid(row=0, column=0, sticky="w")

        self.income_summary_tree = ttk.Treeview(
            parent,
            columns=("category", "subgroup", "transactions", "total"),
            show="headings",
            height=22,
        )
        for name, width in (("category", 150), ("subgroup", 280), ("transactions", 120), ("total", 140)):
            self.income_summary_tree.heading(name, text=name.title())
            self.income_summary_tree.column(name, width=width, anchor="w" if name not in ("transactions", "total") else "e")
        self.income_summary_tree.grid(row=1, column=0, sticky="nsew", pady=(10, 0))
        scroll = ttk.Scrollbar(parent, orient="vertical", command=self.income_summary_tree.yview)
        self.income_summary_tree.configure(yscrollcommand=scroll.set)
        scroll.grid(row=1, column=1, sticky="ns", pady=(10, 0))

    def build_transactions(self, parent: ttk.Frame) -> None:
        parent.columnconfigure(0, weight=1)
        parent.rowconfigure(1, weight=1)

        controls = ttk.Frame(parent)
        controls.grid(row=0, column=0, sticky="ew")
        ttk.Label(controls, text="View").grid(row=0, column=0, sticky="w")
        ttk.Combobox(
            controls,
            textvariable=self.transaction_view_var,
            values=["All", "Income", "Expenses", "Income Misc", "Expense Misc"],
            width=16,
            state="readonly",
        ).grid(row=0, column=1, sticky="w", padx=(8, 0))
        ttk.Button(controls, text="Refresh Table", command=self.refresh_transactions).grid(row=0, column=2, sticky="w", padx=(8, 0))

        self.transaction_tree = ttk.Treeview(
            parent,
            columns=("date", "source", "category", "amount", "matched", "description"),
            show="headings",
        )
        headings = {
            "date": 110,
            "source": 130,
            "category": 120,
            "amount": 110,
            "matched": 80,
            "description": 760,
        }
        for column, width in headings.items():
            self.transaction_tree.heading(column, text=column.title())
            self.transaction_tree.column(column, width=width, anchor="w" if column != "amount" else "e")
        self.transaction_tree.grid(row=1, column=0, sticky="nsew", pady=(8, 0))
        scroll = ttk.Scrollbar(parent, orient="vertical", command=self.transaction_tree.yview)
        self.transaction_tree.configure(yscrollcommand=scroll.set)
        scroll.grid(row=1, column=1, sticky="ns", pady=(8, 0))

    def build_rules(self, parent: ttk.Frame) -> None:
        parent.columnconfigure(0, weight=1)
        parent.rowconfigure(0, weight=1)
        parent.rowconfigure(1, weight=1)

        self.income_rule_editor = RuleTableEditor(
            parent,
            "Income Rules (categories as columns)",
            self.income_rules_path,
            INCOME_CATEGORIES,
            self.on_rules_saved,
        )
        self.income_rule_editor.grid(row=0, column=0, sticky="nsew")

        self.expense_rule_editor = RuleTableEditor(
            parent,
            "Expense Rules (categories as columns)",
            self.expense_rules_path,
            EXPENSE_CATEGORIES,
            self.on_rules_saved,
        )
        self.expense_rule_editor.grid(row=1, column=0, sticky="nsew", pady=(10, 0))

    def on_rules_saved(self) -> None:
        self.status_var.set("Rules saved. Re-running categorisation.")
        self.load_data()

    def load_data(self) -> None:
        try:
            self.stripe_latest_var.set(latest_entry_label("stripe", Path(self.settings["stripe"])))
            self.everyday_latest_var.set(latest_entry_label("everyday", Path(self.settings["everyday"])))
            self.analysis = load_analysis(
                Path(self.settings["stripe"]),
                Path(self.settings["everyday"]),
                self.income_rules_path,
                self.expense_rules_path,
            )
            self.status_var.set("Data loaded. Adjust filters or rule tables as needed.")
            self.apply_filters()
        except FileNotFoundError as exc:
            self.status_var.set(f"Missing file: {exc}")
            messagebox.showerror(APP_NAME, f"Missing file:\n{exc}")
        except Exception as exc:  # pragma: no cover - UI safety
            self.status_var.set(f"Failed to load data: {exc}")
            messagebox.showerror(APP_NAME, f"Failed to load data:\n{exc}")

    def selected_categories(self) -> list[str]:
        selections = self.category_listbox.curselection()
        if not selections:
            return []
        return [self.category_listbox.get(index) for index in selections]

    def clear_category_selection(self) -> None:
        self.category_listbox.selection_clear(0, tk.END)
        self.apply_filters()

    def set_default_dashboard_split(self) -> None:
        if not hasattr(self, "dashboard_pane"):
            return
        try:
            total_height = self.dashboard_pane.winfo_height()
            if total_height <= 0:
                return
            desired_bottom_height = 230
            top_height = max(320, total_height - desired_bottom_height)
            top_height = min(top_height, max(total_height - 160, 200))
            self.dashboard_pane.sashpos(0, top_height)
        except tk.TclError:
            return

    def open_source_file(self, key: str) -> None:
        path = Path(self.settings[key]).resolve()
        if not path.exists():
            messagebox.showerror(APP_NAME, f"File not found:\n{path}")
            return
        webbrowser.open(path.as_uri())

    def on_period_change(self) -> None:
        current_mode = self.period_var.get()
        if current_mode == "Selected Year":
            selected = simpledialog.askinteger(
                "Select Year",
                "Enter calendar year to filter on:",
                initialvalue=self.selected_filter_year,
                minvalue=2000,
                maxvalue=2100,
                parent=self.root,
            )
            if selected is None:
                self.period_var.set(self.last_non_year_period)
                current_mode = self.period_var.get()
            else:
                self.selected_filter_year = selected
                self.apply_filters()
        else:
            self.last_non_year_period = current_mode

        if current_mode == "Custom":
            self.custom_date_frame.grid()
        else:
            self.custom_date_frame.grid_remove()

    def current_period_range(self) -> tuple[date | None, date | None]:
        return period_range(
            self.period_var.get(),
            self.start_var.get(),
            self.end_var.get(),
            self.selected_filter_year,
        )

    def apply_filters(self) -> None:
        start, end = self.current_period_range()
        self.filtered_income = filter_frame(self.analysis.income, start, end)
        self.filtered_expenses = filter_frame(self.analysis.expenses, start, end)

        selected = self.selected_categories()
        if selected:
            self.filtered_income = self.filtered_income[self.filtered_income["category"].isin(selected)]
            self.filtered_expenses = self.filtered_expenses[self.filtered_expenses["category"].isin(selected)]

        income_summary = summarize_categories(self.filtered_income, INCOME_CATEGORIES, [c for c in selected if c in INCOME_CATEGORIES])
        expense_summary = summarize_categories(self.filtered_expenses, EXPENSE_CATEGORIES, [c for c in selected if c in EXPENSE_CATEGORIES])

        self.income_chart.draw_pie(income_summary, "Income by Category")
        self.expense_chart.draw_pie(expense_summary, "Expenses by Category")

        total_income = float(self.filtered_income["amount"].sum()) if not self.filtered_income.empty else 0.0
        total_expenses = float(self.filtered_expenses["amount"].sum()) if not self.filtered_expenses.empty else 0.0
        self.income_total_var.set(currency(total_income))
        self.expense_total_var.set(currency(total_expenses))
        self.net_total_var.set(currency(total_income - total_expenses))

        misc_count = 0
        misc_count += len(filter_frame(self.analysis.misc_income, start, end))
        misc_count += len(filter_frame(self.analysis.misc_expenses, start, end))
        self.misc_var.set(f"Misc review items: {misc_count}")

        self.refresh_breakdown()
        self.refresh_income_summary()
        self.refresh_monthly_chart()
        self.refresh_transactions()

    def refresh_breakdown(self) -> None:
        self.populate_breakdown_tree(self.income_breakdown_tree, self.filtered_income, strip_purchase_prefix=True)
        self.populate_breakdown_tree(self.expense_breakdown_tree, self.filtered_expenses, strip_purchase_prefix=True)

    def populate_breakdown_tree(self, tree: ttk.Treeview, frame: pd.DataFrame, strip_purchase_prefix: bool = False) -> None:
        tree.delete(*tree.get_children())
        if frame.empty:
            return
        ordered = frame.sort_values(["category", "date", "amount"], ascending=[True, True, False])
        for _, row in ordered.iterrows():
            dt = pd.to_datetime(row.get("date"), errors="coerce")
            display_date = "" if pd.isna(dt) else dt.strftime("%Y-%m-%d")
            identifier = row.get("description") or row.get("reference") or ""
            if strip_purchase_prefix and isinstance(identifier, str):
                identifier = re.sub(r"^MUUC\s+(?:Ticketing\s+)?Purchase\s+Id:\s*\d+\s*-\s*", "", identifier, flags=re.IGNORECASE)
            tree.insert(
                "",
                "end",
                values=(
                    display_date,
                    row.get("category", ""),
                    identifier,
                    currency(float(row.get("amount", 0.0))),
                ),
            )

    def refresh_monthly_chart(self) -> None:
        income = self.filtered_income.copy()
        expenses = self.filtered_expenses.copy()
        if income.empty and expenses.empty:
            self.monthly_chart.draw_empty("Monthly Income vs Expenses\nNo data in the selected range.")
            return

        if not income.empty:
            income["month"] = pd.to_datetime(income["date"], errors="coerce").dt.to_period("M").astype(str)
            income_monthly = income.groupby("month")["amount"].sum().sort_index()
        else:
            income_monthly = pd.Series(dtype="float64")

        if not expenses.empty:
            expenses["month"] = pd.to_datetime(expenses["date"], errors="coerce").dt.to_period("M").astype(str)
            expense_monthly = expenses.groupby("month")["amount"].sum().sort_index()
        else:
            expense_monthly = pd.Series(dtype="float64")

        selected = self.selected_categories()
        if selected:
            title = f"Monthly Income vs Expenses ({', '.join(selected)})"
        else:
            title = "Monthly Income vs Expenses (All Categories)"
        self.monthly_chart.draw_monthly_bars(income_monthly, expense_monthly, title)

    def refresh_income_summary(self) -> None:
        self.income_summary_tree.delete(*self.income_summary_tree.get_children())
        if self.filtered_income.empty:
            return

        income_rules = load_rule_table(self.income_rules_path, INCOME_CATEGORIES)
        membership_patterns = [value.strip() for value in income_rules["memberships"].tolist() if str(value).strip()]
        compiled_membership_patterns: list[tuple[str, re.Pattern[str]]] = []
        for pattern in membership_patterns:
            try:
                compiled_membership_patterns.append((pattern, re.compile(pattern, re.IGNORECASE)))
            except re.error:
                continue

        grouped = self.filtered_income.groupby("category", dropna=False)
        for category, frame in grouped:
            total = float(frame["amount"].sum())
            count = int(len(frame))
            self.income_summary_tree.insert(
                "",
                "end",
                values=(category, "All items", count, currency(total)),
            )

            if category == "memberships":
                for pattern, compiled in compiled_membership_patterns:
                    subframe = frame[frame["description"].fillna("").map(lambda value, rx=compiled: bool(rx.search(value)))]
                    if subframe.empty:
                        continue
                    self.income_summary_tree.insert(
                        "",
                        "end",
                        values=(
                            "",
                            pretty_rule_label(pattern),
                            int(len(subframe)),
                            currency(float(subframe["amount"].sum())),
                        ),
                    )
                continue

            if category not in {"trips", "gear hire", "courses"}:
                continue

            subgroup_frame = frame.copy()
            subgroup_frame["subgroup"] = subgroup_frame["description"].map(
                lambda value, cat=category: classify_income_subgroup(cat, value)
            )
            subgroup_grouped = subgroup_frame.groupby("subgroup", dropna=False)
            for subgroup, subframe in subgroup_grouped:
                self.income_summary_tree.insert(
                    "",
                    "end",
                    values=(
                        "",
                        subgroup,
                        int(len(subframe)),
                        currency(float(subframe["amount"].sum())),
                    ),
                )

    def refresh_transactions(self) -> None:
        self.transaction_tree.delete(*self.transaction_tree.get_children())
        view = self.transaction_view_var.get()

        if view == "Income":
            frame = self.filtered_income
        elif view == "Expenses":
            frame = self.filtered_expenses
        elif view == "Income Misc":
            frame = filter_frame(self.analysis.misc_income, *self.current_period_range())
        elif view == "Expense Misc":
            frame = filter_frame(self.analysis.misc_expenses, *self.current_period_range())
        else:
            frame = pd.concat([self.filtered_income, self.filtered_expenses], ignore_index=True, sort=False).sort_values("date")

        for _, row in frame.tail(500).iterrows():
            dt = pd.to_datetime(row.get("date"), errors="coerce")
            display_date = "" if pd.isna(dt) else dt.strftime("%Y-%m-%d")
            self.transaction_tree.insert(
                "",
                "end",
                values=(
                    display_date,
                    row.get("source", ""),
                    row.get("category", ""),
                    currency(float(row.get("amount", 0.0))),
                    "Yes" if bool(row.get("matched")) else "No",
                    row.get("description", ""),
                ),
            )


def print_cli_summary() -> None:
    bundle = load_analysis(
        Path(default_file_paths()["stripe"]),
        Path(default_file_paths()["everyday"]),
        ensure_user_rule_file("income_rules.csv"),
        ensure_user_rule_file("expense_rules.csv"),
    )
    income = bundle.income.groupby("category")["amount"].sum().reindex(INCOME_CATEGORIES, fill_value=0.0)
    expenses = bundle.expenses.groupby("category")["amount"].sum().reindex(EXPENSE_CATEGORIES, fill_value=0.0)

    print("Income by category")
    for category, amount in income.items():
        print(f"  {category:14} {currency(float(amount))}")
    print(f"  {'TOTAL':14} {currency(float(income.sum()))}")
    print()
    print("Expenses by category")
    for category, amount in expenses.items():
        print(f"  {category:14} {currency(float(amount))}")
    print(f"  {'TOTAL':14} {currency(float(expenses.sum()))}")
    print()
    print(f"Misc income rows: {len(bundle.misc_income)}")
    print(f"Misc expense rows: {len(bundle.misc_expenses)}")


def main() -> None:
    import sys

    if "--summary" in sys.argv:
        print_cli_summary()
        return

    root = tk.Tk()
    app = FinanceAnalyzerApp(root)
    app.configure_dark_theme()
    root.minsize(1400, 900)
    root.mainloop()


if __name__ == "__main__":
    main()
