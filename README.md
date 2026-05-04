# MUUC Finance Analyzer


Monthly report and releases here: https://github.com/sek0002/MUUCfinance/releases

Desktop app for categorising MUUC Stripe income and MUUC everyday-account expenses from the source CSV exports.

## What it does

- Loads the fixed source CSV inputs from `source/stripe.csv` and `source/everyday.csv`.
- Removes file-upload controls; the app always reads the bundled `source/` files.
- Breaks Stripe income into the requested categories: `air`, `courses`, `gear hire`, `memberships`, `social`, `trips`, `specialtrips`, `misc`.
- Breaks expenses into the requested categories: `fees`, `air`, `car/boat`, `compressor`, `courses`, `gear`, `refunds`, `social`, `specialtrips`, `trips`, `misc`.
- Uses editable regex rule tables stored as CSV files with categories as columns.
- On first run, copies the bundled rule tables into `~/.muuc_finance_analyzer/config/` so packaged apps keep using your saved rule changes.
- Pushes unmatched rows into `misc` so they can be reviewed and reclassified by editing the rule tables.
- Supports date filtering with:
  - custom between-dates via year/month/day spinboxes
  - month to date
  - year to date
  - financial year to date
  - last 30 days
  - selected month
  - selected year
  - selected financial year
- Uses only debit-side everyday-account entries (negative `Amount` rows) as expense inputs.
- Shows pie charts for income/expenses, transaction-level category breakdown, and transaction review tables.

## Files

- App: `/Users/sekkevin/LocalR/MUUC/muuc_finance_app.py`
- PyInstaller build script: `/Users/sekkevin/LocalR/MUUC/build_pyinstaller.py`
- Income rules: `/Users/sekkevin/LocalR/MUUC/config/income_rules.csv`
- Expense rules: `/Users/sekkevin/LocalR/MUUC/config/expense_rules.csv`

## Run locally

```bash
python3 -m pip install -r requirements.txt
python3 muuc_finance_app.py
```

For a terminal-only summary check:

```bash
python3 muuc_finance_app.py --summary
```

## Run the hosted web app

The repo also includes a FastAPI-hosted version with username/password plus authenticator-app login.

1. Generate a TOTP secret and provisioning URI:

```bash
python3 muuc_finance_web.py --init-auth
```

2. Set the required environment variables:

```bash
export MUUC_WEB_USERNAME="sek0002"
export MUUC_WEB_PASSWORD="choose-a-strong-password"
export MUUC_TOTP_SECRET="paste-secret-from-init-auth"
export MUUC_SESSION_SECRET="choose-a-long-random-session-secret"
export MUUC_WEB_DATA_DIR="$HOME/.muuc_finance_analyzer/web"
```

3. Start the server:

```bash
python3 muuc_finance_web.py --host 0.0.0.0 --port 8000
```

For development with auto-reload:

```bash
python3 muuc_finance_web.py --host 127.0.0.1 --port 8000 --reload
```

The hosted app lets you:

- sign in with password plus 6-digit TOTP code from an authenticator app
- upload replacement `stripe.csv`, `teamapp.csv`, and `everyday.csv`
- download the current source CSVs
- apply the same date and category filters
- export the filtered transactions table as CSV

Uploaded source CSVs are stored persistently in `MUUC_WEB_DATA_DIR/source/`.
If `MUUC_WEB_DATA_DIR` is not set, the default path is `~/.muuc_finance_analyzer/web/source/`.
On first run, the hosted app seeds that folder from the bundled `source/` defaults.

Hosted rule tables are stored persistently in `MUUC_WEB_DATA_DIR/config/`.
If `MUUC_WEB_DATA_DIR` is not set, the default path is `~/.muuc_finance_analyzer/web/config/`.
On first run, the hosted app seeds that folder from the bundled `config/` defaults.

## Build standalone apps

macOS:

```bash
./build_mac.sh
```

Windows:

```bat
build_windows.bat
```

You can also run the shared builder directly on either platform:

```bash
python3 build_pyinstaller.py
```

The build script first syncs your active saved rules from `~/.muuc_finance_analyzer/config/` into the project `config/`, then runs PyInstaller using local temporary build folders (to avoid Windows file-lock issues on shared/network paths) and prints the final packaged app location. The current `config/` and `source/` folders are bundled into the executable.
# muuc_finance
