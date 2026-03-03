from __future__ import annotations

import shutil
import subprocess
import sys
from pathlib import Path


APP_NAME = "MUUC Finance Analyzer"
BASE_DIR = Path(__file__).resolve().parent
PROJECT_CONFIG_DIR = BASE_DIR / "config"
PROJECT_SOURCE_DIR = BASE_DIR / "source"
USER_CONFIG_DIR = Path.home() / ".muuc_finance_analyzer" / "config"
RULE_FILES = ("income_rules.csv", "expense_rules.csv")


def sync_current_rules() -> None:
    PROJECT_CONFIG_DIR.mkdir(parents=True, exist_ok=True)
    for filename in RULE_FILES:
        user_path = USER_CONFIG_DIR / filename
        project_path = PROJECT_CONFIG_DIR / filename
        if user_path.exists():
            shutil.copyfile(user_path, project_path)


def install_requirements() -> None:
    subprocess.run(
        [sys.executable, "-m", "pip", "install", "-r", "requirements.txt"],
        cwd=BASE_DIR,
        check=True,
    )


def build_with_pyinstaller() -> None:
    data_separator = ";" if sys.platform.startswith("win") else ":"
    subprocess.run(
        [
            sys.executable,
            "-m",
            "PyInstaller",
            "--noconfirm",
            "--windowed",
            "--name",
            APP_NAME,
            "--add-data",
            f"{PROJECT_CONFIG_DIR}{data_separator}config",
            "--add-data",
            f"{PROJECT_SOURCE_DIR}{data_separator}source",
            "muuc_finance_app.py",
        ],
        cwd=BASE_DIR,
        check=True,
    )


def main() -> None:
    sync_current_rules()
    install_requirements()
    build_with_pyinstaller()


if __name__ == "__main__":
    main()
