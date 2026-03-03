from __future__ import annotations

import shutil
import subprocess
import sys
import tempfile
from pathlib import Path


APP_NAME = "MUUC Finance Analyzer"
BASE_DIR = Path(__file__).resolve().parent
PROJECT_CONFIG_DIR = BASE_DIR / "config"
PROJECT_SOURCE_DIR = BASE_DIR / "source"
APP_SCRIPT = "muuc_finance_app.py"
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


def build_root() -> Path:
    return Path(tempfile.gettempdir()) / "muuc_finance_analyzer_pyinstaller"


def clean_local_build_dirs() -> tuple[Path, Path, Path]:
    root = build_root()
    dist_dir = root / "dist"
    work_dir = root / "build"
    spec_dir = root / "spec"
    stage_dir = root / "stage"
    for path in (dist_dir, work_dir, spec_dir, stage_dir):
        if path.exists():
            shutil.rmtree(path, ignore_errors=True)
        path.mkdir(parents=True, exist_ok=True)
    return dist_dir, work_dir, spec_dir


def prepare_stage_dir() -> Path:
    root = build_root()
    stage_dir = root / "stage"
    if stage_dir.exists():
        shutil.rmtree(stage_dir, ignore_errors=True)
    stage_dir.mkdir(parents=True, exist_ok=True)

    shutil.copyfile(BASE_DIR / APP_SCRIPT, stage_dir / APP_SCRIPT)
    shutil.copytree(PROJECT_CONFIG_DIR, stage_dir / "config", dirs_exist_ok=True)
    shutil.copytree(PROJECT_SOURCE_DIR, stage_dir / "source", dirs_exist_ok=True)
    return stage_dir


def find_artifact(dist_dir: Path) -> Path:
    candidates = [
        dist_dir / APP_NAME,
        dist_dir / f"{APP_NAME}.app",
        dist_dir / f"{APP_NAME}.exe",
    ]
    for candidate in candidates:
        if candidate.exists():
            return candidate
    return dist_dir


def build_with_pyinstaller() -> None:
    data_separator = ";" if sys.platform.startswith("win") else ":"
    dist_dir, work_dir, spec_dir = clean_local_build_dirs()
    stage_dir = prepare_stage_dir()
    stage_config_dir = stage_dir / "config"
    stage_source_dir = stage_dir / "source"
    subprocess.run(
        [
            sys.executable,
            "-m",
            "PyInstaller",
            "--noconfirm",
            "--clean",
            "--windowed",
            "--name",
            APP_NAME,
            "--distpath",
            str(dist_dir),
            "--workpath",
            str(work_dir),
            "--specpath",
            str(spec_dir),
            "--add-data",
            f"{stage_config_dir}{data_separator}config",
            "--add-data",
            f"{stage_source_dir}{data_separator}source",
            APP_SCRIPT,
        ],
        cwd=stage_dir,
        check=True,
    )
    artifact = find_artifact(dist_dir)
    print(f"Build complete: {artifact}")


def main() -> None:
    sync_current_rules()
    install_requirements()
    build_with_pyinstaller()


if __name__ == "__main__":
    main()
