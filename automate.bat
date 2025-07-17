@echo off
cd /d "%~dp0"

:: ───────── 1.  Create venv once ─────────────────────────
if not exist env\Scripts\python.exe (
    echo [setup] Creating virtual environment...
    python -m venv env
)

:: ───────── 2.  Activate it ──────────────────────────────
call env\Scripts\activate.bat

:: ───────── 3.  Install deps only the FIRST time
::            (flag file env\.deps_ok tells us we’re done)
if not exist env\.deps_ok (
    echo [setup] Installing Python packages...
    pip install -r requirements.txt

    echo done> env\.deps_ok
)

:: ───────── 4.  Run your code ────────────────────────────
echo [run] python main.py
python main.py

:: ───────── 5.  Deactivate & pause ───────────────────────
deactivate
pause
