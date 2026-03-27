@echo off
setlocal

python -m pip install -r requirements.txt
pyinstaller --clean --noconfirm Sims2CCDiagnostics.spec

echo.
echo Build complete.
echo EXE location: dist\Sims2CCDiagnostics.exe
