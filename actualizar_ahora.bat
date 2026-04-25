@echo off
REM Acceso directo para correr los updates manualmente.
REM Re-scrapea hoy + los ultimos 3 dias (ISA) y actualiza DJVE del MAGyP.

cd /d "%~dp0"

if not exist ".venv\Scripts\activate.bat" (
    echo ERROR: no encontre el virtual environment.
    pause
    exit /b 1
)

call .venv\Scripts\activate.bat

echo === ISA (line-up) ===
python update_today.py
echo.
echo === DJVE (MAGyP) ===
python update_djve.py
echo.
echo Listo. Presiona una tecla para cerrar.
pause
