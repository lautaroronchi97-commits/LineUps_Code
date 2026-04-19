@echo off
REM Acceso directo para correr update_today.py manualmente.
REM Re-scrapea hoy + los ultimos 3 dias y actualiza Supabase.

cd /d "%~dp0"

if not exist ".venv\Scripts\activate.bat" (
    echo ERROR: no encontre el virtual environment.
    pause
    exit /b 1
)

call .venv\Scripts\activate.bat
python update_today.py
echo.
echo Listo. Presiona una tecla para cerrar.
pause
