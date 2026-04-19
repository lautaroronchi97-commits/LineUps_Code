@echo off
REM Acceso directo: doble click desde Windows Explorer para abrir el dashboard.
REM Activa el venv, corre streamlit y abre el browser automaticamente.

cd /d "%~dp0"

if not exist ".venv\Scripts\activate.bat" (
    echo ERROR: no encontre el virtual environment en .venv\
    echo Corre primero:
    echo    python -m venv .venv
    echo    .venv\Scripts\activate
    echo    pip install -r requirements.txt
    pause
    exit /b 1
)

if not exist ".env" (
    echo ERROR: falta el archivo .env con las credenciales de Supabase.
    echo Copia .env.example a .env y completalo.
    pause
    exit /b 1
)

call .venv\Scripts\activate.bat
echo.
echo Abriendo dashboard en http://localhost:8501 ...
echo (Cerra esta ventana con Ctrl+C para apagar el dashboard)
echo.
streamlit run dashboard.py
pause
