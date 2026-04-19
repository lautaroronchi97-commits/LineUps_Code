@echo off
REM Version silenciosa de actualizar_ahora.bat para el Programador de Tareas.
REM No tiene pause (correria eternamente esperando una tecla que nunca llega).
REM Loguea stdout+stderr a logs\scheduled_update.log con timestamp.

cd /d "%~dp0"

if not exist "logs" mkdir logs

echo. >> logs\scheduled_update.log
echo ================================================== >> logs\scheduled_update.log
echo Corrida iniciada: %date% %time% >> logs\scheduled_update.log
echo ================================================== >> logs\scheduled_update.log

if not exist ".venv\Scripts\activate.bat" (
    echo ERROR: no encontre el virtual environment en .venv\ >> logs\scheduled_update.log
    exit /b 1
)

call .venv\Scripts\activate.bat
python update_today.py >> logs\scheduled_update.log 2>&1
set EXITCODE=%ERRORLEVEL%

echo Corrida terminada: %date% %time% (exit=%EXITCODE%) >> logs\scheduled_update.log
exit /b %EXITCODE%
