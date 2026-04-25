@echo off
REM Version silenciosa para el Programador de Tareas de Windows.
REM Corre dos updates en secuencia:
REM   1. update_today.py  -> line-up ISA (hoy + 3 dias atras)
REM   2. update_djve.py   -> DJVE acumuladas del MAGyP (ano corriente)
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

REM ---- Paso 1: line-up ISA (puerto) ----
echo --- update_today.py (ISA line-up) --- >> logs\scheduled_update.log
python update_today.py >> logs\scheduled_update.log 2>&1
set EXIT_ISA=%ERRORLEVEL%

REM ---- Paso 2: DJVE MAGyP ----
REM Corre aunque ISA haya fallado: son fuentes independientes y no queremos
REM que un timeout en isa-agents.com.ar bloquee la actualizacion de DJVE.
echo. >> logs\scheduled_update.log
echo --- update_djve.py (DJVE MAGyP) --- >> logs\scheduled_update.log
python update_djve.py >> logs\scheduled_update.log 2>&1
set EXIT_DJVE=%ERRORLEVEL%

REM Exit code: 0 si AL MENOS UNA fuente actualizo OK; 1 si las dos fallaron.
if %EXIT_ISA% EQU 0 (set EXITCODE=0) else (set EXITCODE=%EXIT_DJVE%)

echo. >> logs\scheduled_update.log
echo Corrida terminada: %date% %time% (ISA=%EXIT_ISA% DJVE=%EXIT_DJVE% final=%EXITCODE%) >> logs\scheduled_update.log
exit /b %EXITCODE%
