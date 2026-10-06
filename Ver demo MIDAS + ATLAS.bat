@echo off
chcp 65001 >nul
title MIDAS + ATLAS - Demo
cd /d "%~dp0"
echo.
echo   Iniciando la demo de MIDAS + ATLAS...
echo.

rem ---- 1. Buscar Python 3.11 o mas reciente ----
set "PY="
for %%V in (3.13 3.12 3.11) do (
  if not defined PY ( py -%%V -c "import sys" >nul 2>&1 && set "PY=py -%%V" )
)
if not defined PY (
  python -c "import sys; sys.exit(0 if sys.version_info >= (3, 11) else 1)" >nul 2>&1 && set "PY=python"
)
for %%V in (313 312 311) do (
  if not defined PY if exist "%LOCALAPPDATA%\Programs\Python\Python%%V\python.exe" set PY="%LOCALAPPDATA%\Programs\Python\Python%%V\python.exe"
)
if defined PY goto python_listo

rem ---- 2. Si no hay Python, instalarlo (solo la primera vez) ----
echo   No encontre Python 3.11 o superior. Lo voy a instalar, puede tardar unos minutos...
echo.
winget install -e --id Python.Python.3.12 --scope user --silent --accept-package-agreements --accept-source-agreements
if exist "%LOCALAPPDATA%\Programs\Python\Python312\python.exe" set PY="%LOCALAPPDATA%\Programs\Python\Python312\python.exe"
if defined PY goto python_listo

echo.
echo   No pude instalar Python automaticamente.
echo   Se abrira la pagina de descarga: instalalo, marca "Add python.exe to PATH"
echo   y vuelve a abrir este archivo.
start "" https://www.python.org/downloads/
pause
exit /b 1

:python_listo
rem ---- 3. Preparar, crear el acceso directo y arrancar ----
%PY% run.py --show %*
if errorlevel 1 (
  echo.
  echo   Algo fallo. Toma una foto de esta ventana para revisarlo.
  pause
)
exit /b %errorlevel%
