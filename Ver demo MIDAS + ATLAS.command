#!/bin/bash
# MIDAS + ATLAS · doble clic para ver la demo de los dos proyectos (instala todo la primera vez).
cd "$(dirname "$0")" || exit 1
echo
echo "  Iniciando la demo de MIDAS + ATLAS…"
echo

PY=""
for cand in python3.13 python3.12 python3.11 python3 python; do
  if command -v "$cand" >/dev/null 2>&1 && "$cand" -c 'import sys; sys.exit(0 if sys.version_info >= (3, 11) else 1)' 2>/dev/null; then
    PY="$cand"; break
  fi
done

if [ -z "$PY" ]; then
  echo "  Necesitas Python 3.11 o más reciente."
  if command -v brew >/dev/null 2>&1; then
    read -r -p "  ¿Lo instalo con Homebrew? [s/N] " ans
    if [[ "$ans" =~ ^[sSyY]$ ]]; then brew install python@3.12 && PY="$(brew --prefix)/bin/python3.12"; fi
  fi
  if [ -z "$PY" ]; then
    echo "  Se abrirá la página de descarga: instálalo y vuelve a abrir este archivo."
    open "https://www.python.org/downloads/macos/" 2>/dev/null || true
    read -r -p "  Presiona Enter para cerrar…" _
    exit 1
  fi
fi

"$PY" run.py --show "$@"
status=$?
[ $status -ne 0 ] && read -r -p "  Presiona Enter para cerrar…" _
exit $status
