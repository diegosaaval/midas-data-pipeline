.PHONY: install demo show incidente run status ui test lint dbt-docs clean

install:        ## entorno local (requiere Python 3.11-3.13 y Java 17+)
	uv venv --python 3.12 .venv && uv pip install --python .venv -e ".[dev]"

demo:           ## desde cero: genera 30 días de fuentes y corre el pipeline completo
	.venv/bin/finflow reset
	.venv/bin/finflow generate --start 2026-09-01 --end 2026-09-30
	.venv/bin/finflow run
	.venv/bin/finflow status --limit 1

show:           ## demo en vivo de FINFLOW + ATLAS para presentar (Enter en cada paso; make show AUTO=1 para grabar)
	.venv/bin/finflow show $(if $(AUTO),--auto,)

incidente:      ## llega el 1 de octubre con la pasarela de tarjetas caída: FINFLOW lo publica, ATLAS lo detecta
	.venv/bin/finflow generate --start 2026-10-01 --end 2026-10-01 --anomaly approval_drop
	.venv/bin/finflow run

run:
	.venv/bin/finflow run

status:
	.venv/bin/finflow status

ui:             ## pantalla de etapas en http://localhost:8100
	.venv/bin/finflow ui

test:
	.venv/bin/pytest --cov=finflow --cov-report=term-missing

lint:
	.venv/bin/ruff check .

dbt-docs:       ## documentación navegable de los modelos dbt
	cd dbt && FINFLOW_LAKE=../data/lake ../.venv/bin/dbt docs generate --profiles-dir . && ../.venv/bin/dbt docs serve --profiles-dir .

clean:
	rm -rf data dbt/target dbt/logs .pytest_cache
