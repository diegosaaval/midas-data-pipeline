.PHONY: install demo run status test lint dbt-docs clean

install:        ## entorno local (requiere Python 3.11-3.13 y Java 17+)
	uv venv --python 3.12 .venv && uv pip install --python .venv -e ".[dev]"

demo:           ## genera 30 días de fuentes y corre el pipeline completo
	.venv/bin/finflow generate --start 2026-09-01 --end 2026-09-30
	.venv/bin/finflow run
	.venv/bin/finflow status --limit 1

run:
	.venv/bin/finflow run

status:
	.venv/bin/finflow status

test:
	.venv/bin/pytest --cov=finflow --cov-report=term-missing

lint:
	.venv/bin/ruff check .

dbt-docs:       ## documentación navegable de los modelos dbt
	cd dbt && FINFLOW_LAKE=../data/lake ../.venv/bin/dbt docs generate --profiles-dir . && ../.venv/bin/dbt docs serve --profiles-dir .

clean:
	rm -rf data dbt/target dbt/logs .pytest_cache
