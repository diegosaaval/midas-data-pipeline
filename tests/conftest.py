import json
from datetime import date
from pathlib import Path

import pytest

from midas.config import PROJECT_ROOT, Settings
from midas.generator import write_landing
from midas.pipeline import Pipeline, date_range
from midas.spark import get_spark

EPOCH = date(2026, 9, 1)
DAYS = date_range(EPOCH, date(2026, 9, 5))


def make_settings(tmp: Path, **kw) -> Settings:
    base = dict(lake=str(tmp / "lake"), landing=str(tmp / "landing"), meta_db=str(tmp / "meta" / "midas.db"),
                duckdb=str(tmp / "warehouse" / "midas.duckdb"), seed=7, scale=0.05, epoch=EPOCH,
                schema_v2_from=date(2026, 9, 3), late_data_days=7, max_retries=2, shuffle_partitions=2,
                dbt_dir=str(PROJECT_ROOT / "dbt"))
    return Settings(**(base | kw))


@pytest.fixture(scope="session")
def spark():
    s = get_spark(make_settings(Path("/tmp")), app="midas-tests")
    yield s
    s.stop()


@pytest.fixture(scope="session")
def lake(spark, tmp_path_factory):
    """Pipeline completo (bronze -> silver -> features -> dbt -> publish) sobre 5 días, compartido por los módulos."""
    s = make_settings(tmp_path_factory.mktemp("e2e"))
    for d in DAYS:
        write_landing(d, s)
    pipe = Pipeline(s, spark)
    pipe.backoff_seconds = 0
    run_id = pipe.run_incremental()
    yield s, pipe, run_id
    pipe.close()


@pytest.fixture
def settings(tmp_path) -> Settings:
    return make_settings(tmp_path)


def land(settings: Settings, entity: str, day: date, rows: list[dict | str]) -> None:
    """Deja un archivo en landing con filas construidas a mano (str = línea cruda, p. ej. JSON corrupto)."""
    folder = Path(settings.landing) / entity / f"ingest_date={day.isoformat()}"
    folder.mkdir(parents=True, exist_ok=True)
    lines = [r if isinstance(r, str) else json.dumps(r) for r in rows]
    (folder / "part-00000.jsonl").write_text("\n".join(lines) + "\n", encoding="utf-8")


def payment(pid: str, tx: str, **kw) -> dict:
    row = {"payment_id": pid, "customer_id": "C1", "merchant_id": "M0001", "amount": 1000.0,
           "payment_method": "card", "transaction_date": tx, "status": "approved", "updated_at": tx}
    return row | kw
