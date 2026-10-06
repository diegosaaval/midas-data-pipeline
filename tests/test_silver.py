from datetime import date
from pathlib import Path

from conftest import land, payment
from pyspark.sql import functions as F

from midas.jobs import bronze, silver
from midas.metadata import Metadata
from midas.spark import explain_string

D1, D2, D3 = date(2026, 9, 10), date(2026, 9, 11), date(2026, 9, 12)
MERCHANT = {"merchant_id": "M0001", "name": "x", "category": "marketplace", "city": "Bogotá", "status": "active",
            "updated_at": "2026-09-01T00:00:00"}


def process(spark, s, entity, day, meta=None):
    bronze.run(spark, s, entity, day, meta)
    return silver.run(spark, s, entity, day)


def silver_df(spark, s, entity):
    return spark.read.parquet(s.path("silver", entity))


def quarantine_reasons(spark, s, entity):
    df = spark.read.parquet(s.path("quarantine", entity))
    return sorted(r for row in df.collect() for r in row["_reasons"])


def setup_merchants(spark, s, day=D1):
    land(s, "merchants", day, [MERCHANT])
    process(spark, s, "merchants", day)


def test_contract_violations_go_to_quarantine_with_reasons(spark, settings):
    setup_merchants(spark, settings)
    land(settings, "payments", D1, [
        payment("P1", "2026-09-10T10:00:00"),
        payment("P2", "2026-09-10T10:00:00", amount=-5),
        payment("P3", "2026-09-10T10:00:00", customer_id=None),
        payment("P4", "2026-09-10T10:00:00", payment_method="crypto"),
        payment("P5", "2026-09-10T10:00:00", amount="mil pesos"),
        payment("P6", "2026-09-10T10:00:00", merchant_id="M9999"),
        '{"payment_id": "P7", "amount": 1',  # JSON corrupto
    ])
    result = process(spark, settings, "payments", D1)
    assert silver_df(spark, settings, "payments").where("dt = '2026-09-10'").count() == 1
    assert result.rows_quarantined == 6
    assert quarantine_reasons(spark, settings, "payments") == sorted([
        "amount:below_min", "customer_id:missing", "payment_method:not_allowed", "amount:invalid_type",
        "merchant_id:unknown", "record:malformed_json"])


def test_dedup_keeps_latest_version_by_business_key(spark, settings):
    setup_merchants(spark, settings)
    land(settings, "payments", D1, [
        payment("P1", "2026-09-10T10:00:00", status="pending"),
        payment("P1", "2026-09-10T10:00:00", status="pending"),  # reenvío exacto
    ])
    process(spark, settings, "payments", D1)
    land(settings, "payments", D2, [payment("P1", "2026-09-10T10:00:00", status="approved",
                                            updated_at="2026-09-11T08:00:00")])
    result = process(spark, settings, "payments", D2)
    rows = silver_df(spark, settings, "payments").where("payment_id = 'P1'").collect()
    assert len(rows) == 1 and rows[0]["status"] == "approved"
    assert result.partitions == ["dt=2026-09-10"], "la actualización vuelve a la partición original"


def test_late_data_is_merged_into_its_partition_without_touching_others(spark, settings):
    setup_merchants(spark, settings)
    land(settings, "payments", D1, [payment("P1", "2026-09-10T10:00:00")])
    process(spark, settings, "payments", D1)
    land(settings, "payments", D2, [payment("P2", "2026-09-11T10:00:00")])
    process(spark, settings, "payments", D2)
    d11 = Path(settings.lake) / "silver" / "payments" / "dt=2026-09-11"
    before = {p.name: p.stat().st_mtime_ns for p in d11.glob("*.parquet")}

    land(settings, "payments", D3, [payment("P3", "2026-09-12T09:00:00"),
                                    payment("P0", "2026-09-10T23:59:00")])  # llega 2 días tarde
    result = process(spark, settings, "payments", D3)
    assert set(result.partitions) == {"dt=2026-09-10", "dt=2026-09-12"}
    assert result.details["late_rows"] == 1
    by_dt = {r["dt"].isoformat(): r["count"] for r in silver_df(spark, settings, "payments").groupBy("dt").count().collect()}
    assert by_dt["2026-09-10"] == 2 and by_dt["2026-09-11"] == 1 and by_dt["2026-09-12"] == 1
    assert {p.name: p.stat().st_mtime_ns for p in d11.glob("*.parquet")} == before


def test_too_late_data_goes_to_quarantine(spark, settings):
    setup_merchants(spark, settings)
    land(settings, "payments", D3, [payment("P1", "2026-08-01T10:00:00")])
    process(spark, settings, "payments", D3)
    assert "late:out_of_window" in quarantine_reasons(spark, settings, "payments")


def test_reprocessing_a_day_is_idempotent(spark, settings):
    setup_merchants(spark, settings)
    land(settings, "payments", D1, [payment(f"P{i}", "2026-09-10T10:00:00") for i in range(20)])
    process(spark, settings, "payments", D1)
    land(settings, "payments", D2, [payment(f"P{i}", "2026-09-11T10:00:00") for i in range(20, 30)]
         + [payment("P1", "2026-09-10T10:00:00", status="declined", updated_at="2026-09-11T01:00:00")])
    process(spark, settings, "payments", D2)

    def snapshot():
        df = silver_df(spark, settings, "payments")
        return sorted((r["payment_id"], r["status"], str(r["dt"])) for r in df.collect())

    first = snapshot()
    process(spark, settings, "payments", D2)
    process(spark, settings, "payments", D2)
    assert snapshot() == first
    assert len(first) == 30


def test_schema_evolution_v1_and_v2_coexist_and_unknown_columns_are_dropped(spark, settings):
    setup_merchants(spark, settings)
    meta = Metadata(settings.meta_db)
    land(settings, "payments", D1, [payment("P1", "2026-09-10T10:00:00")])  # v1
    process(spark, settings, "payments", D1, meta)
    land(settings, "payments", D2, [payment("P2", "2026-09-11T10:00:00", currency="USD", channel="web",
                                            promo_code="X1")])  # v2 + columna no acordada
    process(spark, settings, "payments", D2, meta)
    rows = {r["payment_id"]: r for r in silver_df(spark, settings, "payments").collect()}
    assert rows["P1"]["currency"] == "COP" and rows["P1"]["channel"] is None  # default para v1
    assert rows["P2"]["currency"] == "USD" and rows["P2"]["channel"] == "web"
    assert "promo_code" not in silver_df(spark, settings, "payments").columns
    events = meta.query("SELECT kind, columns FROM schema_events")
    assert {"kind": "unknown_columns", "columns": '["promo_code"]'} in events


def test_refund_checks_against_original_payment(spark, settings):
    setup_merchants(spark, settings)
    land(settings, "payments", D1, [payment("P1", "2026-09-10T10:00:00", amount=1000)])
    process(spark, settings, "payments", D1)
    ts = "2026-09-12T10:00:00"
    land(settings, "refunds", D3, [
        {"refund_id": "R1", "payment_id": "P1", "amount": 400, "refund_date": ts, "updated_at": ts},
        {"refund_id": "R2", "payment_id": "P1", "amount": 5000, "refund_date": ts, "updated_at": ts},
        {"refund_id": "R3", "payment_id": "P404", "amount": 10, "refund_date": ts, "updated_at": ts},
    ])
    process(spark, settings, "refunds", D3)
    rows = {r["refund_id"]: r for r in silver_df(spark, settings, "refunds").collect()}
    assert set(rows) == {"R1", "R3"}
    assert rows["R1"]["payment_found"] is True
    assert rows["R3"]["payment_found"] is False  # padre que aún no llega: se conserva marcado
    assert quarantine_reasons(spark, settings, "refunds") == ["amount:exceeds_payment"]


def test_dimension_keeps_current_state(spark, settings):
    c = {"customer_id": "C1", "document_type": "CC", "document_number": "123", "city": "Cali",
         "created_at": "2026-09-01T00:00:00", "risk_segment": "low", "updated_at": "2026-09-01T00:00:00"}
    land(settings, "customers", D1, [c])
    process(spark, settings, "customers", D1)
    land(settings, "customers", D2, [c | {"risk_segment": "high", "updated_at": "2026-09-11T00:00:00"}])
    process(spark, settings, "customers", D2)
    rows = silver_df(spark, settings, "customers").collect()
    assert len(rows) == 1 and rows[0]["risk_segment"] == "high"


def test_entity_without_data_still_has_a_readable_empty_table(spark, settings):
    silver.ensure_table(spark, settings, "chargebacks")
    df = silver_df(spark, settings, "chargebacks")
    assert df.count() == 0 and "chargeback_id" in df.columns


def test_partition_pruning_reads_only_affected_partitions(spark, settings):
    setup_merchants(spark, settings)
    for day in (D1, D2, D3):
        land(settings, "payments", day, [payment(f"P{day.day}", f"{day}T10:00:00")])
        process(spark, settings, "payments", day)
    df = silver_df(spark, settings, "payments").where(F.col("dt") == F.lit(D2))
    plan = explain_string(df)
    pruning = next(line for line in plan.splitlines() if "PartitionFilters" in line)
    assert "2026-09-11" in pruning  # el filtro se aplica al listar archivos, no después de leerlos
    assert df.count() == 1
