from collections import Counter
from datetime import date, timedelta

from conftest import make_settings

from finflow.generator import HOT_MERCHANT, Bank, landed_dates, write_landing

DAY = date(2026, 9, 10)


def bank(tmp_path, **kw):
    return Bank(make_settings(tmp_path, **({"scale": 0.3} | kw)))


def test_generation_is_deterministic(tmp_path):
    assert bank(tmp_path).day(DAY) == bank(tmp_path).day(DAY)


def test_sources_contain_the_problems_the_pipeline_must_solve(tmp_path):
    payments = bank(tmp_path).payments(DAY)
    ids = Counter(p["payment_id"] for p in payments)
    assert any(n > 1 for n in ids.values()), "debe haber reenvíos / actualizaciones"
    assert any(p["transaction_date"][:10] < DAY.isoformat() for p in payments), "debe haber datos tardíos"
    statuses = {p["payment_id"]: [] for p in payments}
    for p in payments:
        statuses[p["payment_id"]].append(p["status"])
    assert any(len(set(v)) > 1 for v in statuses.values()) or any(p["status"] != "pending" for p in payments)
    share_hot = sum(p["merchant_id"] == HOT_MERCHANT for p in payments) / len(payments)
    assert 0.18 < share_hot < 0.32, "el marketplace concentra ~25% (skew)"


def test_invalid_records_are_injected(tmp_path):
    payments = [p for d in range(5) for p in bank(tmp_path, scale=1).payments(DAY + timedelta(days=d))]
    assert any(p["amount"] < 0 for p in payments)
    assert any(p["customer_id"] is None for p in payments)
    assert any(p["payment_method"] == "crypto" for p in payments)


def test_schema_v2_starts_on_configured_date(tmp_path):
    b = bank(tmp_path, schema_v2_from=DAY)
    assert "currency" not in b.payments(DAY - timedelta(days=1))[0]
    assert {"currency", "channel"} <= set(b.payments(DAY)[0])


def test_refunds_and_chargebacks_reference_earlier_payments(tmp_path):
    b = bank(tmp_path, scale=1)
    for row in b.refunds(DAY) + b.chargebacks(DAY + timedelta(days=20)):
        assert row["payment_id"][1:9] < row.get("refund_date", row.get("chargeback_date"))[:10].replace("-", "")


def test_write_landing_is_idempotent(tmp_path):
    s = make_settings(tmp_path)
    write_landing(DAY, s)
    first = (tmp_path / "landing" / "payments" / f"ingest_date={DAY}" / "part-00000.jsonl").read_bytes()
    write_landing(DAY, s)
    assert first == (tmp_path / "landing" / "payments" / f"ingest_date={DAY}" / "part-00000.jsonl").read_bytes()
    assert landed_dates(s) == [DAY]
