"""Generador sintético de las fuentes de una fintech colombiana.

Simula lo que los sistemas fuente dejan cada día en la zona de *landing* (JSON lines,
una carpeta por fecha de ingesta). Es determinístico: cada registro sale de una semilla
derivada de (fecha, índice), así que regenerar un día produce exactamente los mismos
archivos sin guardar estado.

Problemas reales que inyecta a propósito (el pipeline debe resolverlos):
* duplicados exactos dentro del archivo y reenvíos de un mismo pago;
* actualizaciones de estado (pending -> approved/declined) que llegan al día siguiente;
* datos tardíos: pagos cuya fecha de transacción es de días anteriores;
* registros inválidos (montos negativos, llaves vacías, valores fuera de catálogo);
* líneas JSON corruptas;
* cambio de esquema compatible (v2: currency, channel) desde una fecha configurable;
* sesgo (skew): un marketplace concentra ~25% de los pagos.
"""

from __future__ import annotations

import json
import math
import random
from collections.abc import Iterator
from datetime import date, datetime, time, timedelta
from pathlib import Path
from typing import Any

from .config import Settings, get_settings

CITIES = ("Bogotá", "Medellín", "Cali", "Barranquilla", "Cartagena", "Bucaramanga", "Pereira", "Manizales")
CATEGORIES = ("retail", "food", "transport", "marketplace", "services", "travel", "education", "health")
USD_COP = 4000  # debe coincidir con vars.usd_cop en dbt/dbt_project.yml
TICKET = {"retail": 120_000, "food": 45_000, "transport": 25_000, "marketplace": 180_000, "services": 150_000,
          "travel": 650_000, "education": 900_000, "health": 220_000}
N_MERCHANTS = 300
HOT_MERCHANT = "M0001"  # marketplace gigante: genera skew en joins y agregaciones
WEEKDAY = (1.0, 0.95, 0.97, 1.0, 1.15, 1.25, 0.8)  # lun..dom
REFUND_WINDOW = 20
CHARGEBACK_WINDOW = (5, 45)


class Bank:
    """Todas las funciones son puras respecto a (seed, fecha, índice)."""

    def __init__(self, settings: Settings | None = None) -> None:
        self.s = settings or get_settings()

    # ------------------------------------------------------------------ helpers
    def rng(self, *key: Any) -> random.Random:
        return random.Random(f"{self.s.seed}:" + ":".join(map(str, key)))

    @staticmethod
    def ts(day: date, rng: random.Random) -> str:
        return (datetime.combine(day, time()) + timedelta(seconds=rng.randint(0, 86_399))).isoformat()

    def days(self, until: date) -> Iterator[date]:
        d = self.s.epoch
        while d <= until:
            yield d
            d += timedelta(days=1)

    # ---------------------------------------------------------------- volumes
    def n_customers(self, day: date) -> int:
        base = 800 if day == self.s.epoch else 35
        return max(5, int(base * self.s.scale * self.rng("nc", day).uniform(0.8, 1.2)))

    def n_payments(self, day: date) -> int:
        noise = self.rng("np", day).gauss(1, 0.05)
        return max(20, int(4000 * self.s.scale * WEEKDAY[day.weekday()] * noise))

    # --------------------------------------------------------------- entities
    def customer_id(self, day: date, i: int) -> str:
        return f"C{day:%Y%m%d}{i:05d}"

    def random_customer(self, until: date, rng: random.Random) -> str:
        # La mitad de los pagos los hacen clientes "fundacionales" (epoch), el resto clientes recientes.
        if rng.random() < 0.5 or until == self.s.epoch:
            return self.customer_id(self.s.epoch, rng.randrange(self.n_customers(self.s.epoch)))
        offset = rng.randint(1, (until - self.s.epoch).days)
        day = self.s.epoch + timedelta(days=offset)
        return self.customer_id(day, rng.randrange(self.n_customers(day)))

    def customer(self, cid: str) -> dict:
        day = datetime.strptime(cid[1:9], "%Y%m%d").date()
        r = self.rng("c", day, int(cid[9:]))
        created = self.ts(day, r)
        return {"customer_id": cid, "document_type": r.choices(("CC", "CE", "NIT", "PP"), (88, 4, 6, 2))[0],
                "document_number": str(r.randint(10_000_000, 1_199_999_999)), "city": r.choice(CITIES),
                "created_at": created, "risk_segment": r.choices(("low", "medium", "high"), (70, 24, 6))[0],
                "updated_at": created}

    def customers(self, day: date) -> list[dict]:
        rows = [self.customer(self.customer_id(day, i)) for i in range(self.n_customers(day))]
        # Cambios de segmento de riesgo de clientes existentes (la dimensión debe quedarse con el último).
        if day > self.s.epoch:
            r = self.rng("cu", day)
            for _ in range(max(1, int(10 * self.s.scale))):
                cid = self.random_customer(day - timedelta(days=1), r)
                rows.append({**self.customer(cid), "risk_segment": r.choice(("low", "medium", "high")),
                             "updated_at": self.ts(day, r)})
        return rows

    def merchant(self, n: int) -> dict:
        r = self.rng("m", n)
        category = "marketplace" if n == 1 else r.choice(CATEGORIES)
        return {"merchant_id": f"M{n:04d}", "name": f"Comercio {n:04d}", "category": category,
                "city": r.choice(CITIES), "status": "active"}

    def merchants(self, day: date) -> list[dict]:
        if day == self.s.epoch:
            return [{**self.merchant(n), "updated_at": f"{day}T00:00:00"} for n in range(1, N_MERCHANTS + 1)]
        r = self.rng("mu", day)
        rows = []
        for _ in range(r.randint(0, 3)):  # suspensiones / reactivaciones ocasionales
            n = r.randint(2, N_MERCHANTS)
            rows.append({**self.merchant(n), "status": r.choice(("active", "suspended")),
                         "updated_at": self.ts(day, r)})
        return rows

    def merchant_for(self, rng: random.Random) -> str:
        return HOT_MERCHANT if rng.random() < 0.25 else f"M{rng.randint(2, N_MERCHANTS):04d}"

    def payment(self, day: date, i: int) -> dict:
        """El pago i que la fuente emite el día `day` (su fecha de transacción puede ser anterior)."""
        r = self.rng("p", day, i)
        merchant = self.merchant_for(r)
        category = self.merchant(int(merchant[1:]))["category"]
        late = r.random() < 0.03 and day > self.s.epoch
        tx_day = day - timedelta(days=r.randint(1, min(3, (day - self.s.epoch).days))) if late else day
        status = r.choices(("approved", "declined", "pending"), (88, 7, 5))[0]
        tx = self.ts(tx_day, r)
        row = {
            "payment_id": f"P{day:%Y%m%d}{i:06d}", "customer_id": self.random_customer(tx_day, r),
            "merchant_id": merchant,
            "amount": round(r.lognormvariate(math.log(TICKET[category]), 0.6), -2),
            "payment_method": r.choices(("card", "pse", "wallet", "transfer"), (50, 25, 15, 10))[0],
            "transaction_date": tx, "status": status, "updated_at": tx,
        }
        if day >= self.s.schema_v2_from:
            row["currency"] = "USD" if r.random() < 0.02 else "COP"
            if row["currency"] == "USD":  # mismo ticket, expresado en dólares (tasa = var usd_cop de dbt)
                row["amount"] = max(1.0, round(row["amount"] / USD_COP, 2))
            row["channel"] = r.choices(("app", "web", "pos"), (60, 25, 15))[0]
        return row

    def final_status(self, day: date, i: int) -> str:
        return self.rng("ps", day, i).choices(("approved", "declined"), (85, 15))[0]

    def payments(self, day: date) -> list[dict]:
        r = self.rng("pd", day)
        rows = []
        for i in range(self.n_payments(day)):
            row = self.payment(day, i)
            k = r.random()
            if k < 0.001:
                row["amount"] = -abs(row["amount"])
            elif k < 0.002:
                row["customer_id"] = None
            elif k < 0.003:
                row["payment_method"] = "crypto"
            rows.append(row)
            if r.random() < 0.01:  # reenvío exacto del mismo registro
                rows.append(dict(row))
        # Pagos pendientes de ayer: hoy llega su estado final (mismo payment_id, updated_at más reciente).
        if day > self.s.epoch:
            yesterday = day - timedelta(days=1)
            for i in range(self.n_payments(yesterday)):
                prev = self.payment(yesterday, i)
                if prev["status"] == "pending":
                    upd = self.ts(day, self.rng("pu", day, i))
                    rows.append({**prev, "status": self.final_status(yesterday, i), "updated_at": upd})
        return rows

    def _approved_amount(self, day: date, i: int) -> tuple[dict, float] | None:
        p = self.payment(day, i)
        final = p["status"] if p["status"] != "pending" else self.final_status(day, i)
        return (p, p["amount"]) if final == "approved" and p["amount"] > 0 else None

    def refunds(self, day: date) -> list[dict]:
        rows = []
        for back in range(1, REFUND_WINDOW + 1):
            pday = day - timedelta(days=back)
            if pday < self.s.epoch:
                break
            for i in range(self.n_payments(pday)):
                r = self.rng("rf", pday, i)
                if r.random() >= 0.015 or r.randint(1, REFUND_WINDOW) != back:
                    continue
                hit = self._approved_amount(pday, i)
                if not hit:
                    continue
                p, amount = hit
                value = amount if r.random() < 0.6 else round(amount * r.uniform(0.1, 0.9), -2)
                if r.random() < 0.01:  # error de origen: devuelve más de lo pagado
                    value = round(amount * 1.5, -2)
                ts = self.ts(day, r)
                rows.append({"refund_id": f"R{p['payment_id'][1:]}", "payment_id": p["payment_id"],
                             "amount": value, "refund_date": ts, "updated_at": ts})
        return rows

    def chargebacks(self, day: date) -> list[dict]:
        rows = []
        lo, hi = CHARGEBACK_WINDOW
        for back in range(lo, hi + 1):
            pday = day - timedelta(days=back)
            if pday < self.s.epoch:
                break
            for i in range(self.n_payments(pday)):
                r = self.rng("cb", pday, i)
                if r.random() >= 0.003 or r.randint(lo, hi) != back:
                    continue
                hit = self._approved_amount(pday, i)
                if not hit:
                    continue
                p, amount = hit
                ts = self.ts(day, r)
                reason = r.choices(("fraud", "not_received", "duplicate", "not_as_described", "other"),
                                   (45, 20, 10, 15, 10))[0]
                rows.append({"chargeback_id": f"K{p['payment_id'][1:]}", "payment_id": p["payment_id"],
                             "reason": reason, "amount": amount, "chargeback_date": ts, "updated_at": ts})
        return rows

    def day(self, day: date) -> dict[str, list[dict]]:
        return {"customers": self.customers(day), "merchants": self.merchants(day), "payments": self.payments(day),
                "refunds": self.refunds(day), "chargebacks": self.chargebacks(day)}


def write_landing(day: date, settings: Settings | None = None, anomalies: set[str] | None = None) -> dict[str, int]:
    """Escribe los archivos del día en landing. Re-ejecutarlo reemplaza el archivo (idempotente).

    `anomalies` permite simular problemas puntuales en demos y pruebas:
    * "unknown_column": la fuente agrega una columna no acordada (promo_code) a pagos.
    * "corrupt_lines": algunas líneas JSON llegan truncadas.
    """
    s = settings or get_settings()
    anomalies = anomalies or set()
    counts = {}
    for entity, rows in Bank(s).day(day).items():
        folder = Path(s.landing) / entity / f"ingest_date={day.isoformat()}"
        folder.mkdir(parents=True, exist_ok=True)
        lines = []
        for n, row in enumerate(rows):
            if entity == "payments" and "unknown_column" in anomalies:
                row = {**row, "promo_code": f"PROMO{n % 7}"}
            line = json.dumps(row, ensure_ascii=False)
            if entity == "payments" and "corrupt_lines" in anomalies and n % 500 == 7:
                line = line[: len(line) // 2]
            lines.append(line)
        (folder / "part-00000.jsonl").write_text("\n".join(lines) + "\n", encoding="utf-8")
        counts[entity] = len(rows)
    return counts


def landed_dates(settings: Settings | None = None) -> list[date]:
    s = settings or get_settings()
    root = Path(s.landing) / "payments"
    if not root.exists():
        return []
    return sorted(date.fromisoformat(p.name.split("=", 1)[1]) for p in root.glob("ingest_date=*"))
