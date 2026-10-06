"""Contratos de datos: lo que cada fuente se compromete a enviar.

Un contrato define columnas, tipos, reglas de validación, la llave de negocio (para
deduplicar) y la columna de fecha de evento (para particionar). Los cambios de esquema
compatibles (columnas nuevas opcionales) se modelan como versiones del contrato.
"""

from __future__ import annotations

from dataclasses import dataclass, field


@dataclass(frozen=True)
class Column:
    name: str
    type: str  # string | long | double | timestamp | date
    required: bool = False
    allowed: tuple[str, ...] | None = None
    min: float | None = None
    since: int = 1  # versión del contrato en la que aparece
    default: str | None = None  # valor para registros de versiones anteriores


@dataclass(frozen=True)
class Contract:
    entity: str
    version: int
    business_key: tuple[str, ...]
    order_by: str  # el registro más reciente según esta columna gana al deduplicar
    event_date: str | None  # columna que define la partición dt (None = dimensión, sin partición)
    columns: tuple[Column, ...] = field(default_factory=tuple)

    @property
    def names(self) -> list[str]:
        return [c.name for c in self.columns]

    def column(self, name: str) -> Column:
        return next(c for c in self.columns if c.name == name)


CUSTOMERS = Contract(
    "customers", 1, ("customer_id",), "updated_at", None, (
        Column("customer_id", "string", required=True),
        Column("document_type", "string", required=True, allowed=("CC", "CE", "NIT", "PP")),
        Column("document_number", "string", required=True),
        Column("city", "string", required=True),
        Column("created_at", "timestamp", required=True),
        Column("risk_segment", "string", required=True, allowed=("low", "medium", "high")),
        Column("updated_at", "timestamp", required=True),
    ),
)

MERCHANTS = Contract(
    "merchants", 1, ("merchant_id",), "updated_at", None, (
        Column("merchant_id", "string", required=True),
        Column("name", "string", required=True),
        Column("category", "string", required=True,
               allowed=("retail", "food", "transport", "marketplace", "services", "travel", "education", "health")),
        Column("city", "string", required=True),
        Column("status", "string", required=True, allowed=("active", "suspended", "closed")),
        Column("updated_at", "timestamp", required=True),
    ),
)

PAYMENTS = Contract(
    "payments", 2, ("payment_id",), "updated_at", "transaction_date", (
        Column("payment_id", "string", required=True),
        Column("customer_id", "string", required=True),
        Column("merchant_id", "string", required=True),
        Column("amount", "double", required=True, min=0.01),
        Column("payment_method", "string", required=True, allowed=("card", "pse", "wallet", "transfer")),
        Column("transaction_date", "timestamp", required=True),
        Column("status", "string", required=True, allowed=("pending", "approved", "declined")),
        Column("updated_at", "timestamp", required=True),
        # v2 (cambio compatible): la fuente empieza a enviar moneda y canal
        Column("currency", "string", allowed=("COP", "USD"), since=2, default="COP"),
        Column("channel", "string", allowed=("app", "web", "pos"), since=2),
    ),
)

REFUNDS = Contract(
    "refunds", 1, ("refund_id",), "updated_at", "refund_date", (
        Column("refund_id", "string", required=True),
        Column("payment_id", "string", required=True),
        Column("amount", "double", required=True, min=0.01),
        Column("refund_date", "timestamp", required=True),
        Column("updated_at", "timestamp", required=True),
    ),
)

CHARGEBACKS = Contract(
    "chargebacks", 1, ("chargeback_id",), "updated_at", "chargeback_date", (
        Column("chargeback_id", "string", required=True),
        Column("payment_id", "string", required=True),
        Column("reason", "string", required=True,
               allowed=("fraud", "not_received", "duplicate", "not_as_described", "other")),
        Column("amount", "double", required=True, min=0.01),
        Column("chargeback_date", "timestamp", required=True),
        Column("updated_at", "timestamp", required=True),
    ),
)

CONTRACTS: dict[str, Contract] = {c.entity: c for c in (CUSTOMERS, MERCHANTS, PAYMENTS, REFUNDS, CHARGEBACKS)}
# Orden de procesamiento: dimensiones antes que hechos (los hechos validan contra ellas).
ENTITIES = ("customers", "merchants", "payments", "refunds", "chargebacks")
