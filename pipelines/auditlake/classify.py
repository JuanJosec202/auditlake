"""Clasificación pura de eventos del lote contra lo que ya existe en bronze."""

from __future__ import annotations

import hashlib
import uuid
from dataclasses import dataclass
from datetime import datetime, timezone
from enum import Enum
from typing import Iterable, List, Mapping, Optional, Tuple


class Outcome(str, Enum):
    NEW = "new"
    DUPLICATE = "duplicate"  # mismo event_id y mismo hash: ya archivado (p. ej. reintento tras caída)
    CONFLICT = "conflict"  # mismo event_id, distinto hash: bronze no se toca; la fila entrante va a cuarentena


def classify(incoming_hash: str, existing_hash: Optional[str]) -> Outcome:
    if existing_hash is None:
        return Outcome.NEW
    return Outcome.DUPLICATE if existing_hash == incoming_hash else Outcome.CONFLICT


@dataclass(frozen=True)
class BatchCounts:
    inserted: int
    duplicates: int
    conflicts: int


def count_outcomes(
    batch: Iterable[Tuple[str, str]], existing: Mapping[str, str]
) -> BatchCounts:
    """batch: pares (event_id, payload_hash); existing: event_id -> payload_hash en bronze."""
    counts = {o: 0 for o in Outcome}
    for event_id, payload_hash in batch:
        counts[classify(payload_hash, existing.get(event_id))] += 1
    return BatchCounts(counts[Outcome.NEW], counts[Outcome.DUPLICATE], counts[Outcome.CONFLICT])


def verified_ids(
    batch: Iterable[Tuple[int, str, str]], archived: Mapping[str, str]
) -> list:
    """IDs de outbox cuyo event_id está en bronze con el mismo hash. Solo esos se pueden borrar."""
    return [
        outbox_id
        for outbox_id, event_id, payload_hash in batch
        if archived.get(event_id) == payload_hash
    ]


def conflict_rows(
    batch: Iterable[Tuple[int, str, str]], archived: Mapping[str, str]
) -> List[Tuple[int, str, str]]:
    """(outbox_id, hash entrante, hash archivado) de las filas en conflicto: van a cuarentena."""
    return [
        (outbox_id, payload_hash, archived[event_id])
        for outbox_id, event_id, payload_hash in batch
        if event_id in archived and archived[event_id] != payload_hash
    ]


# --- Identidad reproducible del backfill -------------------------------------------------------------

LEGACY_SOURCE = "sqlserver:AuditLake"
LEGACY_TABLE = "legacy.AuditLog"


def legacy_event_id(pk: int, occurred_at: datetime, source: str = LEGACY_SOURCE, table: str = LEGACY_TABLE) -> str:
    """UUIDv7 determinista para una fila histórica: 48 bits con los ms de occurred_at (orden temporal, así la
    búsqueda por rango de event_id sigue podando archivos) y los 74 bits restantes de SHA-256 de la identidad
    estable de origen (sistema/tabla/PK). Reejecutar el backfill produce el mismo event_id. occurred_at sin
    zona horaria se interpreta como UTC."""
    if occurred_at.tzinfo is None:
        occurred_at = occurred_at.replace(tzinfo=timezone.utc)
    ms = int(occurred_at.timestamp() * 1000)
    digest = int.from_bytes(hashlib.sha256(f"{source}/{table}/{int(pk)}".encode()).digest()[:10], "big")
    rand_a = (digest >> 68) & 0xFFF  # 12 bits
    rand_b = digest & ((1 << 62) - 1)  # 62 bits
    value = (ms << 80) | (0x7 << 76) | (rand_a << 64) | (0b10 << 62) | rand_b
    return str(uuid.UUID(int=value))
