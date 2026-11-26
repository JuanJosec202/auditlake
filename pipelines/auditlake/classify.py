"""Clasificación pura de eventos del lote contra lo que ya existe en bronze."""

from __future__ import annotations

from dataclasses import dataclass
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
