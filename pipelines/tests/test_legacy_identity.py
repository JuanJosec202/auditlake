import uuid
from datetime import datetime, timedelta, timezone

from auditlake.classify import legacy_event_id

T = datetime(2026, 5, 1, 0, 0, 7, tzinfo=timezone.utc)


def test_same_row_gives_same_event_id():
    assert legacy_event_id(42, T) == legacy_event_id(42, T)
    assert legacy_event_id(42, T) == legacy_event_id(42, T.replace(tzinfo=None))  # sin zona = UTC


def test_version_variant_and_timestamp_bits():
    u = uuid.UUID(legacy_event_id(42, T))
    assert u.version == 7 and u.variant == uuid.RFC_4122
    assert u.int >> 80 == int(T.timestamp() * 1000)


def test_different_pk_or_table_changes_only_the_hash_bits():
    a, b = uuid.UUID(legacy_event_id(42, T)), uuid.UUID(legacy_event_id(43, T))
    c = uuid.UUID(legacy_event_id(42, T, table="legacy.OtherLog"))
    assert a != b and a != c
    assert a.int >> 80 == b.int >> 80 == c.int >> 80


def test_lexicographic_order_follows_occurred_at():
    ids = [legacy_event_id(1000 - i, T + timedelta(minutes=i)) for i in range(50)]  # PK al revés del tiempo
    assert ids == sorted(ids)
