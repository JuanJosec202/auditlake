from datetime import datetime, timezone

from auditlake.log_ingest import sealable

NOW = datetime(2026, 11, 26, 0, 40, 30, tzinfo=timezone.utc)


def test_closed_files_with_margin_are_sealed_but_not_the_active_one():
    names = ["api-202611260037.clef", "api-202611260038.clef", "api-202611260039.clef", "api-202611260040.clef"]
    # 00:37 cerró 00:38 (+1 min margen = 00:39 <= 00:40:30): sí. 00:38 cerró 00:39 (+1 = 00:40): sí.
    # 00:39 cerró 00:40 (+1 = 00:41 > ahora): no. 00:40 es el activo: nunca.
    assert sealable(names, NOW) == ["api-202611260037.clef", "api-202611260038.clef"]


def test_active_file_is_never_sealed_even_if_old():
    assert sealable(["api-202611251200.clef"], NOW) == []


def test_size_rolled_files_and_foreign_names():
    names = ["api-202611260030.clef", "api-202611260030_001.clef", "notes.txt", "api-202611260040.clef"]
    assert sealable(names, NOW) == ["api-202611260030.clef", "api-202611260030_001.clef"]
