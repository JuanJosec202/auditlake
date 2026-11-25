from auditlake.classify import BatchCounts, Outcome, classify, count_outcomes, verified_ids


def test_new_when_event_not_in_bronze():
    assert classify("h1", None) is Outcome.NEW


def test_duplicate_when_same_hash():
    assert classify("h1", "h1") is Outcome.DUPLICATE


def test_conflict_when_hash_differs():
    assert classify("h1", "h2") is Outcome.CONFLICT


def test_count_outcomes_mixed_batch():
    batch = [("e1", "h1"), ("e2", "h2"), ("e3", "h3")]
    existing = {"e2": "h2", "e3": "otro"}
    assert count_outcomes(batch, existing) == BatchCounts(inserted=1, duplicates=1, conflicts=1)


def test_only_matching_hashes_are_verified_for_delete():
    batch = [(101, "e1", "h1"), (102, "e2", "h2"), (103, "e3", "h3")]
    archived = {"e1": "h1", "e2": "otro"}  # e2 en conflicto, e3 no archivado
    assert verified_ids(batch, archived) == [101]
