import sys

sys.path.insert(0, "/app/benchmarks")
from bench import percentile, summarize  # noqa: E402


def test_percentile_interpolates():
    xs = list(range(1, 101))  # 1..100
    assert percentile(xs, 0.5) == 50.5
    assert round(percentile(xs, 0.95), 2) == 95.05


def test_summarize_counts_errors_apart():
    s = summarize([10.0, 20.0, 30.0], errors=2)
    assert (s["requests"], s["errors"], s["p50_ms"]) == (5, 2, 20.0)
