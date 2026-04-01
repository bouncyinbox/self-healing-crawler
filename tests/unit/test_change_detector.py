"""
Unit tests for change_detector.py — verifying the Jaccard-based structural
similarity algorithm and drift detection logic.

Key invariants:
- Identical HTML → similarity = 1.0, no drift
- Completely different HTML → similarity near 0, drift detected
- Partially changed HTML → similarity between thresholds
- No baseline → no drift by definition (first visit)
"""

import pytest
from crawler.change_detector import (
    ChangeDetector,
    _dom_skeleton_text,
    _dom_shingles,
    _structural_similarity,
)


SIMPLE_HTML = """
<html><body>
  <div id="product" class="product-detail">
    <h1 id="product-title">Widget Pro</h1>
    <span id="product-price">$49.99</span>
    <span id="stock-status">In Stock</span>
  </div>
</body></html>
"""

IDENTICAL_HTML = SIMPLE_HTML  # Same string

MINOR_CHANGE_HTML = """
<html><body>
  <div id="product" class="product-detail">
    <h1 id="product-title">Widget Pro — updated text</h1>
    <span id="product-price">$54.99</span>
    <span id="stock-status">In Stock</span>
  </div>
</body></html>
"""

COMPLETELY_DIFFERENT_HTML = """
<html><body>
  <nav class="top-nav"><a href="/">Home</a><a href="/shop">Shop</a></nav>
  <footer class="site-footer"><p>Copyright 2024</p></footer>
</body></html>
"""

MAJOR_REDESIGN_HTML = """
<html><body>
  <section class="pdp-wrapper">
    <div class="manufacturer-badge">Widget Co</div>
    <h2 class="item-name">Widget Pro</h2>
    <div class="pricing-block"><strong class="price-tag">$49.99</strong></div>
    <div class="stock-chip available">Available</div>
  </section>
</body></html>
"""


class TestDomSkeletonText:
    def test_strips_script_content(self):
        html = "<html><body><script>alert(1)</script><h1>Title</h1></body></html>"
        skeleton = _dom_skeleton_text(html)
        assert "alert" not in skeleton

    def test_strips_style_content(self):
        html = "<html><head><style>.foo{color:red}</style></head><body><p>text</p></body></html>"
        skeleton = _dom_skeleton_text(html)
        assert "color" not in skeleton

    def test_includes_ids_and_classes(self):
        skeleton = _dom_skeleton_text(SIMPLE_HTML)
        assert "product-title" in skeleton or "product" in skeleton

    def test_stable_to_text_changes(self):
        """Changing only text content must not change the skeleton."""
        skeleton1 = _dom_skeleton_text(SIMPLE_HTML)
        skeleton2 = _dom_skeleton_text(MINOR_CHANGE_HTML)
        # They differ only in text content — skeletons should be identical
        # (same tags, same IDs, same classes)
        assert skeleton1 == skeleton2


class TestDomShingles:
    def test_returns_frozenset(self):
        skeleton = _dom_skeleton_text(SIMPLE_HTML)
        shingles = _dom_shingles(skeleton, n=3)
        assert isinstance(shingles, frozenset)

    def test_shingles_non_empty(self):
        skeleton = _dom_skeleton_text(SIMPLE_HTML)
        shingles = _dom_shingles(skeleton, n=3)
        assert len(shingles) > 0

    def test_single_line_returns_itself(self):
        shingles = _dom_shingles("html:{}", n=3)
        assert "html:{}" in shingles

    def test_deterministic(self):
        skeleton = _dom_skeleton_text(SIMPLE_HTML)
        assert _dom_shingles(skeleton) == _dom_shingles(skeleton)


class TestStructuralSimilarity:
    def test_identical_shingles_score_one(self):
        skeleton = _dom_skeleton_text(SIMPLE_HTML)
        shingles = _dom_shingles(skeleton)
        assert _structural_similarity(shingles, shingles) == 1.0

    def test_empty_shingles_score_one(self):
        assert _structural_similarity(frozenset(), frozenset()) == 1.0

    def test_disjoint_shingles_score_zero(self):
        a = frozenset(["a\nb\nc", "b\nc\nd"])
        b = frozenset(["x\ny\nz", "y\nz\nw"])
        assert _structural_similarity(a, b) == 0.0

    def test_partial_overlap_between_zero_and_one(self):
        a = frozenset(["a", "b", "c", "d"])
        b = frozenset(["c", "d", "e", "f"])
        score = _structural_similarity(a, b)
        assert 0.0 < score < 1.0

    def test_text_only_change_gives_max_similarity(self):
        """Changing text should not affect structural similarity at all."""
        s1 = frozenset(_dom_shingles(_dom_skeleton_text(SIMPLE_HTML)))
        s2 = frozenset(_dom_shingles(_dom_skeleton_text(MINOR_CHANGE_HTML)))
        assert _structural_similarity(s1, s2) == 1.0

    def test_major_redesign_gives_low_similarity(self):
        s1 = _dom_shingles(_dom_skeleton_text(SIMPLE_HTML))
        s2 = _dom_shingles(_dom_skeleton_text(MAJOR_REDESIGN_HTML))
        score = _structural_similarity(s1, s2)
        assert score < 0.5


class TestChangeDetector:
    @pytest.fixture
    def detector(self):
        return ChangeDetector(structural_threshold=0.85, visual_threshold=0.80)

    def test_no_baseline_returns_no_drift(self, detector, html_v1):
        report = detector.detect(html_v1, None, baseline=None)
        assert report.has_drift is False
        assert report.structural_similarity == 1.0

    def test_identical_page_no_drift(self, detector, html_v1):
        _, shingles, visual = detector.compute_fingerprint(html_v1)
        sha256, _, _ = detector.compute_fingerprint(html_v1)
        baseline = {
            "structural_hash": sha256,
            "structural_shingles": list(shingles),
            "visual_hash": visual,
        }
        report = detector.detect(html_v1, None, baseline)
        assert report.has_drift is False
        assert report.structural_similarity == 1.0

    def test_text_only_change_no_structural_drift(self, detector, html_v1, html_v2):
        """v1 vs minor-text-change — structural similarity near 1.0 is tested
        through the minor-change fixture in conftest (same IDs, different text)."""
        # Use MINOR_CHANGE_HTML (imported locally for this test)
        minor = MINOR_CHANGE_HTML
        sha256_v1, shingles_v1, _ = detector.compute_fingerprint(SIMPLE_HTML)
        baseline = {
            "structural_hash": sha256_v1,
            "structural_shingles": list(shingles_v1),
            "visual_hash": None,
        }
        report = detector.detect(minor, None, baseline)
        assert report.structural_similarity == 1.0
        assert report.structural_drift is False

    def test_major_redesign_triggers_drift(self, detector):
        sha256, shingles, _ = detector.compute_fingerprint(SIMPLE_HTML)
        baseline = {
            "structural_hash": sha256,
            "structural_shingles": list(shingles),
            "visual_hash": None,
        }
        report = detector.detect(MAJOR_REDESIGN_HTML, None, baseline)
        assert report.structural_drift is True
        assert report.structural_similarity < 0.85

    def test_drift_severity_major(self, detector):
        sha256, shingles, _ = detector.compute_fingerprint(SIMPLE_HTML)
        baseline = {
            "structural_hash": sha256,
            "structural_shingles": list(shingles),
            "visual_hash": None,
        }
        report = detector.detect(COMPLETELY_DIFFERENT_HTML, None, baseline)
        assert report.drift_severity in ("major", "moderate")

    def test_drift_severity_none(self, detector):
        report = detector.detect(SIMPLE_HTML, None, baseline=None)
        assert report.drift_severity == "none"

    def test_current_shingles_populated(self, detector, html_v1):
        report = detector.detect(html_v1, None, baseline=None)
        assert len(report._current_shingles) > 0

    def test_old_cache_without_shingles_falls_back(self, detector, html_v1):
        """Cache entries written before shingle support had no shingles field.
        They should fall back to hash equality comparison without crashing."""
        sha256, _, _ = detector.compute_fingerprint(html_v1)
        baseline = {"structural_hash": sha256, "visual_hash": None}  # no shingles key
        report = detector.detect(html_v1, None, baseline)
        assert report.structural_similarity == 1.0
        assert report.structural_drift is False
