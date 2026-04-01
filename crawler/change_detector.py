"""
change_detector.py — Detects structural and visual drift in web pages.

Two signals:
1. Structural fingerprint: Jaccard similarity over DOM shingle sets.
   (Previously used XOR on SHA256 hashes, which is incorrect — SHA256 has no
   locality property. Even a 1-character DOM change flips ~50% of hash bits,
   making the similarity score meaningless.)
2. Visual fingerprint: Perceptual hash (pHash) of screenshot with Hamming distance.

Why shingles for structural comparison?
  A shingle is an n-gram of consecutive DOM tokens. Two pages that share 90%
  of their DOM structure will share ~90% of their 3-grams, giving a Jaccard
  score of ~0.82. A complete redesign shares ~0% → score near 0.
  The SHA256 of the skeleton is stored for audit purposes only, never for comparison.
"""

from __future__ import annotations


import hashlib
import json
import logging
from dataclasses import dataclass, field
from typing import Optional

from bs4 import BeautifulSoup

from crawler.config import settings

logger = logging.getLogger(__name__)

_KEEP_ATTRS = frozenset({"id", "class", "role", "data-testid", "aria-label", "type", "name"})
_REMOVE_TAGS = frozenset({"script", "style", "noscript", "svg", "img", "iframe"})


# ── DOM skeleton helpers ──────────────────────────────────────────────────────

def _dom_skeleton_text(html: str) -> str:
    """
    Extract a structural representation of the DOM: tag names + key semantic
    attributes only, stripped of text content.

    Stable to: text changes, attribute reordering.
    Sensitive to: tag addition/removal, ID/class renames, structural reordering.
    """
    soup = BeautifulSoup(html, "lxml")
    for tag in soup(_REMOVE_TAGS):
        tag.decompose()

    lines: list[str] = []
    for tag in soup.find_all(True):
        attrs = {k: v for k, v in tag.attrs.items() if k in _KEEP_ATTRS}
        if "class" in attrs and isinstance(attrs["class"], list):
            attrs["class"] = sorted(attrs["class"])
        lines.append(f"{tag.name}:{json.dumps(attrs, sort_keys=True)}")

    return "\n".join(lines)


def _dom_shingles(skeleton_text: str, n: int = 3) -> frozenset[str]:
    """
    Build a set of n-grams from consecutive lines of the DOM skeleton.
    Used for Jaccard similarity: similar skeletons share most n-grams.
    """
    lines = skeleton_text.splitlines()
    if len(lines) < n:
        return frozenset(lines)
    return frozenset(
        "\n".join(lines[i : i + n]) for i in range(len(lines) - n + 1)
    )


def _dom_sha256(skeleton_text: str) -> str:
    """SHA256 of the skeleton text — for storage/audit identity only."""
    return hashlib.sha256(skeleton_text.encode()).hexdigest()


def _structural_similarity(
    shingles_a: frozenset[str], shingles_b: frozenset[str]
) -> float:
    """
    Jaccard similarity between two shingle sets.
    Returns 1.0 for identical structures, 0.0 for completely different ones.
    """
    if not shingles_a and not shingles_b:
        return 1.0
    union = shingles_a | shingles_b
    if not union:
        return 1.0
    intersection = shingles_a & shingles_b
    return len(intersection) / len(union)


# ── Visual fingerprint helpers ────────────────────────────────────────────────

def _perceptual_hash(screenshot_bytes: Optional[bytes]) -> Optional[str]:
    """Compute pHash of screenshot. Returns None if screenshot unavailable."""
    if not screenshot_bytes:
        return None
    try:
        import imagehash
        from PIL import Image
        import io

        img = Image.open(io.BytesIO(screenshot_bytes))
        return str(imagehash.phash(img))
    except Exception as exc:
        logger.warning("Perceptual hash failed: %s", exc)
        return None


def _visual_similarity(hash1: Optional[str], hash2: Optional[str]) -> float:
    """Hamming distance between pHash strings (64-bit = max distance of 64)."""
    if not hash1 or not hash2:
        return 1.0  # Can't compare → assume no drift
    try:
        import imagehash

        h1 = imagehash.hex_to_hash(hash1)
        h2 = imagehash.hex_to_hash(hash2)
        return 1.0 - ((h1 - h2) / 64.0)
    except Exception:
        return 1.0


# ── Public API ────────────────────────────────────────────────────────────────

@dataclass
class DriftReport:
    """Result of comparing the current page against the cached baseline."""

    structural_hash: str           # SHA256 of current skeleton (for storage)
    visual_hash: Optional[str]     # pHash of current screenshot
    structural_drift: bool
    visual_drift: bool
    structural_similarity: float   # 1.0 = identical, 0.0 = completely different
    visual_similarity: float
    previous_structural_hash: Optional[str]
    previous_visual_hash: Optional[str]

    # Shingle sets are carried so the orchestrator can pass them to the cache
    # without recomputing them.
    _current_shingles: frozenset[str] = field(default_factory=frozenset, repr=False)

    @property
    def has_drift(self) -> bool:
        return self.structural_drift or self.visual_drift

    @property
    def drift_severity(self) -> str:
        if not self.has_drift:
            return "none"
        if self.structural_similarity < 0.5:
            return "major"
        if self.structural_similarity < 0.8:
            return "moderate"
        return "minor"


class ChangeDetector:
    """
    Detects page drift by comparing current page against a stored baseline.

    Thresholds are read from CrawlerSettings and can be overridden per-instance
    (useful for tests).
    """

    def __init__(
        self,
        structural_threshold: float | None = None,
        visual_threshold: float | None = None,
        shingle_size: int | None = None,
    ) -> None:
        self.structural_threshold = (
            structural_threshold
            if structural_threshold is not None
            else settings.structural_drift_threshold
        )
        self.visual_threshold = (
            visual_threshold
            if visual_threshold is not None
            else settings.visual_drift_threshold
        )
        self.shingle_size = (
            shingle_size
            if shingle_size is not None
            else settings.dom_shingle_size
        )

    def compute_fingerprint(
        self,
        html: str,
        screenshot: Optional[bytes] = None,
    ) -> tuple[str, frozenset[str], Optional[str]]:
        """
        Compute (sha256_hash, shingles, visual_hash) for a page.

        sha256_hash — used for storage/audit identity.
        shingles    — used for structural similarity comparison.
        visual_hash — perceptual hash of screenshot.
        """
        skeleton_text = _dom_skeleton_text(html)
        shingles = _dom_shingles(skeleton_text, n=self.shingle_size)
        sha256 = _dom_sha256(skeleton_text)
        visual = _perceptual_hash(screenshot)
        return sha256, shingles, visual

    def detect(
        self,
        html: str,
        screenshot: Optional[bytes],
        baseline: Optional[dict],
    ) -> DriftReport:
        """
        Compare current page against baseline fingerprint.

        baseline format (as stored in CacheEntry.to_baseline()):
          {
            "structural_hash": "<sha256>",
            "structural_shingles": ["shingle1", ...],   # optional, for similarity
            "visual_hash": "<phash>"
          }

        Returns a DriftReport with similarity scores and drift flags.
        """
        current_sha256, current_shingles, current_visual = self.compute_fingerprint(
            html, screenshot
        )

        if baseline is None:
            return DriftReport(
                structural_hash=current_sha256,
                visual_hash=current_visual,
                structural_drift=False,
                visual_drift=False,
                structural_similarity=1.0,
                visual_similarity=1.0,
                previous_structural_hash=None,
                previous_visual_hash=None,
                _current_shingles=current_shingles,
            )

        prev_sha256 = baseline.get("structural_hash")
        prev_shingles_raw = baseline.get("structural_shingles")
        prev_visual = baseline.get("visual_hash")

        if prev_sha256 == current_sha256:
            # Exact match — skip Jaccard computation for speed
            struct_sim = 1.0
        elif prev_shingles_raw:
            prev_shingles = frozenset(prev_shingles_raw)
            struct_sim = _structural_similarity(current_shingles, prev_shingles)
        else:
            # Old cache entry without shingles — fall back to hash equality
            struct_sim = 1.0 if prev_sha256 == current_sha256 else 0.0

        visual_sim = _visual_similarity(current_visual, prev_visual)

        struct_drift = struct_sim < self.structural_threshold
        visual_drift = visual_sim < self.visual_threshold

        if struct_drift:
            logger.warning(
                "Structural drift detected: similarity=%.2f (threshold=%.2f)",
                struct_sim,
                self.structural_threshold,
            )
        if visual_drift:
            logger.warning(
                "Visual drift detected: similarity=%.2f (threshold=%.2f)",
                visual_sim,
                self.visual_threshold,
            )

        return DriftReport(
            structural_hash=current_sha256,
            visual_hash=current_visual,
            structural_drift=struct_drift,
            visual_drift=visual_drift,
            structural_similarity=struct_sim,
            visual_similarity=visual_sim,
            previous_structural_hash=prev_sha256,
            previous_visual_hash=prev_visual,
            _current_shingles=current_shingles,
        )
