"""
change_detector.py — Detects structural and visual drift in web pages.

Two signals are used:
1. Structural fingerprint: Hash of the DOM skeleton (tags + attributes, no text content)
2. Visual fingerprint: Perceptual hash of a screenshot (resistant to minor pixel changes)

When either signal drifts beyond threshold → invalidate cache, trigger LLM re-extraction.
"""

import hashlib
import re
import json
import logging
from dataclasses import dataclass
from typing import Optional
from bs4 import BeautifulSoup

logger = logging.getLogger(__name__)


@dataclass
class DriftReport:
    structural_hash: str
    visual_hash: Optional[str]
    structural_drift: bool
    visual_drift: bool
    structural_similarity: float    # 0.0 = completely different, 1.0 = identical
    visual_similarity: float
    previous_structural_hash: Optional[str]
    previous_visual_hash: Optional[str]

    @property
    def has_drift(self) -> bool:
        return self.structural_drift or self.visual_drift

    @property
    def drift_severity(self) -> str:
        if not self.has_drift:
            return "none"
        if self.structural_similarity < 0.5:
            return "major"  # Complete redesign
        if self.structural_similarity < 0.8:
            return "moderate"
        return "minor"


def _dom_skeleton(html: str) -> str:
    """
    Extract structural skeleton from HTML — tags and key attributes only,
    stripping all text content. This gives a hash that's stable to text changes
    but sensitive to structural changes.
    """
    soup = BeautifulSoup(html, "lxml")

    # Remove scripts, styles, comments — we only care about structural DOM
    for tag in soup(["script", "style", "noscript", "svg", "img"]):
        tag.decompose()

    # Keep only tag name + key semantic attributes
    KEEP_ATTRS = {"id", "class", "role", "data-testid", "aria-label", "type", "name"}

    skeleton_parts = []
    for tag in soup.find_all(True):
        attrs = {k: v for k, v in tag.attrs.items() if k in KEEP_ATTRS}
        # Normalize class lists (sort for stability)
        if "class" in attrs and isinstance(attrs["class"], list):
            attrs["class"] = sorted(attrs["class"])
        skeleton_parts.append(f"{tag.name}:{json.dumps(attrs, sort_keys=True)}")

    skeleton = "\n".join(skeleton_parts)
    return hashlib.sha256(skeleton.encode()).hexdigest()


def _structural_similarity(hash1: str, hash2: str) -> float:
    """
    Simple similarity based on common prefix length of hashes.
    This is a rough heuristic — in production, compare shingle sets of the skeleton.
    """
    if hash1 == hash2:
        return 1.0
    # XOR-based bit similarity on hex hashes
    bytes1 = bytes.fromhex(hash1)
    bytes2 = bytes.fromhex(hash2)
    matching_bits = sum(8 - bin(b1 ^ b2).count("1") for b1, b2 in zip(bytes1, bytes2))
    total_bits = len(bytes1) * 8
    return matching_bits / total_bits


def _perceptual_hash(screenshot_bytes: Optional[bytes]) -> Optional[str]:
    """
    Compute perceptual hash of screenshot.
    Returns None if screenshot unavailable (non-blocking).
    """
    if not screenshot_bytes:
        return None
    try:
        import imagehash
        from PIL import Image
        import io
        img = Image.open(io.BytesIO(screenshot_bytes))
        return str(imagehash.phash(img))
    except Exception as e:
        logger.warning(f"Perceptual hash failed: {e}")
        return None


def _visual_similarity(hash1: Optional[str], hash2: Optional[str]) -> float:
    """
    Hamming distance between perceptual hashes.
    imagehash phash produces 64-bit hashes, so max distance = 64.
    """
    if not hash1 or not hash2:
        return 1.0  # Can't compare → assume no drift
    try:
        import imagehash
        h1 = imagehash.hex_to_hash(hash1)
        h2 = imagehash.hex_to_hash(hash2)
        distance = h1 - h2   # Hamming distance
        return 1.0 - (distance / 64.0)
    except Exception:
        return 1.0


class ChangeDetector:
    """
    Detects page drift by comparing current page against stored baseline.

    Thresholds (tunable):
    - structural_threshold: similarity below this → structural drift
    - visual_threshold: similarity below this → visual drift
    """

    def __init__(
        self,
        structural_threshold: float = 0.85,
        visual_threshold: float = 0.80,
    ):
        self.structural_threshold = structural_threshold
        self.visual_threshold = visual_threshold

    def compute_fingerprint(
        self,
        html: str,
        screenshot: Optional[bytes] = None,
    ) -> tuple[str, Optional[str]]:
        """Compute (structural_hash, visual_hash) for a page."""
        return _dom_skeleton(html), _perceptual_hash(screenshot)

    def detect(
        self,
        html: str,
        screenshot: Optional[bytes],
        baseline: Optional[dict],  # {"structural_hash": ..., "visual_hash": ...}
    ) -> DriftReport:
        """
        Compare current page against baseline fingerprint.
        Returns a DriftReport describing detected changes.
        """
        current_struct, current_visual = self.compute_fingerprint(html, screenshot)

        if baseline is None:
            # First visit — no baseline to compare, no drift by definition
            return DriftReport(
                structural_hash=current_struct,
                visual_hash=current_visual,
                structural_drift=False,
                visual_drift=False,
                structural_similarity=1.0,
                visual_similarity=1.0,
                previous_structural_hash=None,
                previous_visual_hash=None,
            )

        prev_struct = baseline.get("structural_hash")
        prev_visual = baseline.get("visual_hash")

        struct_sim = _structural_similarity(current_struct, prev_struct) if prev_struct else 1.0
        visual_sim = _visual_similarity(current_visual, prev_visual)

        struct_drift = struct_sim < self.structural_threshold
        visual_drift = visual_sim < self.visual_threshold

        if struct_drift:
            logger.warning(
                f"Structural drift detected! similarity={struct_sim:.2f} "
                f"(threshold={self.structural_threshold})"
            )
        if visual_drift:
            logger.warning(
                f"Visual drift detected! similarity={visual_sim:.2f} "
                f"(threshold={self.visual_threshold})"
            )

        return DriftReport(
            structural_hash=current_struct,
            visual_hash=current_visual,
            structural_drift=struct_drift,
            visual_drift=visual_drift,
            structural_similarity=struct_sim,
            visual_similarity=visual_sim,
            previous_structural_hash=prev_struct,
            previous_visual_hash=prev_visual,
        )
