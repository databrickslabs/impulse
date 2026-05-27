"""ObjectTracksConfig — ingestion mode for the object_tracks table.

Two modes:
  full_stride (default) — write one row per object at a fixed Hz for the full recording.
                           At 2 Hz and 1,000 recordings: ~108M rows.
                           Default because discovery ergonomics beat the row-count savings
                           of `tsal_gated` for most demo and exploratory workloads.
  tsal_gated             — write rows only for frames inside a TSAL event window ± buffer.
                           At 5% event coverage and 1,000 recordings: ~81M rows.
                           Opt-in for fleet-scale production runs where row count
                           dominates cost.

Anti-pattern: full resolution for all frames → ~1.62B rows at 1,000 recordings.
Anti-pattern: stride < 2 Hz → misses cyclist cut-in events lasting < 800ms (Nyquist floor).
"""

from dataclasses import dataclass, field


@dataclass
class ObjectTracksConfig:
    mode: str = "full_stride"
    pre_event_buffer_ms: int = 500
    post_event_buffer_ms: int = 500
    full_stride_hz: float = 2.0
    min_confidence: float = 0.5

    def __post_init__(self):
        if self.mode not in ("tsal_gated", "full_stride"):
            raise ValueError(f"mode must be 'tsal_gated' or 'full_stride', got {self.mode!r}")
        if self.full_stride_hz < 2.0:
            raise ValueError(
                f"full_stride_hz must be >= 2.0 (Nyquist floor for ADAS events); "
                f"got {self.full_stride_hz}"
            )

    @classmethod
    def tsal_gated(
        cls,
        pre_event_buffer_ms: int = 500,
        post_event_buffer_ms: int = 500,
        min_confidence: float = 0.5,
    ) -> "ObjectTracksConfig":
        return cls(
            mode="tsal_gated",
            pre_event_buffer_ms=pre_event_buffer_ms,
            post_event_buffer_ms=post_event_buffer_ms,
            min_confidence=min_confidence,
        )

    @classmethod
    def full_stride(
        cls,
        stride_hz: float = 2.0,
        min_confidence: float = 0.5,
    ) -> "ObjectTracksConfig":
        return cls(mode="full_stride", full_stride_hz=stride_hz, min_confidence=min_confidence)
