"""Value data type of a time series' samples."""

from enum import StrEnum


class SeriesValueType(StrEnum):
    """The value data type of a channel's samples.

    ``DOUBLE`` — numeric values (default). ``STRING`` — string values (e.g. DTC
    codes). String series support only sampling and equality (``==`` / ``!=``);
    arithmetic, ordering and numeric reductions are **not implemented** for them.
    """

    DOUBLE = "double"
    STRING = "string"

    @property
    def is_numeric(self) -> bool:
        """Whether @_numeric_only ops (arithmetic, ordering, numeric reductions) apply."""
        return self in [SeriesValueType.DOUBLE]
