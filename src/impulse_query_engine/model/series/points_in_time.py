"""PointInTime class implementation"""

from __future__ import annotations

import numpy as np
import numpy.typing as npt
import pyspark.sql.types as T


class PointsInTime:
    def __init__(self, tstarts: npt.NDArray):
        """
        Initialize the PointsInTime object.

        Parameters
        ----------
        tstarts : numpy.ndarray or array-like
            Array of time points.
        """
        self.tstarts = np.array(tstarts, dtype=np.float64)

    def dtype(self):
        """
        Returns the Spark data type for points in time.

        Returns
        -------
        pyspark.sql.types.ArrayType
           Spark ArrayType for points in time: [tstart_1, ..., tstart_N].
        """
        return T.ArrayType(T.DoubleType())

    def get_data(self) -> list:
        """
        Returns a list of time points.

        Returns
        -------
        list
            List of time points.
        """
        return self.tstarts

    def __len__(self):
        """
        Returns the number of time points.

        Returns
        -------
        int
            Number of time points.
        """
        return len(self.tstarts)

    def __and__(self, other: PointsInTime) -> PointsInTime:
        """
        Returns the intersection with another PointsInTime object.

        Parameters
        ----------
        other : PointsInTime
            PointsInTime object to intersect with.

        Returns
        -------
        PointsInTime
            Intersection result.
        """
        return PointsInTime(np.intersect1d(self.tstarts, other.tstarts, assume_unique=True))

    def __or__(self, other: PointsInTime) -> PointsInTime:
        """
        Returns the union with another PointsInTime object.

        Parameters
        ----------
        other : PointsInTime
            PointsInTime object to union with.

        Returns
        -------
        PointsInTime
            Union result.
        """
        result = np.union1d(self.tstarts, other.tstarts)
        return PointsInTime(np.array(sorted(result)))

    def expand_right(self, width: float):
        """
        Expands each point in time to an interval to the right by the given width.

        Parameters
        ----------
        width : float
            Amount to expand to the right (in seconds).

        Returns
        -------
        Intervals
            Intervals object with expanded right bounds.
        """
        from impulse_query_engine.model.series.intervals import Intervals

        return Intervals(self.tstarts, self.tstarts + width, merge_overlaps=True)

    def expand_left(self, width: float):
        """
        Expands each point in time to an interval to the left by the given width.

        Parameters
        ----------
        width : float
            Amount to expand to the left (in seconds).

        Returns
        -------
        Intervals
            Intervals object with expanded left bounds.
        """
        from impulse_query_engine.model.series.intervals import Intervals

        return Intervals(self.tstarts - width, self.tstarts, merge_overlaps=True)

    def expand(self, width: float):
        """
        Expands each point in time to an interval on both sides by the given width.

        Parameters
        ----------
        width : float
            Amount to expand both sides (in seconds).

        Returns
        -------
        Intervals
            Intervals object with expanded bounds.
        """
        from impulse_query_engine.model.series.intervals import Intervals

        return Intervals(self.tstarts - width, self.tstarts + width, merge_overlaps=True)

    @staticmethod
    def empty():
        """
        Returns an empty PointsInTime object.

        Returns
        -------
        PointsInTime
            Empty PointsInTime object.
        """
        return PointsInTime([])
