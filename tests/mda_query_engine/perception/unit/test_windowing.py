"""Unit tests for mda_query_engine.perception.windowing.in_any_window — the pure-Python predicate.

The Spark DataFrame helper `filter_dataframe_to_windows` is tested in
tests/mda_query_engine/perception/integration/test_windowing.py where a real SparkSession is
available.
"""

import pytest

from mda_query_engine.perception.windowing import DEFAULT_BUFFER_US, in_any_window


class TestInAnyWindow:
    def test_inside_single_window(self):
        assert in_any_window(1_500_000, [(1_000_000, 2_000_000)], pre_buffer_us=0, post_buffer_us=0)

    def test_outside_single_window(self):
        assert not in_any_window(
            2_500_000, [(1_000_000, 2_000_000)], pre_buffer_us=0, post_buffer_us=0
        )

    def test_at_window_start_inclusive(self):
        assert in_any_window(
            1_000_000, [(1_000_000, 2_000_000)], pre_buffer_us=0, post_buffer_us=0
        )

    def test_at_window_end_inclusive(self):
        assert in_any_window(
            2_000_000, [(1_000_000, 2_000_000)], pre_buffer_us=0, post_buffer_us=0
        )

    def test_pre_buffer_extends_left(self):
        # ts is 200 ms before window start; 500 ms pre-buffer covers it.
        assert in_any_window(
            800_000, [(1_000_000, 2_000_000)], pre_buffer_us=500_000, post_buffer_us=0
        )
        assert not in_any_window(
            400_000, [(1_000_000, 2_000_000)], pre_buffer_us=500_000, post_buffer_us=0
        )

    def test_post_buffer_extends_right(self):
        assert in_any_window(
            2_300_000, [(1_000_000, 2_000_000)], pre_buffer_us=0, post_buffer_us=500_000
        )
        assert not in_any_window(
            2_700_000, [(1_000_000, 2_000_000)], pre_buffer_us=0, post_buffer_us=500_000
        )

    def test_multiple_windows_any_match(self):
        windows = [(1_000_000, 2_000_000), (5_000_000, 6_000_000)]
        assert in_any_window(5_500_000, windows, pre_buffer_us=0, post_buffer_us=0)

    def test_empty_window_list_returns_false(self):
        assert not in_any_window(1_000_000, [])

    def test_default_buffer_is_500ms(self):
        assert DEFAULT_BUFFER_US == 500_000
        # 400 ms before window start is covered by the default 500 ms buffer.
        assert in_any_window(600_000, [(1_000_000, 2_000_000)])
        # 600 ms before — not covered.
        assert not in_any_window(400_000, [(1_000_000, 2_000_000)])
