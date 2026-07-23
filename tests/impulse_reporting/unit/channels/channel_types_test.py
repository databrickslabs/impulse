# pylint: disable=missing-function-docstring
"""Unit tests for the ChannelType registry."""

import pytest
from pyspark.sql.types import StructType

from impulse_reporting.channels.calculated_channel import CalculatedChannel
from impulse_reporting.channels.channel_types import ChannelType


def test_member_maps_to_class():
    assert ChannelType.CALCULATED_CHANNEL.value is CalculatedChannel


def test_table_names():
    assert ChannelType.CALCULATED_CHANNEL.get_fact_table_name() == "calculated_channel_fact"
    assert (
        ChannelType.CALCULATED_CHANNEL.get_dimension_table_name() == "calculated_channel_dimension"
    )


def test_schemas_non_empty():
    assert isinstance(ChannelType.CALCULATED_CHANNEL.get_fact_schema(), StructType)
    assert isinstance(ChannelType.CALCULATED_CHANNEL.get_dimension_schema(), StructType)
    assert len(ChannelType.CALCULATED_CHANNEL.get_fact_schema().fields) > 0
    assert len(ChannelType.CALCULATED_CHANNEL.get_dimension_schema().fields) > 0


def test_reverse_lookup_round_trips():
    assert (
        ChannelType.get_any_for_fact_table("calculated_channel_fact")
        is ChannelType.CALCULATED_CHANNEL
    )
    assert (
        ChannelType.get_any_for_dimension_table("calculated_channel_dimension")
        is ChannelType.CALCULATED_CHANNEL
    )


def test_reverse_lookup_raises_on_unknown():
    with pytest.raises(ValueError, match="No ChannelType found"):
        ChannelType.get_any_for_fact_table("nonexistent_table")
    with pytest.raises(ValueError, match="No ChannelType found"):
        ChannelType.get_any_for_dimension_table("nonexistent_table")
