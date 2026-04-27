import pytest

from mda_query_engine.measurement_db import MeasurementDB, MeasurementDBConfig


@pytest.fixture
def narrow_db() -> MeasurementDB:
    cfg = MeasurementDBConfig()
    db = MeasurementDB(cfg)
    return db
