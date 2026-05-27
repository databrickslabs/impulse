"""LakeVision side-car schema for per-object event windows."""

import pyspark.sql.types as T

import mda_query_engine.perception.schema as S


class TestSideCarSchemaShape:
    def test_has_expected_fields(self):
        names = {f.name for f in S.PERCEPTION_EVENT_INSTANCE_OBJECTS}
        assert names == {"container_id", "event_id", "event_instance_id", "object_id"}

    def test_all_fields_are_long_and_non_null(self):
        for field in S.PERCEPTION_EVENT_INSTANCE_OBJECTS:
            assert isinstance(field.dataType, T.LongType), (
                f"{field.name} must be LongType; got {field.dataType}"
            )
            assert field.nullable is False, (
                f"{field.name} must be NOT NULL"
            )

    def test_primary_key_columns_present(self):
        # PK is (container_id, event_id, event_instance_id) — at most one
        # side-car row per event_instance_fact row.
        names = [f.name for f in S.PERCEPTION_EVENT_INSTANCE_OBJECTS]
        for col in ("container_id", "event_id", "event_instance_id"):
            assert col in names


class TestSideCarSchemaReExported:
    def test_top_level_re_export(self):
        import mda_query_engine.perception.schema as S_alias
        assert S_alias.PERCEPTION_EVENT_INSTANCE_OBJECTS is S.PERCEPTION_EVENT_INSTANCE_OBJECTS

    def test_listed_in_all(self):
        import mda_query_engine.perception.schema as S_alias
        assert "PERCEPTION_EVENT_INSTANCE_OBJECTS" in S_alias.__all__


class TestPerceptionDBConfigSurfacesSideCarTable:
    def test_unity_catalog_default_locates_side_car_in_perception_silver(self):
        from mda_query_engine.perception.perception_db import PerceptionDBConfig
        cfg = PerceptionDBConfig.for_unity_catalog("my_catalog")
        assert (
            cfg.perception_event_instance_objects_table
            == "my_catalog.perception_silver.perception_event_instance_objects"
        )

    def test_unity_catalog_custom_perception_schema_honoured(self):
        from mda_query_engine.perception.perception_db import PerceptionDBConfig
        cfg = PerceptionDBConfig.for_unity_catalog(
            "cat", perception_schema="p"
        )
        assert (
            cfg.perception_event_instance_objects_table
            == "cat.p.perception_event_instance_objects"
        )
