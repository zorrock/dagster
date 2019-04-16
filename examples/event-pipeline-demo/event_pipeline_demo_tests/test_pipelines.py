import pandas as pd
import pytest

from dagster import execute_pipeline

# another py2/3 difference
try:
    import unittest.mock as mock
except ImportError:
    import mock

from dagster.utils import load_yaml_from_globs, script_relative_path

from event_pipeline_demo.pipelines import define_event_ingest_pipeline

spark = pytest.mark.spark
'''Tests that require Spark.'''

skip = pytest.mark.skip


def create_mock_connector(*_args, **_kwargs):
    return connect_with_fetchall_returning(pd.DataFrame())


def connect_with_fetchall_returning(value):
    cursor_mock = mock.MagicMock()
    cursor_mock.fetchall.return_value = value
    snowflake_connect = mock.MagicMock()
    snowflake_connect.cursor.return_value = cursor_mock
    m = mock.Mock()
    m.return_value = snowflake_connect
    return m


# To support this test, we need to do the following:
# 1. Have CircleCI publish Scala/Spark jars when that code changes
# 2. Ensure we have Spark available to CircleCI
# 3. Include example / test data in this repository
# pylint: disable=unused-argument
@spark
@mock.patch('snowflake.connector.connect', new_callable=create_mock_connector)
def test_event_pipeline(snowflake_connect, spark_home, secrets_yml):
    config = load_yaml_from_globs(
        script_relative_path('../environments/default.yml'),
        script_relative_path('../environments/secrets.yml'),
    )
    result_pipeline = execute_pipeline(define_event_ingest_pipeline(), config)
    assert result_pipeline.success

    # We're not testing Snowflake loads here, so at least test that we called the connect
    # appropriately
    snowflake_connect.assert_called_once()
