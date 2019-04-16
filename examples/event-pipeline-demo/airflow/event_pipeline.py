# Move this file to your Airflow dags directory (typically this is $AIRFLOW_HOME/dags) in order to
# run the event ingest pipeline in Airflow. You should make sure that dagster_airflow and the
# event_pipeline_demo are both installed in your Airflow environment, and you should edit
# PATH_TO_CONFIG_YML to point at the event-pipeline-demo/environments directory.
import os

from dagster.utils import load_yaml_from_globs
from dagster.utils.merger import dict_merge

from dagster_airflow.factory import make_airflow_dag

from event_pipeline_demo import define_event_ingest_pipeline

PATH_TO_CONFIG_YML = '/path/to/dagster/examples/event-pipeline-demo/environments/*.yml'

pipeline = define_event_ingest_pipeline()

secrets = {
    'solids': {
        'snowflake_load': {
            'config': {
                'account': os.getenv('SNOWFLAKE_ACCOUNT', '<< SET ME >>'),
                'user': os.getenv('SNOWFLAKE_USER', '<< SET ME >>'),
                'password': os.getenv('SNOWFLAKE_PASSWORD', '<< SET ME >>'),
            }
        }
    }
}

env_config = dict_merge(
    dict_merge(
        {'storage': {'filesystem': {'base_dir': '/tmp'}}}, load_yaml_from_globs(PATH_TO_CONFIG_YML)
    ),
    secrets,
)

dag, steps = make_airflow_dag(
    pipeline,
    env_config=env_config,
    dag_id='event_ingest_pipeline',
    dag_description='A demo Airflow DAG corresponding to the event ingest pipeline.',
    dag_kwargs=None,
    op_kwargs=None,
)
