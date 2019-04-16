"""Pipeline definitions for the airline_demo."""

import gzip
import os
import shutil

from dagster import (
    lambda_solid,
    DependencyDefinition,
    InputDefinition,
    OutputDefinition,
    List,
    Nothing,
    Path,
    PipelineDefinition,
    SolidInstance,
    String,
)
from dagster.utils import safe_isfile, mkdir_p

from dagster_aws import download_from_s3
from dagster_spark import SparkSolidDefinition
from dagster_snowflake import SnowflakeSolidDefinition, SnowflakeLoadSolidDefinition
from dagstermill import define_dagstermill_solid


@lambda_solid(inputs=[InputDefinition('gzip_file', String)], output=OutputDefinition(List(String)))
def gunzipper(gzip_file):
    '''gunzips /path/to/foo.gz to /path/to/raw/2019/01/01/data.json
    '''
    # TODO: take date as an input

    path_prefix = os.path.dirname(gzip_file)
    output_folder = os.path.join(path_prefix, 'raw/2019/01/01')
    outfile = os.path.join(output_folder, 'data.json')

    if not safe_isfile(outfile):
        mkdir_p(output_folder)

        with gzip.open(gzip_file, 'rb') as f_in, open(outfile, 'wb') as f_out:
            shutil.copyfileobj(f_in, f_out)

    return [path_prefix]


geo_events = define_dagstermill_solid(
    'geo_events',
    os.path.join(
        os.path.dirname(os.path.abspath(__file__)),
        'notebooks',
        'Geographic_Distribution_of_Recent_Events.ipynb',
    ),
    inputs=[InputDefinition('table_ready', Nothing)],
    outputs=[OutputDefinition(Path, 'path')],
)


def define_event_ingest_pipeline():
    event_ingest = SparkSolidDefinition(
        name='event_ingest',
        main_class='io.dagster.events.EventPipeline',
        description='Ingest events from JSON to Parquet',
    )

    # TODO: express dependency of this solid on event_ingest
    snowflake_load = SnowflakeLoadSolidDefinition(
        'snowflake_load',
        # TODO: need to pull this out to a config
        src='file:///tmp/dagster/events/data/output/2019/01/01/*.parquet',
        table='events',
    )

    return PipelineDefinition(
        name='event_ingest_pipeline',
        solids=[download_from_s3, gunzipper, event_ingest, snowflake_load, geo_events],
        dependencies={
            SolidInstance('gunzipper'): {'gzip_file': DependencyDefinition('download_from_s3')},
            SolidInstance('event_ingest'): {'spark_inputs': DependencyDefinition('gunzipper')},
            SolidInstance('snowflake_load'): {
                SnowflakeSolidDefinition.INPUT_READY: DependencyDefinition('event_ingest', 'paths')
            },
            'geo_events': {'table_ready': DependencyDefinition('snowflake_load')},
        },
    )
