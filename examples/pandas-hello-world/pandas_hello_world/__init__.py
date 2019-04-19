import os
from dagster import (
    DependencyDefinition,
    InputDefinition,
    OutputDefinition,
    PipelineDefinition,
    lambda_solid,
    RepositoryDefinition,
)

from dagster_pandas import DataFrame
import dagstermill as dm


@lambda_solid(inputs=[InputDefinition('num', DataFrame)], output=OutputDefinition(DataFrame))
def sum_solid(num):
    sum_df = num.copy()
    sum_df['sum'] = sum_df['num1'] + sum_df['num2']
    return sum_df


@lambda_solid(inputs=[InputDefinition('sum_df', DataFrame)], output=OutputDefinition(DataFrame))
def sum_sq_solid(sum_df):
    sum_sq_df = sum_df.copy()
    sum_sq_df['sum_sq'] = sum_df['sum'] ** 2
    return sum_sq_df


def define_pandas_hello_world_pipeline():
    return PipelineDefinition(
        name='pandas_hello_world',
        solids=[sum_solid, sum_sq_solid],
        dependencies={
            'sum_solid': {},
            'sum_sq_solid': {'sum_df': DependencyDefinition(sum_solid.name)},
        },
    )


def nb_test_path(name):
    return os.path.join(
        os.path.dirname(os.path.realpath(__file__)), 'notebooks/{name}.ipynb'.format(name=name)
    )


def define_papermill_pandas_hello_world_solid():
    return dm.define_dagstermill_solid(
        name='papermill_pandas_hello_world',
        notebook_path=nb_test_path('papermill_pandas_hello_world'),
        inputs=[InputDefinition(name='df', dagster_type=DataFrame)],
        outputs=[OutputDefinition(DataFrame)],
    )


def define_papermill_pandas_hello_world_pipeline():
    return PipelineDefinition(
        name='papermill_pandas_hello_world_pipeline',
        solids=[define_papermill_pandas_hello_world_solid()],
    )


def define_pandas_repository():
    return RepositoryDefinition(
        name='test_dagstermill_pandas_solids',
        pipeline_dict={
            'papermill_pandas_hello_world_pipeline': define_papermill_pandas_hello_world_pipeline,
            'pandas_hello_world': define_pandas_hello_world_pipeline,
        },
    )
