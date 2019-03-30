'''Scaffolding machinery for dagster-airflow.

Entrypoint is scaffold_airflow_dag, which consumes a pipeline definition and a config, and
generates an Airflow DAG definition each of whose nodes corresponds to one step of the execution
plan.
'''

import itertools
import os

from collections import defaultdict
from datetime import datetime, timedelta

from six import string_types
from yaml import dump

from dagster import check, PipelineDefinition
from dagster.core.execution import create_execution_plan

from .utils import IndentingBlockPrinter


DEFAULT_ARGS = {
    'owner': 'airflow',
    'depends_on_past': False,
    'email': ['airflow@example.com'],
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(0, 300),
}

DOCKER_TEMPDIR = '/tmp/results'


def _normalize_key(key):
    '''We need to make sure task ids play nicely with Airflow.'''
    return key.replace('_', '__').replace('.', '_')


def _is_py(path):
    '''We need to make sure we are writing out Python files.'''
    return path.split('.')[-1] == 'py'


def _bad_import(path):
    '''We need to make sure our relative import will work.'''
    return '.' in os.path.basename(path)[:-3]


def _split_lines(lines):
    '''Fancy utility adds a trailing comma to the last line of output.'''
    return (lines.strip('\n') + ',').split('\n')


def _steps_for_key(solid_name):
    return 'STEPS_FOR_' + solid_name.upper()


def _format_config(config):
    '''This recursive descent thing formats a config dict for GraphQL.'''

    def _format_config_subdict(config, current_indent=0):
        check.dict_param(config, 'config', key_type=str)

        printer = IndentingBlockPrinter(indent_level=2, current_indent=current_indent)
        printer.line('{')

        n_elements = len(config)
        for i, key in enumerate(sorted(config, key=lambda x: x[0])):
            value = config[key]
            with printer.with_indent():
                formatted_value = (
                    _format_config_item(value, current_indent=printer.current_indent)
                    .lstrip(' ')
                    .rstrip('\n')
                )
                printer.line(
                    '{key}: {formatted_value}{comma}'.format(
                        key=key,
                        formatted_value=formatted_value,
                        comma=',' if i != n_elements - 1 else '',
                    )
                )
        printer.line('}')

        return printer.read()

    def _format_config_sublist(config, current_indent=0):
        printer = IndentingBlockPrinter(indent_level=2, current_indent=current_indent)
        printer.line('[')

        n_elements = len(config)
        for i, value in enumerate(config):
            with printer.with_indent():
                formatted_value = (
                    _format_config_item(value, current_indent=printer.current_indent)
                    .lstrip(' ')
                    .rstrip('\n')
                )
                printer.line(
                    '{formatted_value}{comma}'.format(
                        formatted_value=formatted_value, comma=',' if i != n_elements - 1 else ''
                    )
                )
        printer.line(']')

        return printer.read()

    def _format_config_item(config, current_indent=0):
        printer = IndentingBlockPrinter(indent_level=2, current_indent=current_indent)

        if isinstance(config, dict):
            return _format_config_subdict(config, printer.current_indent)
        elif isinstance(config, list):
            return _format_config_sublist(config, printer.current_indent)
        elif isinstance(config, bool):
            return repr(config).lower()
        else:
            return repr(config).replace('\'', '"')

    check.dict_param(config, 'config', key_type=str)
    if not isinstance(config, dict):
        check.failed('Expected a dict to format as config, got: {item}'.format(item=repr(config)))

    return _format_config_subdict(config)


def _coalesce_solid_order(execution_plan):
    solid_order = [s.tags['solid'] for s in execution_plan.topological_steps()]
    reversed_coalesced_solid_order = []
    for solid in reversed(solid_order):
        if solid in reversed_coalesced_solid_order:
            continue
        reversed_coalesced_solid_order.append(solid)
    return [x for x in reversed(reversed_coalesced_solid_order)]


def coalesce_execution_steps(execution_plan):
    '''Groups execution steps by solid, in topological order of the solids.'''

    solid_order = _coalesce_solid_order(execution_plan)

    steps = defaultdict(list)

    for solid_name, solid_steps in itertools.groupby(
        execution_plan.topological_steps(), lambda x: x.tags['solid']
    ):
        steps[solid_name] += list(solid_steps)

    return [(solid_name, steps[solid_name]) for solid_name in solid_order]


def _make_editable_scaffold(
    pipeline_name, pipeline_description, env_config, static_scaffold, default_args, operator_kwargs
):

    pipeline_description = '***Autogenerated by dagster-airflow***' + (
        '''

    {pipeline_description}
    '''.format(
            pipeline_description=pipeline_description
        )
        if pipeline_description
        else ''
    )

    with IndentingBlockPrinter() as printer:
        printer.block(
            '\'\'\'Editable scaffolding autogenerated by dagster-airflow from pipeline '
            '{pipeline_name} with config:'.format(pipeline_name=pipeline_name)
        )
        printer.blank_line()

        with printer.with_indent():
            for line in dump(env_config).split('\n'):
                printer.line(line)
        printer.blank_line()

        printer.block(
            'By convention, users should attempt to isolate post-codegen changes and '
            'customizations to this "editable" file, rather than changing the definitions in the '
            '"static" {static_scaffold}.py file. Please let us know if you are encountering use '
            'cases where it is necessary to make changes to the static file.'.format(
                static_scaffold=static_scaffold
            )
        )
        printer.line('\'\'\'')
        printer.blank_line()

        printer.line('import datetime')
        printer.blank_line()

        printer.line(
            'from {static_scaffold} import make_dag'.format(static_scaffold=static_scaffold)
        )
        printer.blank_line()

        printer.comment(
            'Arguments to be passed to the ``default_args`` parameter of the ``airflow.DAG`` '
            'constructor.You can override these with values of your choice.'
        )
        printer.line('DEFAULT_ARGS = {')
        with printer.with_indent():
            for key, value in sorted(default_args.items(), key=lambda x: x[0]):
                printer.line('\'{key}\': {value_repr},'.format(key=key, value_repr=repr(value)))
        printer.line('}')
        printer.blank_line()

        printer.comment(
            'Any additional keyword arguments to be passed to the ``airflow.DAG`` constructor. '
            'You can override these with values of your choice.'
        )
        printer.line('DAG_KWARGS = {')
        with printer.with_indent():
            printer.line('\'schedule_interval\': \'0 0 * * *\',')
        printer.line('}')
        printer.blank_line()

        printer.comment(
            'The name of the autogenerated DAG. By default, this is just the name of the Dagster '
            'pipeline from which the Airflow DAG was generated ({pipeline_name}). You may want to '
            'override this if, for instance, you want to schedule multiple DAGs corresponding to '
            'different configurations of the same Dagster pipeline.'.format(
                pipeline_name=pipeline_name
            )
        )
        printer.line('DAG_ID = \'{pipeline_name}\''.format(pipeline_name=pipeline_name))
        printer.blank_line()

        printer.comment(
            'The description of the autogenerated DAG. By default, this is the description of the '
            'Dagster pipeline from which the Airflow DAG was generated. You may want to override '
            'this, as with the DAG_ID parameter.'
        )
        printer.line(
            'DAG_DESCRIPTION = \'\'\'{pipeline_description}\'\'\''.format(
                pipeline_description=pipeline_description
            )
        )
        printer.blank_line()

        printer.comment(
            'Additional arguments, if any, to pass through to the underlying '
            '``dagster_airflow.dagster_plugin.ModifiedDockerOperator`` constructor. Set these if, '
            'for instance, you need to set special TLS parameters.'
        )
        if not operator_kwargs:
            printer.line('OPERATOR_KWARGS = {}')
            printer.blank_line()
        else:
            printer.line('OPERATOR_KWARGS = {')
            with printer.with_indent():
                for key, value in sorted(operator_kwargs.items(), key=lambda x: x[0]):
                    printer.line('\'{key}\': {value_repr},'.format(key=key, value_repr=repr(value)))
            printer.line('}')
            printer.blank_line()

        # This is the canonical way to hide globals from import, but not from Airflow's DagBag
        printer.line(
            '# The \'unusual_prefix\' ensures that the following code will be executed only when'
        )
        printer.line(
            '# Airflow imports this file. See: '
            'https://bcb.github.io/airflow/hide-globals-in-dag-definition-file'
        )
        printer.line('if __name__.startswith(\'unusual_prefix\'):')
        with printer.with_indent():
            printer.line('dag, tasks = make_dag(')
            with printer.with_indent():
                printer.line('dag_id=DAG_ID,')
                printer.line('dag_description=DAG_DESCRIPTION,')
                printer.line('dag_kwargs=dict(default_args=DEFAULT_ARGS, **DAG_KWARGS),')
                printer.line('operator_kwargs=OPERATOR_KWARGS,')
            printer.line(')')

        return printer.read()


# pylint: disable=too-many-statements
def _make_static_scaffold(pipeline_name, env_config, execution_plan, image, editable_scaffold):
    with IndentingBlockPrinter() as printer:
        printer.block(
            '\'\'\'Static scaffolding autogenerated by dagster-airflow from pipeline '
            '{pipeline_name} with config:'.format(pipeline_name=pipeline_name)
        )
        printer.blank_line()

        with printer.with_indent():
            for line in dump(env_config).split('\n'):
                printer.line(line)
        printer.blank_line()

        printer.block(
            'By convention, users should attempt to isolate post-codegen changes and '
            'customizations to the "editable" {editable_scaffold}.py file, rather than changing '
            'the definitions in this "static" file. Please let us know if you are encountering '
            'use cases where it is necessary to make changes to the static file.'.format(
                editable_scaffold=editable_scaffold
            )
        )
        printer.line('\'\'\'')
        printer.blank_line()

        printer.line('from airflow import DAG')
        printer.line('try:')
        with printer.with_indent():
            printer.line('from airflow.operators.dagster_plugin import DagsterOperator')
        printer.line('except (ModuleNotFoundError, ImportError):')
        with printer.with_indent():
            printer.line('from dagster_airflow import DagsterOperator')
        printer.blank_line()
        printer.blank_line()

        printer.line('CONFIG = \'\'\'')
        with printer.with_indent():
            for line in _format_config(env_config).strip('\n').split('\n'):
                printer.line(line)
        printer.line('\'\'\'.strip(\'\\n\').strip(\' \')')
        printer.blank_line()
        printer.line('PIPELINE_NAME = \'{pipeline_name}\''.format(pipeline_name=pipeline_name))
        printer.blank_line()

        for (solid_name, solid_steps) in coalesce_execution_steps(execution_plan):
            steps_for_key = _steps_for_key(solid_name)

            printer.line('{steps_for_key} = ['.format(steps_for_key=steps_for_key))
            with printer.with_indent():
                for step in solid_steps:
                    printer.line('\'{step_key}\','.format(step_key=step.key))
            printer.line(']')
        printer.blank_line()

        printer.line('def make_dag(')
        with printer.with_indent():
            printer.line('dag_id,')
            printer.line('dag_description,')
            printer.line('dag_kwargs,')
            printer.line('operator_kwargs,')
        printer.line('):')
        with printer.with_indent():
            printer.line('dag = DAG(')
            with printer.with_indent():
                printer.line('dag_id=dag_id,')
                printer.line('description=dag_description,')
                printer.line('**dag_kwargs')
            printer.line(')')
            printer.blank_line()

            printer.line('tasks = []')
            printer.blank_line()

            for (solid_name, solid_steps) in coalesce_execution_steps(execution_plan):
                steps_for_key = _steps_for_key(solid_name)

                printer.line(
                    '{airflow_step_key}_task = DagsterOperator('.format(airflow_step_key=solid_name)
                )
                with printer.with_indent():
                    printer.line('step=\'{step_key}\','.format(step_key=solid_name))
                    printer.line('config=CONFIG,')
                    printer.line('dag=dag,')
                    printer.line('tmp_dir=\'{tmp}\','.format(tmp=DOCKER_TEMPDIR))
                    printer.line('image=\'{image}\','.format(image=image))
                    printer.line(
                        'task_id=\'{airflow_step_key}\','.format(airflow_step_key=solid_name)
                    )
                    printer.line('pipeline_name=PIPELINE_NAME,')
                    printer.line('step_keys={steps_for_key},'.format(steps_for_key=steps_for_key))
                    printer.line('**operator_kwargs')  # py 2
                printer.line(')')
                printer.line(
                    'tasks.append({airflow_step_key}_task)'.format(airflow_step_key=solid_name)
                )
                printer.blank_line()

            for (solid_name, solid_steps) in coalesce_execution_steps(execution_plan):
                for solid_step in solid_steps:
                    for step_input in solid_step.step_inputs:
                        prev_airflow_step_key = execution_plan.get_step_by_key(
                            step_input.prev_output_handle.step_key
                        ).tags['solid']
                        airflow_step_key = solid_name
                        if airflow_step_key != prev_airflow_step_key:
                            printer.line(
                                '{prev_airflow_step_key}_task.set_downstream('
                                '{airflow_step_key}_task)'.format(
                                    prev_airflow_step_key=prev_airflow_step_key,
                                    airflow_step_key=airflow_step_key,
                                )
                            )
            printer.blank_line()
            # We return both the DAG and the tasks to make testing, etc. easier
            printer.line('return (dag, tasks)')

        return printer.read()


def _generate_output_paths(pipeline_name, output_path):
    if output_path is None:
        static_path = os.path.join(
            os.getcwd(), '{pipeline_name}_static__scaffold.py'.format(pipeline_name=pipeline_name)
        )
        editable_path = os.path.join(
            os.getcwd(), '{pipeline_name}_editable__scaffold.py'.format(pipeline_name=pipeline_name)
        )
    elif isinstance(output_path, tuple):
        if not len(output_path) == 2:
            check.failed(
                'output_path must be a tuple(str, str), str, or None. Got a tuple with bad '
                'length {length}'.format(length=len(output_path))
            )

        if not os.path.isabs(output_path[0]) or not os.path.isabs(output_path[1]):
            check.failed(
                'Bad value for output_path: expected a tuple of absolute paths, but got '
                '({path_0}, {path_1}).'.format(path_0=output_path[0], path_1=output_path[1])
            )

        if not _is_py(output_path[0]) or not _is_py(output_path[1]):
            check.failed(
                'Bad value for output_path: expected a tuple of absolute paths to python files '
                '(*.py), but got ({path_0}, {path_1}).'.format(
                    path_0=output_path[0], path_1=output_path[1]
                )
            )

        if _bad_import(output_path[0]) or _bad_import(output_path[1]):
            check.failed(
                'Bad value for output_path: no dots permitted in filenames. Got: '
                '({path_0}, {path_1}).'.format(path_0=output_path[0], path_1=output_path[1])
            )

        static_path, editable_path = output_path
    elif isinstance(output_path, string_types):
        if not os.path.isabs(output_path):
            check.failed(
                'Bad value for output_path: expected an absolute path, but got {path}.'.format(
                    path=output_path
                )
            )

        if not os.path.isdir(output_path):
            check.failed(
                'Bad value for output_path: No directory found at {output_path}'.format(
                    output_path=output_path
                )
            )

        static_path = os.path.join(
            output_path, '{pipeline_name}_static__scaffold.py'.format(pipeline_name=pipeline_name)
        )
        editable_path = os.path.join(
            output_path, '{pipeline_name}_editable__scaffold.py'.format(pipeline_name=pipeline_name)
        )
    else:
        check.failed(
            'output_path must be a tuple(str, str), str, or None. Got: {type_}'.format(
                type_=type(output_path)
            )
        )

    return (static_path, editable_path)


# pylint: disable=too-many-locals
def scaffold_airflow_dag(
    pipeline,
    env_config,
    image,
    output_path=None,
    dag_kwargs=None,
    operator_kwargs=None,
    regenerate=False,
):
    '''Scaffold a new Airflow DAG based on a PipelineDefinition and config.

    Creates an "editable" scaffold (intended for end user modification) and a "static" scaffold.
    The editable scaffold imports the static scaffold and defines the Airflow DAG. As a rule, both
    scaffold files need to be present in your Airflow DAG directory (by default, this is
    $AIRFLOW_HOME/dags) in order to be correctly parsed by Airflow.

    Note that an Airflow DAG corresponds to a Dagster execution plan, since many different
    execution plans may be created when a PipelineDefinition is parametrized by various config
    values. You may want to create multiple Airflow DAGs corresponding to, e.g., test and
    production configs of your Dagster pipelines.

    Parameters:
        pipeline (dagster.PipelineDefinition): Pipeline to use to construct the Airflow DAG.
        env_config (dict): The config to use to construct the Airflow DAG.
        image (str): The name of the Docker image in which your pipeline has been containerized.
        output_path (Union[Tuple[str, str], str, None]): Optionally specify the path at which to
            write the scaffolded files. If this parameter is a tuple of absolute paths, the static
            scaffold will be written to the first member of the tuple and the editable scaffold
            will be written to the second member of the tuple. If this parameter is a path to a
            directory, the scaffold files will be written to that directory as
            '{pipeline_name}_static__scaffold.py' and '{pipeline_name}_editable__scaffold.py'
            respectively. If this parameter is None, the scaffolds will be written to the present
            working directory. Default: None.
        dag_kwargs (dict, optional): Any additional keyword arguments to pass to the ``airflow.DAG``
            constructor. If `dag_kwargs.default_args` is set, values set there will smash the
            values in ``dagster_airflow.scaffold.DEFAULT_ARGS``. Default: None.
        operator_kwargs (dict, optional): Any additional keyword arguments to pass to the
            ``DagsterOperator`` constructor. These will be passed through to the underlying
            ``ModifiedDockerOperator``. Default: None
        regenerate (bool): If true, write only the static scaffold. Default: False.

    Returns:
        (str, str): Paths to the static and editable scaffold files.
    '''
    check.inst_param(pipeline, 'pipeline', PipelineDefinition)
    check.opt_dict_param(env_config, 'env_config', key_type=str)
    dag_kwargs = check.opt_dict_param(dag_kwargs, 'dag_kwargs', key_type=str)
    operator_kwargs = check.opt_dict_param(operator_kwargs, 'operator_kwargs', key_type=str)
    check.bool_param(regenerate, 'regenerate')

    pipeline_name = pipeline.name
    pipeline_description = pipeline.description

    static_path, editable_path = _generate_output_paths(pipeline_name, output_path)

    execution_plan = create_execution_plan(pipeline, env_config)

    default_args = dict(
        dict(DEFAULT_ARGS, start_date=datetime.utcnow()), **(dag_kwargs.pop('default_args', {}))
    )

    operator_kwargs = operator_kwargs or {}

    editable_scaffold_module_name = os.path.basename(editable_path).split('.')[-2]
    static_scaffold_module_name = os.path.basename(static_path).split('.')[-2]

    static_scaffold = _make_static_scaffold(
        pipeline_name=pipeline_name,
        env_config=env_config,
        execution_plan=execution_plan,
        image=image,
        editable_scaffold=editable_scaffold_module_name,
    )

    if not regenerate:
        editable_scaffold = _make_editable_scaffold(
            pipeline_name=pipeline_name,
            pipeline_description=pipeline_description,
            env_config=env_config,
            static_scaffold=static_scaffold_module_name,
            default_args=default_args,
            operator_kwargs=operator_kwargs,
        )

    with open(static_path, 'w') as static_fd:
        static_fd.write(static_scaffold)

    if not regenerate:
        with open(editable_path, 'w') as editable_fd:
            editable_fd.write(editable_scaffold)

    return (static_path, editable_path)
