import pytest
from dagster import config
from dagster.core import types
from dagster.core.definitions import (
    OutputDefinition,
    InputDefinition,
    DagsterInvalidDefinitionError,
)
from dagster.core.decorators import solid, source, materialization, with_context
from dagster.core.execution import (
    output_single_solid,
    execute_single_solid,
    DagsterExecutionContext,
)

# This file tests a lot of parameter name stuff
# So these warnings are spurious
# unused variables, unused arguments
# pylint: disable=W0612, W0613


def create_test_context():
    return DagsterExecutionContext()


def create_empty_test_env():
    return config.Environment(inputs=[])


def test_solid():
    @solid()
    def hello_world():
        return {'foo': 'bar'}

    result = execute_single_solid(
        create_test_context(),
        hello_world,
        environment=create_empty_test_env(),
    )

    assert result.success

    assert result.transformed_value['foo'] == 'bar'


def test_solid_with_name():
    @solid(name="foobar")
    def hello_world():
        return {'foo': 'bar'}

    result = execute_single_solid(
        create_test_context(),
        hello_world,
        environment=create_empty_test_env(),
    )

    assert result.success

    assert result.transformed_value['foo'] == 'bar'


def test_solid_with_context():
    @solid(name="foobar")
    @with_context
    def hello_world(_context):
        return {'foo': 'bar'}

    result = execute_single_solid(
        create_test_context(),
        hello_world,
        environment=create_empty_test_env(),
    )

    assert result.success

    assert result.transformed_value['foo'] == 'bar'


def test_solid_with_input():
    @source(name="TEST", argument_def_dict={'foo': types.STRING})
    def test_source(foo):
        return {'foo': foo}

    @solid(inputs=[InputDefinition(name="foo_to_foo", sources=[test_source])])
    def hello_world(foo_to_foo):
        return foo_to_foo

    result = execute_single_solid(
        create_test_context(),
        hello_world,
        environment=config.Environment(
            inputs=[config.Input(
                input_name='foo_to_foo',
                args={'foo': 'bar'},
                source='TEST',
            )]
        ),
    )

    assert result.success

    assert result.transformed_value['foo'] == 'bar'


def test_sources():
    @source(name="WITH_CONTEXT", argument_def_dict={'foo': types.STRING})
    @with_context
    def context_source(_context, foo):
        return {'foo': foo}

    @source(name="NO_CONTEXT", argument_def_dict={'foo': types.STRING})
    def no_context_source(foo):
        return {'foo': foo}

    @solid(inputs=[InputDefinition(name="i", sources=[context_source, no_context_source])])
    def hello_world(i):
        return i

    result = execute_single_solid(
        create_test_context(),
        hello_world,
        environment=config.Environment(
            inputs=[config.Input(
                input_name='i',
                args={'foo': 'bar'},
                source='NO_CONTEXT',
            )]
        ),
    )

    assert result.success

    assert result.transformed_value['foo'] == 'bar'

    result = execute_single_solid(
        create_test_context(),
        hello_world,
        environment=config.Environment(
            inputs=[config.Input(
                input_name='i',
                args={'foo': 'bar'},
                source='WITH_CONTEXT',
            )]
        ),
    )

    assert result.success

    assert result.transformed_value['foo'] == 'bar'


def test_materializations():
    test_output = {}

    @materialization(name="CONTEXT", argument_def_dict={'foo': types.STRING})
    @with_context
    def materialization_with_context(_context, data, foo):
        test_output['test'] = data

    @materialization(name="NO_CONTEXT", argument_def_dict={'foo': types.STRING})
    def materialization_no_context(data, foo):
        test_output['test'] = data

    @solid(
        output=OutputDefinition(
            materializations=[materialization_with_context, materialization_no_context]
        )
    )
    def hello():
        return {'foo': 'bar'}

    output_single_solid(
        create_test_context(),
        hello,
        environment=config.Environment(inputs=[]),
        materialization_type='CONTEXT',
        arg_dict={'foo': 'bar'}
    )

    assert test_output['test'] == {'foo': 'bar'}

    test_output = {}

    output_single_solid(
        create_test_context(),
        hello,
        environment=config.Environment(inputs=[]),
        materialization_type='NO_CONTEXT',
        arg_dict={'foo': 'bar'}
    )

    assert test_output['test'] == {'foo': 'bar'}


def test_solid_definition_errors():
    @source(name="test_source")
    def test_source():
        pass

    with pytest.raises(DagsterInvalidDefinitionError):

        @solid(
            inputs=[InputDefinition(name="foo", sources=[test_source])], output=OutputDefinition()
        )
        @with_context
        def vargs(_context, foo, *args):
            pass

    with pytest.raises(DagsterInvalidDefinitionError):

        @solid(
            inputs=[InputDefinition(name="foo", sources=[test_source])], output=OutputDefinition()
        )
        def wrong_name(bar):
            pass

    with pytest.raises(DagsterInvalidDefinitionError):

        @solid(
            inputs=[
                InputDefinition(name="foo", sources=[test_source]),
                InputDefinition(name="bar", sources=[test_source])
            ],
            output=OutputDefinition()
        )
        def wrong_name_2(foo):
            pass

    with pytest.raises(DagsterInvalidDefinitionError):

        @solid(
            inputs=[InputDefinition(name="foo", sources=[test_source])], output=OutputDefinition()
        )
        @with_context
        def no_context(foo):
            pass

    with pytest.raises(DagsterInvalidDefinitionError):

        @solid(
            inputs=[InputDefinition(name="foo", sources=[test_source])], output=OutputDefinition()
        )
        def yes_context(_context, foo):
            pass

    with pytest.raises(DagsterInvalidDefinitionError):

        @solid(
            inputs=[InputDefinition(name="foo", sources=[test_source])], output=OutputDefinition()
        )
        def extras(foo, bar):
            pass

    @solid(
        inputs=[
            InputDefinition(name="foo", sources=[test_source]),
            InputDefinition(name="bar", sources=[test_source])
        ],
        output=OutputDefinition()
    )
    def valid_kwargs(**kwargs):
        pass

    @solid(
        inputs=[
            InputDefinition(name="foo", sources=[test_source]),
            InputDefinition(name="bar", sources=[test_source])
        ],
        output=OutputDefinition()
    )
    def valid(foo, bar):
        pass

    @solid(
        inputs=[
            InputDefinition(name="foo", sources=[test_source]),
            InputDefinition(name="bar", sources=[test_source])
        ],
        output=OutputDefinition()
    )
    @with_context
    def valid_rontext(context, foo, bar):
        pass

    @solid(
        inputs=[
            InputDefinition(name="foo", sources=[test_source]),
            InputDefinition(name="bar", sources=[test_source])
        ],
        output=OutputDefinition()
    )
    @with_context
    def valid_context_2(_context, foo, bar):
        pass


def test_source_definition_errors():
    with pytest.raises(DagsterInvalidDefinitionError):

        @source(argument_def_dict={'foo': types.STRING})
        @with_context
        def vargs(context, foo, *args):
            pass

    with pytest.raises(DagsterInvalidDefinitionError):

        @source(argument_def_dict={'foo': types.STRING})
        def wrong_name(bar):
            pass

    with pytest.raises(DagsterInvalidDefinitionError):

        @source(argument_def_dict={'foo': types.STRING, 'bar': types.STRING})
        def wrong_name_2(foo):
            pass

    with pytest.raises(DagsterInvalidDefinitionError):

        @source(argument_def_dict={'foo': types.STRING})
        @with_context
        def no_context(foo):
            pass

    with pytest.raises(DagsterInvalidDefinitionError):

        @source(argument_def_dict={'foo': types.STRING})
        def yes_context(context, foo):
            pass

    with pytest.raises(DagsterInvalidDefinitionError):

        @source(argument_def_dict={'foo': types.STRING})
        def extras(foo, bar):
            pass

    @source(argument_def_dict={'foo': types.STRING})
    def valid_kwargs(**kwargs):
        pass

    @source(argument_def_dict={'foo': types.STRING, 'bar': types.STRING})
    def valid(foo, bar):
        pass

    @source(argument_def_dict={'foo': types.STRING, 'bar': types.STRING})
    @with_context
    def valid_rontext(context, foo, bar):
        pass

    @source(argument_def_dict={'foo': types.STRING, 'bar': types.STRING})
    @with_context
    def valid_context_2(_context, foo, bar):
        pass


def test_materialization_definition_errors():
    with pytest.raises(DagsterInvalidDefinitionError):

        @materialization(argument_def_dict={'foo': types.STRING})
        @with_context
        def no_data(context, foo):
            pass

    with pytest.raises(DagsterInvalidDefinitionError):

        @materialization(argument_def_dict={'foo': types.STRING})
        @with_context
        def vargs(context, data, foo, *args):
            pass

    with pytest.raises(DagsterInvalidDefinitionError):

        @materialization(argument_def_dict={'foo': types.STRING})
        def wrong_name(data, bar):
            pass

    with pytest.raises(DagsterInvalidDefinitionError):

        @materialization(argument_def_dict={'foo': types.STRING, 'bar': types.STRING})
        def wrong_name_2(data, foo):
            pass

    with pytest.raises(DagsterInvalidDefinitionError):

        @materialization(argument_def_dict={'foo': types.STRING})
        @with_context
        def no_context(data, foo):
            pass

    with pytest.raises(DagsterInvalidDefinitionError):

        @materialization(argument_def_dict={'foo': types.STRING})
        def yes_context(context, data, foo):
            pass

    with pytest.raises(DagsterInvalidDefinitionError):

        @materialization(argument_def_dict={'foo': types.STRING})
        def extras(data, foo, bar):
            pass

    @materialization(argument_def_dict={'foo': types.STRING})
    def valid_kwargs(data, **kwargs):
        pass

    @materialization(argument_def_dict={'foo': types.STRING, 'bar': types.STRING})
    def valid(data, foo, bar):
        pass

    @materialization(argument_def_dict={'foo': types.STRING, 'bar': types.STRING})
    @with_context
    def valid_rontext(context, data, foo, bar):
        pass

    @materialization(argument_def_dict={'foo': types.STRING, 'bar': types.STRING})
    @with_context
    def valid_context_2(_context, _data, foo, bar):
        pass