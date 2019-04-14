import os
import uuid

import pytest

from dagster import (
    Bool as Bool_,
    check,
    List as List_,
    String as String_,
    PipelineDefinition,
    RunConfig,
)
from dagster.core.execution import yield_pipeline_execution_context
from dagster.core.types.marshal import SerializationStrategy
from dagster.core.object_store import TypeStoragePlugin
from dagster.core.types.runtime import resolve_to_runtime_type, RuntimeType, String

from dagster_aws.s3_object_store import S3ObjectStore


def aws_credentials_present():
    return os.getenv('AWS_ACCESS_KEY_ID') and os.getenv('AWS_SECRET_ACCESS_KEY')


aws = pytest.mark.skipif(not aws_credentials_present(), reason='Couldn\'t find AWS credentials')
nettest = pytest.mark.nettest


class UppercaseSerializationStrategy(SerializationStrategy):  # pylint: disable=no-init
    def serialize_value(self, _context, value, write_file_obj):
        return write_file_obj.write(bytes(value.upper().encode('utf-8')))

    def deserialize_value(self, _context, read_file_obj):
        return read_file_obj.read().decode('utf-8').lower()


class LowercaseString(RuntimeType):
    def __init__(self):
        super(LowercaseString, self).__init__(
            'lowercase_string',
            'LowercaseString',
            serialization_strategy=UppercaseSerializationStrategy(),
        )


class FancyStringS3TypeStoragePlugin(TypeStoragePlugin):  # pylint:disable=no-init
    @classmethod
    def set_object(cls, object_store, obj, context, runtime_type, paths):
        check.inst_param(object_store, 'object_store', S3ObjectStore)
        paths.append(obj)
        return object_store.set_object('', context, runtime_type, paths)

    @classmethod
    def get_object(cls, object_store, context, runtime_type, paths):
        check.inst_param(object_store, 'object_store', S3ObjectStore)
        return object_store.s3.list_objects(
            Bucket=object_store.bucket, Prefix=object_store.key_for_paths(paths)
        )['Contents'][0]['Key'].split('/')[-1]


@aws
@nettest
def test_s3_object_store_with_custom_serializer():
    run_id = str(uuid.uuid4())

    # FIXME need a dedicated test bucket
    object_store = S3ObjectStore(run_id=run_id, s3_bucket='dagster-airflow-scratch')

    with yield_pipeline_execution_context(
        PipelineDefinition([]), {}, RunConfig(run_id=run_id)
    ) as context:
        try:
            object_store.set_object('foo', context, LowercaseString.inst(), ['foo'])

            assert (
                object_store.s3.get_object(
                    Bucket=object_store.bucket, Key='/'.join([object_store.root] + ['foo'])
                )['Body']
                .read()
                .decode('utf-8')
                == 'FOO'
            )

            assert object_store.has_object(context, ['foo'])
            assert object_store.get_object(context, LowercaseString.inst(), ['foo']) == 'foo'
        finally:
            object_store.rm_object(context, ['foo'])


@aws
@nettest
def test_s3_object_store_composite_types_with_custom_serializer_for_inner_type():
    run_id = str(uuid.uuid4())

    object_store = S3ObjectStore(run_id=run_id, s3_bucket='dagster-airflow-scratch')
    with yield_pipeline_execution_context(
        PipelineDefinition([]), {}, RunConfig(run_id=run_id)
    ) as context:
        try:
            object_store.set_object(
                ['foo', 'bar'],
                context,
                resolve_to_runtime_type(List_(LowercaseString)).inst(),
                ['list'],
            )
            assert object_store.has_object(context, ['list'])
            assert object_store.get_object(
                context, resolve_to_runtime_type(List_(Bool_)).inst(), ['list']
            ) == ['foo', 'bar']

        finally:
            object_store.rm_object(context, ['foo'])


@aws
@nettest
def test_s3_object_store_with_type_storage_plugin():
    run_id = str(uuid.uuid4())

    # FIXME need a dedicated test bucket
    object_store = S3ObjectStore(
        run_id=run_id,
        s3_bucket='dagster-airflow-scratch',
        types_to_register={String.inst(): FancyStringS3TypeStoragePlugin},
    )

    with yield_pipeline_execution_context(
        PipelineDefinition([]), {}, RunConfig(run_id=run_id)
    ) as context:
        try:
            object_store.set_value('hello', context, String.inst(), ['obj_name'])

            assert object_store.has_object(context, ['obj_name'])
            assert object_store.get_value(context, String.inst(), ['obj_name']) == 'hello'

        finally:
            object_store.rm_object(context, ['obj_name'])


@aws
@nettest
def test_s3_object_store_with_composite_type_storage_plugin():
    run_id = str(uuid.uuid4())

    # FIXME need a dedicated test bucket
    object_store = S3ObjectStore(
        run_id=run_id,
        s3_bucket='dagster-airflow-scratch',
        types_to_register={String.inst(): FancyStringS3TypeStoragePlugin},
    )

    with yield_pipeline_execution_context(
        PipelineDefinition([]), {}, RunConfig(run_id=run_id)
    ) as context:
        with pytest.raises(check.NotImplementedCheckError):
            object_store.set_value(
                ['hello'], context, resolve_to_runtime_type(List_(String_)), ['obj_name']
            )
