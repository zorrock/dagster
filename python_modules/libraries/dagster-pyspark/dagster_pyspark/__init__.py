# getting false positives in vscode
from pyspark.sql import SparkSession  # pylint: disable=no-name-in-module,import-error
from pyspark.rdd import RDD  # pylint: disable=no-name-in-module,import-error

from dagster import (
    Bool,
    Dict,
    Field,
    Selector,
    Path,
    String,
    as_dagster_type,
    check,
    input_selector_schema,
    output_selector_schema,
    resource,
)
from dagster.core.object_store import get_valid_target_path, TypeStoragePlugin
from dagster.core.runs import RunStorageMode
from dagster.core.types.runtime import define_any_type

from dagster_spark.configs_spark import spark_config

from dagster_spark.utils import flatten_dict


@input_selector_schema(
    Selector(
        {
            'csv': Field(
                Dict(
                    {
                        'path': Field(Path),
                        'sep': Field(String, is_optional=True),
                        'header': Field(Bool, is_optional=True),
                    }
                )
            )
        }
    )
)
def load_rdd(context, file_type, file_options):
    if file_type == 'csv':
        return context.resources.spark.read.csv(
            file_options['path'], sep=file_options.get('sep')
        ).rdd
    else:
        check.failed('Unsupported file type: {}'.format(file_type))


@output_selector_schema(
    Selector(
        {
            'csv': Field(
                Dict(
                    {
                        'path': Field(Path),
                        'sep': Field(String, is_optional=True),
                        'header': Field(Bool, is_optional=True),
                    }
                )
            )
        }
    )
)
def write_rdd(context, file_type, file_options, spark_rdd):
    if file_type == 'csv':
        df = context.resources.spark.createDataFrame(spark_rdd)
        context.log.info('DF: {}'.format(df))
        df.write.csv(
            file_options['path'], header=file_options.get('header'), sep=file_options.get('sep')
        )
    else:
        check.failed('Unsupported file type: {}'.format(file_type))


class SparkRDDS3StoragePlugin(TypeStoragePlugin):  # pylint: disable=no-init
    @classmethod
    def set_object(cls, object_store, obj, _context, _runtime_type, paths):
        target_path = object_store.key_for_paths(paths)
        obj.write.csv(object_store.url_for_paths(paths, protocol='s3a://'))
        return target_path

    @classmethod
    def get_object(cls, object_store, context, _runtime_type, paths):
        return context.resources.spark.read.parquet(
            object_store.url_for_paths(paths, protocol='s3a://')
        )


class SparkRDDFilesystemStoragePlugin(TypeStoragePlugin):  # pylint: disable=no-init
    @classmethod
    def set_object(cls, object_store, obj, context, _runtime_type, paths):
        target_path = get_valid_target_path(object_store.root, paths)
        # obj.saveAsTextFile()
        # obj.write.csv(object_store.url_for_paths(paths))
        df = context.resources.spark.createDataFrame(obj)
        df.write.csv(target_path)
        return target_path

    @classmethod
    def get_object(cls, object_store, context, _runtime_type, paths):
        return context.resources.spark.read.csv(get_valid_target_path(object_store.root, paths)).rdd


SparkRDD = as_dagster_type(
    RDD,
    'SparkRDD',
    input_schema=load_rdd,
    output_schema=write_rdd,
    storage_plugins={
        RunStorageMode.S3: SparkRDDS3StoragePlugin,
        RunStorageMode.FILESYSTEM: SparkRDDFilesystemStoragePlugin,
    },
)


@resource(config_field=Field(Dict({'spark_conf': spark_config()})))
def spark_session_resource(init_context):
    builder = SparkSession.builder
    flat = flatten_dict(init_context.resource_config['spark_conf'])
    for key, value in flat:
        builder = builder.config(key, value)

    spark = builder.getOrCreate()
    try:
        yield spark
    finally:
        spark.stop()

