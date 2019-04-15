import os
import subprocess

import pytest
import yaml

from dagster.utils import script_relative_path


@pytest.fixture(scope='session')
def spark_home():
    prev_spark_home = os.getenv('SPARK_HOME')

    if not prev_spark_home:
        try:
            pyspark_show = subprocess.check_output(['pip', 'show', 'pyspark'])
        except subprocess.CalledProcessError:
            pass
        else:
            os.environ['SPARK_HOME'] = os.path.join(
                list(filter(lambda x: 'Location' in x, pyspark_show.decode('utf-8').split('\n')))[
                    0
                ].split(' ')[1],
                'pyspark',
            )

    yield os.getenv('SPARK_HOME')

    if not prev_spark_home:
        try:
            del os.environ['SPARK_HOME']
        except KeyError:
            pass


@pytest.fixture(scope='session')
def secrets_yml():
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
    secrets_path = script_relative_path('../environments/secrets.yml')
    with open(secrets_path, 'w') as file_obj:
        yaml.dump(secrets, file_obj)

    yield

    os.unlink(secrets_path)
