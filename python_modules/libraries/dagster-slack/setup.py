import argparse
import sys

from setuptools import find_packages, setup

# pylint: disable=E0401, W0611
if sys.version_info[0] < 3:
    import __builtin__ as builtins
else:
    import builtins


def get_version(name):
    version = {}
    with open("dagster_gcp/version.py") as fp:
        exec(fp.read(), version)  # pylint: disable=W0122

    if name == 'dagster-gcp':
        return version['__version__']
    elif name == 'dagster-gcp-nightly':
        return version['__nightly__']
    else:
        raise Exception('Shouldn\'t be here: bad package name {name}'.format(name=name))


parser = argparse.ArgumentParser()
parser.add_argument('--nightly', action='store_true')


def _do_setup(name='dagster-gcp'):
    setup(
        name='dagster_gcp',
        version=get_version(name),
        author='Elementl',
        license='Apache-2.0',
        description='Package for GCP-specific Dagster framework solid and resource components.',
        url='https://github.com/dagster-io/dagster/tree/master/python_modules/libraries/dagster-gcp',
        classifiers=[
            'Programming Language :: Python :: 2.7',
            'Programming Language :: Python :: 3.5',
            'Programming Language :: Python :: 3.6',
            'License :: OSI Approved :: Apache Software License',
            'Operating System :: OS Independent',
        ],
        packages=find_packages(exclude=['test']),
        install_requires=['dagster'],
        zip_safe=False,
    )


if __name__ == '__main__':
    parsed, unparsed = parser.parse_known_args()
    sys.argv = [sys.argv[0]] + unparsed
    if parsed.nightly:
        _do_setup('dagster-gcp-nightly')
    else:
        _do_setup('dagster-gcp')
