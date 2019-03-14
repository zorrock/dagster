from collections import namedtuple, OrderedDict
from contextlib import contextmanager
import json
import os
import shutil

from dagster import check
from dagster.utils import mkdir_p, list_pull

from dagster import seven


def base_run_directory():
    return os.path.join(seven.get_system_temp_directory(), 'dagster', 'runs')


def run_directory(run_id):
    check.str_param(run_id, 'run_id')
    return os.path.join(base_run_directory(), run_id)


def meta_file(base_dir):
    return os.path.join(base_dir, 'runmeta.jsonl')


class DagsterRunMeta(namedtuple('_DagsterRunMeta', 'run_id timestamp pipeline_name')):
    def __new__(cls, run_id, timestamp, pipeline_name):
        return super(DagsterRunMeta, cls).__new__(
            cls,
            check.str_param(run_id, 'run_id'),
            check.float_param(timestamp, 'timestamp'),
            check.str_param(pipeline_name, 'pipeline_name'),
        )


class RunStorage:
    def get_run_metas(self):
        check.failed('must implement')

    def get_run_ids(self):
        return list_pull(self.get_run_metas(), 'run_id')


from .files import LocalTempFileStore


class FileStorageBasedRunStorage(RunStorage):
    @staticmethod
    def default():
        return FileStorageBasedRunStorage(base_run_directory())

    def __init__(self, base_dir):
        self._base_dir = check.str_param(base_dir, 'base_dir')
        mkdir_p(self._base_dir)
        self._meta_file = meta_file(self._base_dir)
        self._file_store = LocalTempFileStore(self._base_dir)

    @contextmanager
    def writeable_run_file(self, run_id, *path_comps):
        check.str_param(run_id, 'run_id')

        with self.writeable_run_file(*([run_id] + list(path_comps))) as ff:
            yield ff

    def write_dagster_run_meta(self, dagster_run_meta):
        check.inst_param(dagster_run_meta, 'dagster_run_meta', DagsterRunMeta)
        with self._file_store.writeable_binary_stream('runmeta.jsonl') as ff:
            ff.write((json.dumps(dagster_run_meta._asdict()) + '\n').encode('utf-8'))

    def get_run_ids(self):
        return list_pull(self.get_run_metas(), 'run_id')

    def get_run_meta(self, run_id):
        check.str_param(run_id, 'run_id')
        for run_meta in self.get_run_metas():
            if run_meta.run_id == run_id:
                return run_meta

        return None

    def get_run_metas(self):
        if not os.path.exists(self._meta_file):
            return []

        run_metas = []
        with self._file_store.readable_binary_stream('runmeta.jsonl') as ff:
            line = ff.readline()
            while line:
                raw_run_meta = json.loads(line.decode('utf-8'))
                run_metas.append(
                    DagsterRunMeta(
                        run_id=raw_run_meta['run_id'],
                        timestamp=raw_run_meta['timestamp'],
                        pipeline_name=raw_run_meta['pipeline_name'],
                    )
                )
                line = ff.readline()

        return run_metas

    def nuke(self):
        shutil.rmtree(self._base_dir)


class InMemoryRunStorage(RunStorage):
    def __init__(self):
        self._run_metas = OrderedDict()

    def write_dagster_run_meta(self, dagster_run_meta):
        check.inst_param(dagster_run_meta, 'dagster_run_meta', DagsterRunMeta)
        self._run_metas[dagster_run_meta.run_id] = dagster_run_meta

    def get_run_metas(self):
        return list(self._run_metas.values())

    def get_run_meta(self, run_id):
        check.str_param(run_id, 'run_id')
        return self._run_metas[run_id]

    def nuke(self):
        self._run_metas = OrderedDict()
