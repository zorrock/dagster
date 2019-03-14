from abc import ABCMeta, abstractmethod
from contextlib import contextmanager
import os

import six

from dagster import check
from dagster.utils import mkdir_p


class FileStore(six.with_metaclass(ABCMeta)):  # pylint: disable=no-init
    @abstractmethod
    @contextmanager
    def writeable_binary_stream(self, *path_comps):
        pass

    @abstractmethod
    @contextmanager
    def readable_binary_stream(self, *path_comps):
        pass


def check_path_comps(path_comps):
    path_list = check.list_param(list(path_comps), 'path_comps', of_type=str)
    check.param_invariant(path_list, 'path_list', 'Must have at least one comp')
    return path_list


class LocalTempFileStore(FileStore):
    def __init__(self, root):
        check.str_param(root, 'root')
        self.root = root
        self._created = False

    def ensure_root_exists(self):
        if not self._created:
            mkdir_p(self.root)

        self._created = True

    @contextmanager
    def writeable_binary_stream(self, *path_comps):
        self.ensure_root_exists()

        target_path = self.newmethod97(path_comps)

        with open(target_path, 'wb') as ff:
            yield ff

    def newmethod97(self, path_comps):
        path_list = check_path_comps(path_comps)

        target_dir = os.path.join(self.root, *path_list[:-1])
        mkdir_p(target_dir)

        target_path = os.path.join(target_dir, path_list[-1])
        check.invariant(not os.path.exists(target_path))
        return target_path

    @contextmanager
    def readable_binary_stream(self, *path_comps):
        self.ensure_root_exists()

        path_list = check_path_comps(path_comps)

        target_path = os.path.join(self.root, *path_list)
        with open(target_path, 'rb') as ff:
            yield ff

    def has_file(self, *path_comps):
        self.ensure_root_exists()

        path_list = check_path_comps(path_comps)

        target_path = os.path.join(self.root, *path_list)

        if os.path.exists(target_path):
            check.invariant(os.path.isfile(target_path))
            return True
        else:
            return False
