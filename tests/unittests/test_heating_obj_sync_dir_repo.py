import pytest

from boiler.heating_obj.io.sync.sync_heating_obj_pickle_reader import SyncHeatingObjPickleReader
from boiler.heating_obj.io.sync.sync_heating_obj_pickle_writer import SyncHeatingObjPickleWriter
from boiler.heating_obj.repository.sync.sync_heating_obj_dir_repository import SyncHeatingObjDirRepository
from unittests.heating_obj_sync_repo_testing import HeatingObjSyncRepoTesting


class TestHeatingObjectSyncDirRepo(HeatingObjSyncRepoTesting):

    filename_ext = ".pickle"

    @pytest.fixture
    def reader(self):
        return SyncHeatingObjPickleReader()

    @pytest.fixture
    def writer(self):
        return SyncHeatingObjPickleWriter()

    @pytest.fixture
    def repository(self, reader, writer, tmpdir, filter_algorithm):
        return SyncHeatingObjDirRepository(
            dir_path=tmpdir,
            filename_ext=self.filename_ext,
            reader=reader,
            writer=writer,
            filter_algorithm=filter_algorithm
        )