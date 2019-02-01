import copy
from intake.source import base

from . import __version__


class AvroTableSource(base.DataSource):
    """
    Source to load tabular Avro datasets.

    Parameters
    ----------
    urlpath: str
        Location of the data files; can include protocol and glob characters.
    """
    version = __version__
    container = 'dataframe'
    name = 'avro_table'

    def __init__(self, urlpath, blocksize=100000000,
                 metadata=None, storage_options=None):
        self._urlpath = urlpath
        self._storage_options = storage_options or {}
        self._bs = blocksize
        self._df = None
        super(AvroTableSource, self).__init__(metadata=metadata)

    def _get_schema(self):
        if self._df is None:
            from uavro import dask_read_avro
            from uavro.core import read_header
            from dask.bytes import open_files
            self._df = dask_read_avro(self._urlpath, blocksize=self._bs,
                                      storage_options=self._storage_options)

            files = open_files(self._urlpath, **self._storage_options)
            with copy.copy(files[0]) as f:
                # we assume the same header for all files
                self.metadata.update(read_header(f))
            self.npartitions = self._df.npartitions
        dtypes = {k: str(v) for k, v in self._df.dtypes.items()}
        return base.Schema(datashape=None,
                           dtype=dtypes,
                           shape=(None, len(dtypes)),
                           npartitions=self.npartitions,
                           extra_metadata={})

    def _get_partition(self, i):
        self._get_schema()
        return self._df.get_partition(i).compute()

    def read(self):
        self._get_schema()
        return self.compute()

    def to_dask(self):
        """Create lazy dask dataframe object"""
        self._get_schema()
        return self._df

    def to_spark(self):
        """Pass URL to spark to load as a DataFrame

        Note that this requires ``org.apache.spark.sql.avro.AvroFileFormat``
        to be installed in your spark classes.

        This feature is experimental.
        """
        from intake_spark.base import SparkHolder
        sh = SparkHolder(True, [
            ['read'],
            ['format', ["com.databricks.spark.avro"]],
            ['load', [self._urlpath]]
        ], {})
        return sh.setup()


class AvroSequenceSource(base.DataSource):
    """
    Source to load Avro datasets as sequence of Python dicts.

    Parameters
    ----------
    urlpath: str
        Location of the data files; can include protocol and glob characters.
    """
    version = __version__
    container = 'python'
    name = 'avro_sequence'

    def __init__(self, urlpath, blocksize=100000000, metadata=None,
                 storage_options=None):
        self._urlpath = urlpath
        self._bs = blocksize
        self._storage_options = storage_options or {}
        self._bag = None
        super(AvroSequenceSource, self).__init__(metadata=metadata)

    def _get_schema(self):
        if self._bag is None:
            from dask.bag import read_avro
            self._bag = read_avro(self._urlpath, blocksize=self._bs,
                                  storage_options=self._storage_options)
        self.npartitions = self._bag.npartitions

        return base.Schema(datashape=None,
                           dtype=None,
                           shape=None,
                           npartitions=self._bag.npartitions,
                           extra_metadata={})

    def _get_partition(self, i):
        self._get_schema()
        return self._bag.to_delayed()[i].compute()

    def read(self):
        self._get_schema()
        return self._bag.compute()

    def to_dask(self):
        """Create lazy dask bag object"""
        self._get_schema()
        return self._bag
