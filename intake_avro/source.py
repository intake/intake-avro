from intake.source import base
import io

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

    def __init__(self, urlpath, metadata=None, storage_options=None):
        self._urlpath = urlpath
        self._storage_options = storage_options or {}
        self._head = None
        super(AvroTableSource, self).__init__(metadata=metadata)

    def _get_schema(self):
        from dask.bytes.core import open_files
        import uavro.core as avrocore
        self._files = open_files(self._urlpath, mode='rb',
                                 **self._storage_options)
        if self._head is None:
            with self._files[0] as f:
                self._head = avrocore.read_header(f)

        dtypes = self._head['dtypes']
        # Avro schemas have a "namespace" and a "name" that could be metadata
        return base.Schema(datashape=None,
                           dtype=dtypes,
                           shape=(None, len(dtypes)),
                           npartitions=len(self._files),
                           extra_metadata={})

    def _get_partition(self, i):
        return read_file_uavro(self._files[i], self._head)

    def read(self):
        self._get_schema()
        return self.to_dask().compute()

    def to_dask(self):
        """Create lazy dask dataframe object"""
        import dask.dataframe as dd
        from dask import delayed
        self.discover()
        dpart = delayed(read_file_uavro)
        return dd.from_delayed([dpart(f, self._head) for f in self._files],
                               meta=self.dtype)


def read_file_uavro(f, head):
    import uavro.core as avrocore
    with f as f:
        data = f.read()
        return avrocore.filelike_to_dataframe(io.BytesIO(data), len(data),
                                              head, scan=True)


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

    def __init__(self, urlpath, metadata=None, storage_options=None):
        self._urlpath = urlpath
        self._storage_options = storage_options or {}
        self._head = None
        super(AvroSequenceSource, self).__init__(metadata=metadata)

    def _get_schema(self):
        from dask.bytes.core import open_files
        self._files = open_files(self._urlpath, mode='rb',
                                 **self._storage_options)
        # avro schemas have a "namespace" and a "name" that could be metadata
        return base.Schema(datashape=None,
                           dtype=None,
                           shape=None,
                           npartitions=len(self._files),
                           extra_metadata={})

    def _get_partition(self, i):
        self._get_schema()
        return read_file_fastavro(self._files[i])

    def read(self):
        self._get_schema()
        return self.to_dask().compute()

    def to_dask(self):
        """Create lazy dask bag object"""
        from dask import delayed
        import dask.bag as db
        self._get_schema()
        dpart = delayed(read_file_fastavro)
        return db.from_delayed([dpart(f) for f in self._files])


def read_file_fastavro(f):
    import fastavro
    with f as f:
        return list(fastavro.reader(f))
