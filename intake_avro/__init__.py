import io

from intake.source import base
from dask import delayed
import dask.dataframe as dd
import dask.bag as db
from dask.bytes import open_files
from uavro import core as avrocore

__version__ = '0.0.1'


class TablePlugin(base.Plugin):
    def __init__(self):
        super(TablePlugin, self).__init__(
            name='avro_table', version=__version__, container='dataframe',
            partition_access=True)

    def open(self, urlpath, **kwargs):
        base_kwargs, source_kwargs = self.separate_base_kwargs(kwargs)
        return AvroTableSource(urlpath=urlpath,
                               metadata=base_kwargs['metadata'])


class SequencePlugin(base.Plugin):
    def __init__(self):
        super(TablePlugin, self).__init__(
            name='avro_sequence', version=__version__, container='python',
            partition_access=True)

    def open(self, urlpath, **kwargs):
        base_kwargs, source_kwargs = self.separate_base_kwargs(kwargs)
        return AvroSequenceSource(urlpath=urlpath,
                                  metadata=base_kwargs['metadata'])


class AvroTableSource(base.DataSource):
    """
    Source to load tabular avro datasets.
    """

    def __init__(self, urlpath, metadata=None):
        self._urlpath = urlpath
        self._files = open_files(urlpath, mode='rb')
        self._head = None
        super(AvroTableSource, self).__init__(container='dataframe',
                                              metadata=metadata)

    def _get_schema(self):
        if self._head is None:
            with self._files[0] as f:
                self._head = avrocore.read_header(f)

        dtypes = self._head['dtypes']
        return base.Schema(datashape=None,
                           dtype=dtypes,
                           shape=(None, len(dtypes)),
                           npartitions=len(self._files),
                           extra_metadata={})

    def _get_partition(self, i):
        with self._files[i] as f:
            data = f.read()

        return avrocore.filelike_to_dataframe(io.BytesIO(data),
                                              len(data), self._head, scan=True)

    def to_dask(self):
        self.discover()
        dpart = delayed(self._get_partition)
        return dd.from_delayed([dpart(i) for i in range(self.npartitions)],
                               meta=self.dtype)


class AvroSequenceSource(base.DataSource):
    """
    Source to load tabular avro datasets.
    """

    def __init__(self, urlpath, metadata=None):
        self._urlpath = urlpath
        self._files = open_files(urlpath, mode='rb')
        self._head = None
        super(AvroSequenceSource, self).__init__(container='python',
                                                 metadata=metadata)

    def _get_schema(self):
        if self._head is None:
            with self._files[0] as f:
                self._head = avrocore.read_header(f)

        dtypes = self._head['dtypes']
        return base.Schema(datashape=None,
                           dtype=None,
                           shape=None,
                           npartitions=len(self._files),
                           extra_metadata={})

    def _get_partition(self, i):
        with self._files[i] as f:
            data = f.read()

        import fastavro.reader

        return list(fastavro.reader(io.BytesIO(data)))

    def to_dask(self):
        self.discover()
        dpart = delayed(self._get_partition)
        return db.from_delayed([dpart(i) for i in range(self.npartitions)])
