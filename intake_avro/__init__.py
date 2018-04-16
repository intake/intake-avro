from intake.source import base
from ._version import get_versions
__version__ = get_versions()['version']
del get_versions


class TablePlugin(base.Plugin):
    """Fast avro to dataframe reader"""

    def __init__(self):
        super(TablePlugin, self).__init__(
            name='avro_table', version=__version__, container='dataframe',
            partition_access=True)

    def open(self, urlpath, **kwargs):
        """Create new AvroTableSource"""
        from intake_avro.source import AvroTableSource
        storage_options = kwargs.pop('storage_options', None)
        base_kwargs, source_kwargs = self.separate_base_kwargs(kwargs)
        return AvroTableSource(urlpath=urlpath,
                               metadata=base_kwargs['metadata'],
                               storage_options=storage_options)


class SequencePlugin(base.Plugin):
    """Avro to sequence of python dicts reader"""
    def __init__(self):
        super(SequencePlugin, self).__init__(
            name='avro_sequence', version=__version__, container='python',
            partition_access=True)

    def open(self, urlpath, **kwargs):
        """Create new AvroSequenceSource"""
        from intake_avro.source import AvroSequenceSource
        storage_options = kwargs.pop('storage_options', None)
        base_kwargs, source_kwargs = self.separate_base_kwargs(kwargs)
        return AvroSequenceSource(urlpath=urlpath,
                                  metadata=base_kwargs['metadata'],
                                  storage_options=storage_options)

