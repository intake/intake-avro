Quickstart
==========

``intake_avro`` provides quick and easy access to tabular data stored in
the Apache `Avro`_ binary, columnar format.

.. _Avro: https://avro.apache.org/docs/current/

Installation
------------

To use this plugin for `intake`_, install with the following command::

   conda install -c conda-forge intake-avro

.. _intake: https://github.com/ContinuumIO/intake

Usage
-----

Ad-hoc
~~~~~~

After installation, the functions ``intake.open_avro_table``
and ``intake.open_avro_sequence`` will become available. The former, much faster
method can be used to open one or more Avro files with `flat` schema into dataframes, but
the latter can be used for any files and produces generic sequences of dictionaries.

Assuming some Avro files in a given path, the following would load them into a
dataframe::

   import intake
   source = intake.open_avro_table('data_path/*.avro')
   dataframe = source.read()

There will, by default, be partitions within each file of about 100MB in size. To skip
scanning files for the purpose of partitioning, you can pass `blocksize=None`.

Arguments to the ``open_avro_*`` functions:

- ``urlpath`` : the location of the data. This can be a single file, a list of specific files,
 or a glob string (containing ``"*"``). The
 URLs can be local files or, if using a protocol specifier such as ``'s3://'``, a remote file
 location.

- `blocksize`: defines the partitioning within input files. The special value `None` avoids
 partitioning within files - you get exactly one partition per input file. This avoids some
 upfront overhead to scan for block markers within files, so may be desirable in some cases.
 The default value of about 100MB, so for small files, there will be no difference.

- ``storage_options`` : other parameters that are to be passed to the filesystem
 implementation, in the case that a remote filesystem is referenced in ``urlpath``. For
 specifics, see the Dask `documentation`_.

.. _documentation : http://dask.pydata.org/en/latest/remote-data-services.html

A source so defined will provide the usual methods such as ``discover`` and ``read_partition``.

Creating Catalog Entries
~~~~~~~~~~~~~~~~~~~~~~~~

To use for a data-source within a catalog, a spec may look something like

sources:
  test:
    description: Sample description of some avro dataset
    driver: avro_table
    args:
      urlpath: '{{ CATALOG_DIR }}/data.*.avro'

and entries must specify ``driver: avro_table`` or ``driver: avro_sequence``.
The further arguments are exactly the same
as for the ``open_avro_*`` functions.

Using a Catalog
~~~~~~~~~~~~~~~

Assuming a catalog file called ``cat.yaml``, containing a Avro source ``pdata``, one could
load it into a dataframe as follows::

   import intake
   cat = intake.Catalog('cat.yaml')
   df = cat.pdata.read()

The type of the output will depend on the plugin that was defined in the catalog. You can
inspect this before loading by looking at the ``.container`` attribute, which will be
either ``"dataframe"`` or ``"python"``.

The number of partitions will be at least one for the number of files pointed to.
