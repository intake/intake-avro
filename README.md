# Intake-avro

[![Build Status](https://travis-ci.org/ContinuumIO/intake-avro.svg?branch=master)](https://travis-ci.org/ContinuumIO/intake-avro)
[![Documentation Status](https://readthedocs.org/projects/intake-avro/badge/?version=latest)](http://intake-avro.readthedocs.io/en/latest/?badge=latest)

The [Apache Avro](https://avro.apache.org/) format is a popular method for information
exchange, particularly for message and reord distribution in high-throughput systems.

In [Intake](https://github.com/ContinuumIO/intake/), there are two plugins provided for reading avro data:

- `avro_table` is appropriate for data which have a schema simple enough to be
  represented as columns with simple types, i.e., *flat* schema. The output of
  this plugin are dataframes, and the reading is optimized for speed.
- `avro_sequence` is more generic and can handle all possible avro schema, but
  it produces generic python sequences of dictionaries, and is consequently
  much slower than the table reader.

### Installation

The conda install instructions are:

```
conda install -c conda-forge fastavro uavro
conda install -c intake intake_avro
```

### Examples

See the notebook in the examples/ directory.
