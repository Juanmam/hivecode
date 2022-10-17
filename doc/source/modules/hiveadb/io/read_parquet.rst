Read Parquet
============

.. role:: method
.. role:: param

hivecode.hiveadb.io. :method:`read_parquet` (:param:`file_name: str, path: str, source: str, as_type: bool`)

    Reads a Parquet file from dbfs into a Koalas DataFrame. Can be configured to return a Pandas DataFrame.

Example
^^^^^^^
..  code-block:: python

    df = read_parquet('dataset.parquet', '/mnt/raw-zone/')

Example
^^^^^^^
..  code-block:: python

    df = read_parquet('clients.parquet', '/mnt/silver-zone/', as_type='pandas')