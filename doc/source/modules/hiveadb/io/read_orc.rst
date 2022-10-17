Read ORC
========

.. role:: method
.. role:: param

hivecode.hiveadb.io. :method:`read_orc` (:param:`file_name: str, path: str, source: str, as_type: bool`)

    Reads an ORC file from dbfs into a Koalas DataFrame. Can be configured to return a Pandas DataFrame.

Example
^^^^^^^
..  code-block:: python

    df = read_orc('accounts.orc', '/mnt/raw-zone/', as_pandas=True)

Example
^^^^^^^
..  code-block:: python

    df = read_orc('clients.orc', '/mnt/silver-zone/', 'gender=M', as_pandas=True)