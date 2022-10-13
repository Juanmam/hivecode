Read ORC
========

.. role:: method
.. role:: param

hivecode.hiveadb.functions. :method:`read_orc` (:param:`file_name: str, path: str, condition: str, as_pandas: bool`)

    Reads an ORC file from dbfs into a Spark DataFrame. Can be configured to return a Pandas DataFrame.

Example
^^^^^^^
..  code-block:: python

    df = read_orc('accounts.orc', '/mnt/raw-zone/', as_pandas=True)

Example
^^^^^^^
..  code-block:: python

    df = read_orc('clients.orc', '/mnt/silver-zone/', 'gender=M', as_pandas=True)