Read Text
=========

.. role:: method
.. role:: param

hivecode.hiveadb.functions. :method:`read_text` (:param:`file_name: str, path: str, line_separator: str, whole_text: bool, as_pandas: bool`)

    Reads a text file from dbfs into a Spark DataFrame. Can be configured to return a Pandas DataFrame.

Example
^^^^^^^
..  code-block:: python

    df = read_text('file.txt', '/mnt/raw-zone/', whole_text=True)

Example
^^^^^^^
..  code-block:: python

    df = read_text('data.txt', '/mnt/silver-zone/', line_separator=',', as_pandas=True)