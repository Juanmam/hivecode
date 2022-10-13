Read JSON
=========

.. role:: method
.. role:: param

hivecode.hiveadb.functions. :method:`read_json` (:param:`file_name: str, file_path: str, is_multiline: bool, as_pandas: bool`)

    Reads a JSON file from dbfs into a Spark DataFrame. Can be configured to return a Pandas DataFrame.

Example
^^^^^^^
..  code-block:: python

    df = read_json('config.json', '/mnt/raw-zone/', is_multiline=True)

Example
^^^^^^^
..  code-block:: python

    df = read_json('data.json', '/mnt/silver-zone/', as_pandas=True)