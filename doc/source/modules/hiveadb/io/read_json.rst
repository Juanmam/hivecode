Read JSON
=========

.. role:: method
.. role:: param

hivecode.hiveadb.io. :method:`read_json` (:param:`file_name: str, path: str, source: str, as_type: bool`)

    Reads a JSON file from dbfs into a Koalas DataFrame. Can be configured to return a Pandas DataFrame.

Example
^^^^^^^
..  code-block:: python

    df = read_json('config.json', '/mnt/raw-zone/')

Example
^^^^^^^
..  code-block:: python

    df = read_json('data.json', '/mnt/silver-zone/', as_type=pandas)