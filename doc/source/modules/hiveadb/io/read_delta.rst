Read Delta
==========

.. role:: method
.. role:: param

hivecode.hiveadb.io. :method:`read_delta` (:param:`file_name: str, path: str, source: str, as_type: bool`)

    Reads a Delta file from dbfs into a Koalas DataFrame. Can be configured to return a Pandas DataFrame.

Example
^^^^^^^
..  code-block:: python

    df = read_delta('titanic.csv', '/mnt/raw-zone/')

Example
^^^^^^^
..  code-block:: python

    df = read_delta('diabetes.csv', '/mnt/silver-zone/', as_type="spark")