Read CSV
========

.. role:: method
.. role:: param

hivecode.hiveadb.io. :method:`read_csv` (:param:`file_name: str, path: str, source: str, as_type: bool`)

    Reads a CSV file from dbfs into a Koalas DataFrame. Can be configured to return a Pandas DataFrame.

Example
^^^^^^^
..  code-block:: python

    df = read_csv('titanic.csv', '/mnt/raw-zone/')

Example
^^^^^^^
..  code-block:: python

    df = read_csv('diabetes.csv', '/mnt/silver-zone/', as_type="spark")