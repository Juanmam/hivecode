Read CSV
========

.. role:: method
.. role:: param

hivecode.hiveadb.functions. :method:`read_csv` (:param:`file_name: str, file_path: str, delimiter: str, infer_schema: bool, include_header: bool, as_pandas: bool`)

    Reads a CSV file from dbfs into a Spark DataFrame. Can be configured to return a Pandas DataFrame.

Example
^^^^^^^
..  code-block:: python

    df = read_csv('titanic.csv', '/mnt/raw-zone/', delimiter='|')

Example
^^^^^^^
..  code-block:: python

    df = read_csv('diabetes.csv', '/mnt/silver-zone/', infer_schema=True, as_pandas=True)