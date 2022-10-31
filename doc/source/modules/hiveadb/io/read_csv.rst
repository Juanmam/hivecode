Read CSV
========

.. role:: method
.. role:: param

hivecode.hiveadb.io. :method:`read_csv` (:param:`file_name: str, path: str, source: str, as_type: bool, engine: str, threads: int`)

    Reads a CSV file from dbfs into a Koalas DataFrame. Can be configured to return a Pandas DataFrame.

Read Single File Example
^^^^^^^^^^^^^^^^^^^^^^^^
Read CSV supports reading a single file into a DataFrame by just passing a single string parameter, by default using Koalas as an engine and will return a Koalas DataFrame. Parameters as_type and engine can be used to specify the use of others engines and return types.

..  code-block:: python

    df = read_csv('titanic.csv', '/mnt/raw-zone/')

Read Multiple File Example
^^^^^^^^^^^^^^^^^^^^^^^^^^
Read CSV can also support reading multiple DataFrame at the same time. This is achived using threading and is recommended over a simple loop using single calls as multiple read operations can be performed at a time. It is recommended to keep the threads parameter between 2-8, but further tuning can be used to improve performance.

..  code-block:: python

    df = read_csv(['titanic.csv', 'diabetes.csv'], '/mnt/silver-zone/', as_type="pandas", engine = "spark", threads = 4)

.. Note::
    The read operation will be done by Spark and will return the DataFrame as Pandas.