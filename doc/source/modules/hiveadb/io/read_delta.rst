Read Delta
==========

.. role:: method
.. role:: param

hivecode.hiveadb.io. :method:`read_delta` (:param:`file_name: str, path: str, source: str, as_type: bool, engine: str, threads: int`)

    Reads a Delta file from dbfs into a Koalas DataFrame. Can be configured to return a Pandas DataFrame.

Read Single File Example
^^^^^^^^^^^^^^^^^^^^^^^^
Read Delta supports reading a single file into a DataFrame by just passing a single string parameter, by default using Koalas as an engine and will return a Koalas DataFrame. Parameters as_type and engine can be used to specify the use of others engines and return types.

..  code-block:: python

    df = read_delta('titanic.csv', '/mnt/raw-zone/')

Read Multiple File Example
^^^^^^^^^^^^^^^^^^^^^^^^^^
Read Delta can also support reading multiple DataFrame at the same time. This is achived using threading and is recommended over a simple loop using single calls as multiple read operations can be performed at a time. It is recommended to keep the threads parameter between 2-8, but further tuning can be used to improve performance.

..  code-block:: python

    df = read_delta(['titanic.csv', 'diabetes.csv'], '/mnt/silver-zone/', as_type="spark", engine="koalas")

.. Note::
    The read operation will be done by Koalas and will return the DataFrame as Spark.