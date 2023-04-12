Write Parquet
=============

.. autofunction:: hiveadb.io.write_parquet
   :noindex:

Write Single File Example
^^^^^^^^^^^^^^^^^^^^^^^^^
Write Parquet supports reading a single file into a DataFrame by just passing a single string parameter, by default using Koalas as an engine and will return a Koalas DataFrame. Parameters as_type and engine can be used to specify the use of others engines and return types.

..  code-block:: python

    df = write_parquet('titanic.parquet', '/mnt/raw-zone/')

Write Multiple File Example
^^^^^^^^^^^^^^^^^^^^^^^^^^^
Write Parquet can also support reading multiple DataFrame at the same time. This is achived using threading and is recommended over a simple loop using single calls as multiple read operations can be performed at a time. It is recommended to keep the threads parameter between 2-8, but further tuning can be used to improve performance.

..  code-block:: python

    df = write_parquet([df_1, df_2], ['titanic.parquet', 'diabetes.parquet'], '/mnt/silver-zone/', threads = 6)