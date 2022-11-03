Write Delta
===========

.. role:: method
.. role:: param

hivecode.hiveadb.io. :method:`write_delta` (:param:`df, file_name: str, path: str, source: str, mode: str, threads: int`)

    Writes a Delta file from a DataFrame into the file system.

Write Single File Example
^^^^^^^^^^^^^^^^^^^^^^^^^
Write Delta supports reading a single file into a DataFrame by just passing a single string parameter, by default using Koalas as an engine and will return a Koalas DataFrame. Parameters as_type and engine can be used to specify the use of others engines and return types.

..  code-block:: python

    df = write_delta('titanic.delta', '/mnt/raw-zone/')

Write Multiple File Example
^^^^^^^^^^^^^^^^^^^^^^^^^^^
Write Delta can also support reading multiple DataFrame at the same time. This is achived using threading and is recommended over a simple loop using single calls as multiple read operations can be performed at a time. It is recommended to keep the threads parameter between 2-8, but further tuning can be used to improve performance.

..  code-block:: python

    df = write_delta([df_1, df_2], ['titanic.delta', 'diabetes.delta'], '/mnt/silver-zone/', threads = 6)