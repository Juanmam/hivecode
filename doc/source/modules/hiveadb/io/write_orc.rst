Write ORC
=============

.. role:: method
.. role:: param

hivecode.hiveadb.io. :method:`write_orc` (:param:`df, file_name: str, path: str, source: str, mode: str, num_files: int, threads: int`)

    Writes a ORC file from a DataFrame into the file system.

Write Single File Example
^^^^^^^^^^^^^^^^^^^^^^^^^
Write ORC supports reading a single file into a DataFrame by just passing a single string parameter, by default using Koalas as an engine and will return a Koalas DataFrame. Parameters as_type and engine can be used to specify the use of others engines and return types.

..  code-block:: python

    df = write_orc('titanic.orc', '/mnt/raw-zone/')

Write Multiple File Example
^^^^^^^^^^^^^^^^^^^^^^^^^^^
Write ORC can also support reading multiple DataFrame at the same time. This is achived using threading and is recommended over a simple loop using single calls as multiple read operations can be performed at a time. It is recommended to keep the threads parameter between 2-8, but further tuning can be used to improve performance.

..  code-block:: python

    df = write_orc([df_1, df_2], ['titanic.orc', 'diabetes.orc'], '/mnt/silver-zone/', threads = 6)