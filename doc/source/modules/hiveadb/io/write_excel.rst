Write Excel
===========

.. role:: method
.. role:: param

hivecode.hiveadb.io. :method:`write_excel` (:param:`df, file_name: str, path: str, source: str, extension: str, sheet_name: List[str], threads: int`)

    Writes a Excel file from a DataFrame into the file system.

Write Single File Example
^^^^^^^^^^^^^^^^^^^^^^^^^
Write Excel supports reading a single file into a DataFrame by just passing a single string parameter, by default using Koalas as an engine and will return a Koalas DataFrame. Parameters as_type and engine can be used to specify the use of others engines and return types.

..  code-block:: python

    df = write_excel('titanic.xlsx', '/mnt/raw-zone/')

Write Multiple File Example
^^^^^^^^^^^^^^^^^^^^^^^^^^^
Write Excel can also support reading multiple DataFrame at the same time. This is achived using threading and is recommended over a simple loop using single calls as multiple read operations can be performed at a time. It is recommended to keep the threads parameter between 2-8, but further tuning can be used to improve performance.

..  code-block:: python

    df = write_excel([df_1, df_2], ['titanic.xlsx', 'diabetes.xlsx'], '/mnt/silver-zone/', threads = 6)