Write SQL
=========

.. autofunction:: hiveadb.io.write_sql
   :noindex:

Write Single File Example
^^^^^^^^^^^^^^^^^^^^^^^^^
Write SQL supports reading a single file into a DataFrame by just passing a single string parameter, by default using Koalas as an engine and will return a Koalas DataFrame. Parameters as_type and engine can be used to specify the use of others engines and return types.

..  code-block:: python

    df = write_sql('titanic.sql', '/mnt/raw-zone/')

Write Multiple File Example
^^^^^^^^^^^^^^^^^^^^^^^^^^^
Write SQL can also support reading multiple DataFrame at the same time. This is achived using threading and is recommended over a simple loop using single calls as multiple read operations can be performed at a time. It is recommended to keep the threads parameter between 2-8, but further tuning can be used to improve performance.

..  code-block:: python

    df = write_sql([df_1, df_2], ['dbo.clientes', 'dbo.usuarios'], database='prodmercadeodb', server = "prodmercadeo", port = "1433", user = "Admin", password = "1234", engine="spark", threads = 6)

.. Note::
    The parameter cert is set by default to ".database.windows.net" to make reading from Azure SQL easier. Change it if the read is from another source.