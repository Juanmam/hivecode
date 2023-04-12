Read SQL
========

.. autofunction:: hiveadb.io.read_sql
   :noindex:

.. note::
    
    The parameter 'table_name' can also be a sql statement.

Read Single File Example
^^^^^^^^^^^^^^^^^^^^^^^^
Read SQL supports reading a single file into a DataFrame by just passing a single string parameter, by default using Koalas as an engine and will return a Koalas DataFrame. Parameters as_type and engine can be used to specify the use of others engines and return types.

..  code-block:: python

    schema = "//localhost:3305/default"
    table_name = "users"
    df = read_sql(table_name, schema)

Read Multiple File Example
^^^^^^^^^^^^^^^^^^^^^^^^^^
Read SQL can also support reading multiple DataFrame at the same time. This is achived using threading and is recommended over a simple loop using single calls as multiple read operations can be performed at a time. It is recommended to keep the threads parameter between 2-8, but further tuning can be used to improve performance.

..  code-block:: python

    schema = "//localhost:3306/specific_db"
    table_name = "accounts"
    sql_type = "mysql"
    df = read_sql(table_name, schema, sql_type)

Read Multiple File Example
^^^^^^^^^^^^^^^^^^^^^^^^^^
Read SQL can also support reading multiple DataFrame at the same time. This is achived using threading and is recommended over a simple loop using single calls as multiple read operations can be performed at a time. It is recommended to keep the threads parameter between 2-8, but further tuning can be used to improve performance.

..  code-block:: python

    schema = "//localhost:3307/default"
    table_name = ["accounts", "SELECT * FROM clients"]
    sql_type = "postgresql"
    df = read_sql(table_name, schema, sql_type)
