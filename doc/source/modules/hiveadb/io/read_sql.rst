Read JDBC
=========

.. role:: method
.. role:: param

hivecode.hiveadb.io. :method:`read_sql` (:param:`table_name: str, db: str, sql_type: str, as_type: str`)

    Reads a Table from a JDBC DB into a Koalas DataFrame. Can be configured to return a Pandas DataFrame.

.. note::
    
    The parameter 'table_name' can also be a sql statement.

Example
^^^^^^^
..  code-block:: python

    schema = "//localhost:3305/default"
    table_name = "users"
    df = read_sql(table_name, schema)

Example
^^^^^^^
..  code-block:: python

    schema = "//localhost:3306/specific_db"
    table_name = "accounts"
    sql_type = "mysql"
    df = read_sql(table_name, schema, sql_type)

Example
^^^^^^^
..  code-block:: python

    schema = "//localhost:3307/default"
    table_name = "SELECT * FROM clients"
    sql_type = "postgresql"
    df = read_sql(table_name, schema, sql_type)
