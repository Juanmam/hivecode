Read JDBC
=========

.. role:: method
.. role:: param

hivecode.hiveadb.functions. :method:`read_jdbc` (:param:`server_url: str, schema: str, table_name: str, user: str, password: str, sql_type: str, as_pandas: bool`)

    Reads a Table from a JDBC DB into a Spark DataFrame. Can be configured to return a Pandas DataFrame.

Example
^^^^^^^
..  code-block:: python

    URL = "//localhost:3306"
    schema = "default"
    table_name = "accounts"
    user = "admin"
    password = "1234"
    sql_type = "mysql"
    df = read_jdbc(URL, schema, table_name, user, password, sql_type=sql_type)

Example
^^^^^^^
..  code-block:: python

    URL = "//localhost:3306"
    schema = "default"
    table_name = "clients"
    user = "admin"
    password = "1234"
    sql_type = "postgresql"
    df = read_jdbc(URL, schema, table_name, user, password, sql_type=sql_type, as_pandas=True)
