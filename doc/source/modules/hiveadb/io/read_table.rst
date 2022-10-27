Read Table
==========

.. role:: method
.. role:: param

hivecode.hiveadb.io. :method:`read_table` (:param:`table_name: str, db: str, as_type: str, threads: int`)

    Reads a table from the hive metastore into a DataFrame. By default, it will return a Spark dataframe.
    You can specify the number of threads to use to make the reading faster if you are trying to read multiple
    tables at a time.

Example
^^^^^^^
..  code-block:: python

    df = read_table('clients')

Trying to read multiple tables at a time is better since it uses a ThreadPool from multiprocessing to read
everything at a time. Try to tune the threads parameter, by default it's set to 2. 

Example
^^^^^^^
..  code-block:: python

    dfs = read_table(["clients", "accounts", "balance", "documents"], "client_stuff_db", "pandas", 4)

In this case, the function will read all 4 tables, all from the database "client_stuff_db". It will return
a list of 4 pandas dataframes and all 4 read operations will be done at the same time.
