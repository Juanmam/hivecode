Read Table
==========

.. role:: method
.. role:: param

hivecode.hiveadb.io. :method:`read_table` (:param:`table_name: str, db: str, as_type: str, index: Union[str, List[str], None], engine: str, threads: int`)

    Reads a table from the hive metastore into a DataFrame. By default, it will return a Spark dataframe.
    You can specify the number of threads to use to make the reading faster if you are trying to read multiple
    tables at a time.

Read Single File Example
^^^^^^^^^^^^^^^^^^^^^^^^
Read Table supports reading a single table into a DataFrame by just passing a single string parameter, by default using Koalas as an engine and will return a Koalas DataFrame. Parameters as_type and engine can be used to specify the use of others engines and return types.

..  code-block:: python

    df = read_table('clients')

Trying to read multiple tables at a time is better since it uses a ThreadPool from multiprocessing to read
everything at a time. Try to tune the threads parameter, by default it's set to 2. 

Read Multiple File Example
^^^^^^^^^^^^^^^^^^^^^^^^^^
Read Table can also support reading multiple DataFrame at the same time. This is achived using threading and is recommended over a simple loop using single calls as multiple read operations can be performed at a time. It is recommended to keep the threads parameter between 2-8, but further tuning can be used to improve performance.

..  code-block:: python

    dfs = read_table(["clients", "accounts", "balance", "documents"], "client_stuff_db", "pandas", 4)

In this case, the function will read all 4 tables, all from the database "client_stuff_db". It will return
a list of 4 pandas dataframes and all 4 read operations will be done at the same time.
