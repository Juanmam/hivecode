Write Table
===========

.. role:: method
.. role:: param


hivecode.hiveadb.io. :method:`write_table` (:param:`df: Union[DataFrame, List[DataFrame]], table_name: Union[str, List[str]], db: str, delta_path: str,mode: str, partitions: Union[str, List[str], None], index: Union[str, List[str], None], threads: int`)

    Writes a table into the hive metastore from a DataFrame. You can specify the number of threads to use
    to make the writing faster if you are trying to write multiple tables at a time.

Example
^^^^^^^
..  code-block:: python

    df = write_table(df_cli, 'clients')

Trying to write multiple tables at a time is better since it uses Thread from threading to write
everything at a time. Try to tune the threads parameter, by default it's set to 2. 

Example
^^^^^^^
..  code-block:: python

    dfs = write_table([df_cli, df_acc, df_bal, df_docs], ["clients", "accounts", "balance", "documents"],\
    "client_stuff_db", "/mnt/silver-zone/", 4)

In this case, the function will write all 4 tables, all into the database "client_stuff_db". It will performe
all 4 write operations at the same time.
