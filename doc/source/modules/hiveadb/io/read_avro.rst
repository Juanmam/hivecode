Read Avro
=========

.. autofunction:: hiveadb.io.read_avro
   :noindex:

Read Single File Example
^^^^^^^^^^^^^^^^^^^^^^^^
Read Avro supports reading a single file into a DataFrame by just passing a single string parameter, by default using Koalas as an engine and will return a Koalas DataFrame. Parameters as_type and engine can be used to specify the use of others engines and return types.

..  code-block:: python

    df = read_avro('titanic.avro', '/mnt/raw-zone/')

Read Multiple File Example
^^^^^^^^^^^^^^^^^^^^^^^^^^
Read Avro can also support reading multiple DataFrame at the same time. This is achived using threading and is recommended over a simple loop using single calls as multiple read operations can be performed at a time. It is recommended to keep the threads parameter between 2-8, but further tuning can be used to improve performance.

..  code-block:: python

    df = read_avro(['titanic.avro', 'diabetes.avro'], '/mnt/silver-zone/', as_type="pandas", engine = "spark", threads = 4)

.. Note::
    The read operation will be done by Spark and will return the DataFrame as Pandas.