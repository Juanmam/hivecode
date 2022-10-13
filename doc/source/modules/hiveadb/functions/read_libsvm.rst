Read Libsvm
===========

.. role:: method
.. role:: param

hivecode.hiveadb.functions. :method:`read_libsvm` (:param:`file_name: str, path: str, number_features: str, as_pandas: bool`)

    Reads a Libsvm file from dbfs into a Spark DataFrame. Can be configured to return a Pandas DataFrame.

Example
^^^^^^^
..  code-block:: python

    df = read_libsvm('sample_libsvm_data.txt', '/mnt/raw-zone/', number_features="780")

Example
^^^^^^^
..  code-block:: python

    df = read_libsvm('sample_libsvm_data.txt', '/mnt/silver-zone/', as_pandas=True)