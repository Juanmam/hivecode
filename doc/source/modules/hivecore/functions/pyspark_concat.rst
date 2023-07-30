pyspark_concat
==============

.. autofunction:: hivecore.functions.pyspark_concat
   :noindex:


Example
-------

To use the `pyspark_concat` function, simply call it as follows:

.. code-block:: python

    from hivecore.functions import pyspark_concat
    from pyspark.sql import SparkSession

    spark = SparkSession.builder.getOrCreate()

    # Create DataFrame df1
    data1 = [(0, 1), (2, 3)]
    df1 = spark.createDataFrame(data1, ["A", "B"])

    # Create DataFrame df2
    data2 = [(4, 5), (6, 7)]
    df2 = spark.createDataFrame(data2, ["C", "D"])

    df3 = pyspark_concat(df1, df2, axis=1)
    df3.show()

Expected Output
---------------

The output will be an horizontal concat. For vertical concat, use axis=0.

.. code-block:: text

    +---+---+---+---+
    |  A|  B|  C|  D|
    +---+---+---+---+
    |  0|  1|  4|  5|
    |  2|  3|  6|  7|
    +---+---+---+---+