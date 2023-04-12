Data Convert
============

.. autofunction:: hiveadb.function.data_convert
   :noindex:

Parameters
^^^^^^^^^^
* as_type: Selects the type of DataFrame to return.
    * Pandas: "pandas"
    * Spark: "spark"
    * Koalas: "koalas"
    * Pyspark.Pandas: "pyspark.pandas", "spark.pandas", "ps"

Example
^^^^^^^
..  code-block:: python
    
    from pandas import DataFrame

    pandas_df = DataFrame(...)

    koalas_df = data_convert(pandas_df, as_type="koalas")
    spark_df = data_convert(pandas_df, as_type="spark")
    pandas_df_copy = data_convert(spark_df, as_type="pandas")
    pandas_on_spark_df = data_convert(spark_df, as_type="pyspark.pandas")