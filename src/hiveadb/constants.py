PANDAS = "pandas"
KOALAS = "koalas"
SPARK  = "pyspark"
PANDAS_ON_SPARK = "pandas_on_spark"

IN_PANDAS_ON_SPARK = [PANDAS_ON_SPARK, "pyspark.pandas", "ps"]

PANDAS_TYPES = {
    "df": "<class 'pandas.core.frame.DataFrame'>",
    "dataframe": "<class 'pandas.core.frame.DataFrame'>",
    "frame": "<class 'pandas.core.frame.DataFrame'>",
    "series":"<class 'pandas.core.series.Series'>"
}

PD_TYPES = PANDAS_TYPES

SPARK_TYPES = {
    "df": "<class 'pyspark.sql.dataframe.DataFrame'>",
    "dataframe": "<class 'pyspark.sql.dataframe.DataFrame'>",
    "frame": "<class 'pyspark.sql.dataframe.DataFrame'>",
    "series": "<class 'pyspark.sql.column.Column'>"
}

PYSPARK_TYPES = SPARK_TYPES

KOALAS_TYPES = {
    "df": "<class 'databricks.koalas.frame.DataFrame'>",
    "dataframe": "<class 'databricks.koalas.frame.DataFrame'>",
    "frame": "<class 'databricks.koalas.frame.DataFrame'>",
    "series": "<class 'databricks.koalas.series.Series'>"
}

PANDAS_ON_SPARK_TYPES = {
    "df": "<class 'pyspark.pandas.frame.DataFrame'>",
    "dataframe": "<class 'pyspark.pandas.frame.DataFrame'>",
    "frame": "<class 'pyspark.pandas.frame.DataFrame'>",
    "series": "<class 'pyspark.pandas.series.Series'>"
}

PS_TYPES = PANDAS_ON_SPARK_TYPES