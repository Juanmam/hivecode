from typing import List
from tqdm import tqdm

# Pyspark
from pyspark.context import SparkContext
from pyspark.sql.session import SparkSession
from pyspark.pandas import from_pandas as ps_from_pandas
from databricks.koalas import from_pandas, DataFrame as KoalasDataFrame

def get_spark() -> SparkSession:
    """
    Fetches the current instance of spark. If none exists, creates a new one.
    Returns:
        SparkSession: The current spark session.
    """
    # Spark
    sc = SparkContext.getOrCreate()
    return SparkSession(sc)


# DBUtils
try:
    # up to DBR 8.2
    from dbutils import DBUtils  # pylint: disable=import-error,wrong-import-position
except:
    # above DBR 8.3
    from dbruntime.dbutils import DBUtils  # pylint: disable=import-error,wrong-import-position


def get_dbutils(spark: SparkSession = None) -> DBUtils:
    """
    Feches the current instance of DBUtils.
    Args:
        spark (SparkSession, optional): The current spark session. Defaults to None.
    Returns:
        DBUtils: _description_
    """
        
    # We try to create dbutils from spark or by using IPython
    try:
        return DBUtils(spark)
    except:
        import IPython
        return IPython.get_ipython().user_ns["dbutils"]


spark   = get_spark()
dbutils = get_dbutils()


def mount(storage:     str,
          key:         str,
          mount_point: str       = '/mnt/',
          mounts:      List[str] = ["raw", "silver", "gold"], 
          verbose:     bool      = False
          ) -> None:
    """
    Mounts a set of zones into the system.
    Args:
        storage (str): The name of the storage to mount. This can be found at keys access in your storage account.
        key (str): The key of the storage to mount. This can be found at keys access in your storage account.
        mount_point (str, optional): The mount point to use. 
            Defaults to '/mnt/'.
        mounts (List[str], optional): A list of all the mounts you want. This doesn't include the prefix. Check example. 
            Defaults to ["raw", "silver", "gold"].
        postfix (str, optional): The postfix is the ending you want to put to your mount zones. Set it to an empty 
        string if you don't want to apply it. 
            Defaults to '-zone'.
        include_tqdm (bool, optional): A flag to include tqdm bars for mounts. 
            Defaults to False.
    """
    def __mount(mount_name: str) -> None:
        """
        Mounts a single zone to the system.
        Args:
            mount_name (str): The name of the zone to mount.
        """
        if not f"{mount_point}{mount_name}" in list(map(lambda mount: mount.mountPoint, dbutils.fs.mounts())):
            dbutils.fs.mount(
                source = f"wasbs://{mount_name}@{storage}.blob.core.windows.net/",
                mount_point = f"{mount_point}{mount_name}",
                extra_configs = { 
                    f"fs.azure.account.key.{storage}.blob.core.windows.net": key
                }
            )

    if verbose:
        list(map(lambda mount_name: __mount(mount_name), tqdm(mounts, desc="Mounts", position=0, leave=True)))
    else:
        list(map(lambda mount_name: __mount(mount_name), mounts))


def data_convert(df, as_type: str):
    # Koalas
    if str(type(df)) == "<class 'databricks.koalas.frame.DataFrame'>":
        if as_type.lower() == "pandas":
            return df.to_pandas()  # Koalas to Pandas
        elif as_type.lower() == "spark":
            return df.to_spark()   # Koalas to Spark
        elif as_type.lower() in ["pyspark.pandas", "spark.pandas", "ps"]:
            try:
                return data_convert(data_convert(df, as_type = "pandas"), as_type = "pyspark.pandas")
            except:
                try:
                    return data_convert(data_convert(df, as_type = "spark"), as_type = "pyspark.pandas")
                except:
                    raise
        elif as_type.lower() == "koalas":
            return df

    # Pandas
    elif str(type(df)) == "<class 'pandas.core.frame.DataFrame'>":
        if as_type.lower() == "koalas":
            return from_pandas(df) # Pandas to Koalas
        elif as_type.lower() == "spark":
            # Pandas to Spark
            try:
                return spark.createDataFrame(df)
            except:
                return spark.createDataFrame(df.astype(str))
        elif as_type.lower() in ["pyspark.pandas", "spark.pandas", "ps"]:
            return ps_from_pandas(df)
        elif as_type.lower() == "pandas":
            return df

    # Spark
    elif str(type(df)) == "<class 'pyspark.sql.dataframe.DataFrame'>":
        if as_type.lower() == "pandas":
            return df.toPandas()   # Spark to Pandas
        elif as_type.lower() == "koalas":
            return KoalasDataFrame(df)
        elif as_type.lower() in ["pyspark.pandas", "spark.pandas", "ps"]:
            try:
                return df.pandas_api()
            except:
                try:
                    return df.to_pandas_on_spark()
                except:
                    raise
        elif as_type.lower() == "spark":
            return df
    # Pyspark.pandas
    elif str(type(df)) == "<class 'pyspark.pandas.frame.DataFrame'>":
        if as_type.lower() == "pandas":
            return df.to_pandas()
        elif as_type.lower() == "koalas":
            # No native support for pyspark.pandas to databrick.koalas.
            # This needs to be done by a doble transformation.
            try:
                return from_pandas(df.to_pandas())
            except:
                try:
                    return KoalasDataFrame(df)
                except:
                    raise
        elif as_type.lower() == "spark":
            return df.to_spark()
        elif as_type.lower() in ["pyspark.pandas", "spark.pandas", "ps"]:
            return df


def select(df, columns: List[str]):
    if str(type(df)) == "<class 'pandas.core.frame.DataFrame'>":
        return df[columns]
    elif str(type(df)) == "<class 'pyspark.sql.dataframe.DataFrame'>":
        return df.select(columns)
    elif str(type(df)) == "<class 'databricks.koalas.frame.DataFrame'>":
        return df[columns]
    elif str(type(df)) == "<class 'pyspark.pandas.frame.DataFrame'>":
        return df[columns]


def to_list(df, columns: List[str] = None):
    if str(type(df)) in ["<class 'databricks.koalas.series.Series'", "<class 'pandas.core.series.Series'", "<class 'pyspark.sql.column.Column'", "<class 'pyspark.pandas.series.Series'"]:
        if str(type(df)) == "<class 'databricks.koalas.series.Series'":
            return df.to_list()
        elif str(type(df)) == "<class 'pandas.core.series.Series'":
            return df.to_list()
        elif str(type(df)) == "<class 'pyspark.sql.column.Column'":
            return df.rdd.flatMap(lambda x: x).collect()
        elif str(type(df)) == "<class 'pyspark.pandas.series.Series'":
            return df.to_list()
    elif str(type(df)) in ["<class 'databricks.koalas.frame.DataFrame'>", "<class 'pandas.core.frame.DataFrame'>", "<class 'pyspark.sql.dataframe.DataFrame'>", "<class 'pyspark.pandas.frame.DataFrame'>"]:
        if not isinstance(columns, list):
            columns = [columns]
        if str(type(df)) == "<class 'pandas.core.frame.DataFrame'>":
            return list(map(lambda column: df[column].to_list(), columns))
        elif str(type(df)) == "<class 'pyspark.sql.dataframe.DataFrame'>":
            return df.select(columns).rdd.flatMap(lambda x: x).collect()
        elif str(type(df)) == "<class 'databricks.koalas.frame.DataFrame'>":
            return list(map(lambda column: df[column].to_list(), columns))
        elif str(type(df)) == "<class 'pyspark.pandas.frame.DataFrame'>":
            return list(map(lambda column: df[column].to_list(), columns))