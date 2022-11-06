from .constants import PANDAS_TYPES, PYSPARK_TYPES, KOALAS_TYPES, PANDAS_ON_SPARK_TYPES, PANDAS, KOALAS, SPARK, PANDAS_ON_SPARK, IN_PANDAS_ON_SPARK

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
    if df_type(df) == KOALAS:
        if as_type.lower() == PANDAS:
            return df.to_pandas()  # Koalas to Pandas
        elif as_type.lower() == SPARK:
            return df.to_spark()   # Koalas to Spark
        elif as_type.lower() in IN_PANDAS_ON_SPARK:
            try:
                return data_convert(data_convert(df, as_type = PANDAS), as_type = PANDAS_ON_SPARK)
            except:
                try:
                    return data_convert(data_convert(df, as_type = SPARK), as_type = PANDAS_ON_SPARK)
                except:
                    raise
        elif as_type.lower() == KOALAS:
            return df

    # Pandas
    elif df_type(df) == PANDAS:
        if as_type.lower() == KOALAS:
            return from_pandas(df) # Pandas to Koalas
        elif as_type.lower() == SPARK:
            # Pandas to Spark
            try:
                return spark.createDataFrame(df)
            except:
                return spark.createDataFrame(df.astype(str))
        elif as_type.lower() in IN_PANDAS_ON_SPARK:
            return ps_from_pandas(df)
        elif as_type.lower() == PANDAS:
            return df

    # Spark
    elif df_type(df) == PYSPARK:
        if as_type.lower() == PANDAS:
            return df.toPandas()   # Spark to Pandas
        elif as_type.lower() == KOALAS:
            return KoalasDataFrame(df)
        elif as_type.lower() in IN_PANDAS_ON_SPARK:
            try:
                return df.pandas_api()
            except:
                try:
                    return df.to_pandas_on_spark()
                except:
                    raise
        elif as_type.lower() == SPARK:
            return df
        
    # Pyspark.pandas
    elif df_type(df) == PANDAS_ON_SPARK:
        if as_type.lower() == PANDAS:
            return df.to_pandas()
        elif as_type.lower() == KOALAS:
            # No native support for pyspark.pandas to databrick.koalas.
            # This needs to be done by a doble transformation.
            try:
                return from_pandas(df.to_pandas())
            except:
                try:
                    return KoalasDataFrame(df)
                except:
                    raise
        elif as_type.lower() == SPARK:
            return df.to_spark()
        elif as_type.lower() in IN_PANDAS_ON_SPARK:
            return df


def select(df, columns: List[str]):
    if str(type(df)) == PANDAS_TYPES.get("df"):
        return df[columns]
    elif str(type(df)) == PYSPARK_TYPES.get("df"):
        return df.select(columns)
    elif str(type(df)) == KOALAS_TYPES.get("df"):
        return df[columns]
    elif str(type(df)) == PANDAS_ON_SPARK_TYPES.get("df"):
        return df[columns]


def to_list(df, columns: List[str] = None):
    if str(type(df)) in [KOALAS_TYPES.get("series"), PANDAS_TYPES.get("series"), PYSPARK_TYPES.get("series"), PANDAS_ON_SPARK_TYPES.get("series")]:
        if str(type(df)) == KOALAS_TYPES.get("series"):
            return df.to_list()
        elif str(type(df)) == PANDAS_TYPES.get("series"):
            return df.to_list()
        elif str(type(df)) == PYSPARK_TYPES.get("series"):
            return df.rdd.flatMap(lambda x: x).collect()
        elif str(type(df)) == PANDAS_ON_SPARK_TYPES.get("series"):
            return df.to_list()
    elif str(type(df)) in [KOALAS_TYPES.get("df"), PANDAS_TYPES.get("df"), PYSPARK_TYPES.get("df"), PANDAS_ON_SPARK_TYPES.get("df")]:
        if not isinstance(columns, list):
            columns = [columns]
        if str(type(df)) == PANDAS_TYPES.get("df"):
            return list(map(lambda column: df[column].to_list(), columns))
        elif str(type(df)) == PYSPARK_TYPES.get("df"):
            return df.select(columns).rdd.flatMap(lambda x: x).collect()
        elif str(type(df)) == KOALAS_TYPES.get("df"):
            return list(map(lambda column: df[column].to_list(), columns))
        elif str(type(df)) == PANDAS_ON_SPARK_TYPES.get("df"):
            return list(map(lambda column: df[column].to_list(), columns))


def df_type(df):
    if str(type(df)) == "<class 'pandas.core.frame.DataFrame'>":
        return "pandas"
    elif str(type(df)) == "<class 'pyspark.sql.dataframe.DataFrame'>":
        return "spark"
    elif str(type(df)) == "<class 'databricks.koalas.frame.DataFrame'>":
        return "koalas"
    elif str(type(df)) == "<class 'pyspark.pandas.frame.DataFrame'>":
        return "ps"