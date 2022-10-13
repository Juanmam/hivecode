from typing import List
from tqdm import tqdm

# Pyspark
from pyspark.context import SparkContext
from pyspark.sql.session import SparkSession


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


def read_csv(file_name:      str, 
             file_path:      str, 
             delimiter:      str  = ',',
             infer_schema:   bool = True,
             include_header: bool = False,
             as_pandas:      bool = False):
    if as_pandas:
        try:
            return spark.read.format("csv")\
                             .option("delimiter", delimiter)\
                             .option("inferSchema", infer_schema)\
                             .option("header", include_header)\
                             .load(f"{file_path}/{file_name}").toPandas()
        except:
            raise
    else:
        try:
            return spark.read.format("csv")\
                             .option("delimiter", delimiter)\
                             .option("inferSchema", infer_schema)\
                             .option("header", include_header)\
                             .load(f"{file_path}/{file_name}")
        except:
            raise


def read_json(file_name:   str, 
             file_path:    str,
             is_multiline: bool = False,
             as_pandas:    bool = False):
    if as_pandas:
        try:
            return spark.read.format("json")\
                             .option("multiline",is_multiline) \
                             .load(f"{file_path}/{file_name}").toPandas()
        except:
            raise
    else:
        try:
            return spark.read.format("json")\
                             .option("multiline",is_multiline) \
                             .load(f"{file_path}/{file_name}")
        except:
            raise


def read_parquet(file_name: str,
                 path:      str,
                 as_pandas: bool = False):
    if as_pandas:
        try:
            return spark.read.format("parquet")\
                             .load(f"{path}/{file_name}").toPandas()
        except:
            raise
    else:
        try:
            return spark.read.format("parquet")\
                             .load(f"{path}/{file_name}")
        except:
            raise


def read_orc(file_name: str,
             path:      str,
             condition: str  = '',
             as_pandas: bool = False):
    if as_pandas:
        try:
            return spark.read.format("orc")\
                             .load(f"{path}/{file_name}/{condition}").toPandas()
        except:
            raise
    else:
        try:
            return spark.read.format("orc")\
                             .load(f"{path}/{file_name}/{condition}")
        except:
            raise


def read_text(file_name:        str,
              path:             str,
              line_separator:   str  = "\n",
              whole_text:       bool = False,
              as_pandas:        bool = False):
    if as_pandas:
        try:
            return spark.read.format("text")\
                             .option("lineSep", line_separator)\
                             .option("wholetext", whole_text)\
                             .load(f"{path}/{file_name}").toPandas()
        except:
            raise
    else:
        try:
            return spark.read.format("text")\
                             .option("lineSep", line_separator)\
                             .option("wholetext", whole_text)\
                             .load(f"{path}/{file_name}")
        except:
            raise


def read_jdbc(server_url: str,
              schema:     str,
              table_name: str,
              user:       str,
              password:   str,
              sql_type:   str  = "mysql",
              as_pandas:  bool = False):
    if as_pandas:
        try:
            return spark.read.format("jdbc")\
                             .option("url", f"jdbc:{sql_type}:{server_url}")\
                             .option("dbtable", f"{schema}.{table_name}")\
                             .option("user", user)\
                             .option("password", password)\
                             .load().toPandas()
        except:
            raise
    else:
        try:
            return spark.read.format("jdbc")\
                             .option("url", f"jdbc:{sql_type}:{server_url}")\
                             .option("dbtable", f"{schema}.{table_name}")\
                             .option("user", user)\
                             .option("password", password)\
                             .load()
        except:
            raise


def read_libsvm(file_name:        str,
                path:             str,
                number_features:  str = None,
                as_pandas:        bool = False):
    if as_pandas:
        
        try:
            if number_features:
                return spark.read.format("libsvm")\
                                 .option("numFeatures", number_features)\
                                 .load(f"{path}/{file_name}").toPandas()
            else:
                return spark.read.format("libsvm")\
                                 .load(f"{path}/{file_name}").toPandas()
        except:
            raise
    else:
        try:
            if number_features:
                return spark.read.format("libsvm")\
                                 .option("numFeatures", number_features)\
                                 .load(f"{path}/{file_name}")        
            else:
                return spark.read.format("libsvm")\
                                 .load(f"{path}/{file_name}")
        except:
            raise
