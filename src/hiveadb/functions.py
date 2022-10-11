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
