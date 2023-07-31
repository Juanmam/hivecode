from typing import List, Union, Any

from hivecore.functions import lib_required, LazyImport

from pyspark.context import SparkContext
from pyspark.sql.session import SparkSession

# DBUtils
try:
    # up to DBR 8.2
    from dbutils import DBUtils  # pylint: disable=import-error,wrong-import-position
except:
    # above DBR 8.3
    from dbruntime.dbutils import DBUtils  # pylint: disable=import-error,wrong-import-position


def get_spark() -> SparkSession:
    """
    Fetches the current instance of Spark. If none exists, creates a new one.

    :return: The current Spark session.
    :rtype: pyspark.sql.SparkSession
    """
    # Spark
    sc: SparkContext = SparkContext.getOrCreate()
    return SparkSession(sc)


def get_dbutils(spark: SparkSession = None) -> DBUtils:
    """
    Fetches the current instance of DBUtils.

    :param spark: The current spark session. Defaults to None.
    :type spark: pyspark.sql.SparkSession

    :return: An instance of DBUtils.
    :rtype: databricks.DBUtils
    """
        
    # We try to create dbutils from spark or by using IPython
    try:
        return DBUtils(spark)
    except:
        import IPython
        return IPython.get_ipython().user_ns["dbutils"]


spark   = get_spark()
dbutils = get_dbutils()


def mount(
    storage: str,
    key: str,
    mount_point: str = '/mnt/',
    mounts: List[str] = ["raw", "silver", "gold"],
    postfix: str = '-zone',
    include_tqdm: bool = False,
    verbose: bool = False
) -> None:
    """
    Mounts a set of zones into the system.

    :param storage: The name of the storage to mount. This can be found at keys access in your storage account.
    :type storage: str
    
    :param key: The key of the storage to mount. This can be found at keys access in your storage account.
    :type key: str
    
    :param mount_point: The mount point to use, defaults to '/mnt/'.
    :type mount_point: str, optional
    
    :param mounts: A list of all the mounts you want. This doesn't include the prefix. Check example. 
        Defaults to ["raw", "silver", "gold"].
    :type mounts: List[str], optional
    
    :param postfix: The postfix is the ending you want to put to your mount zones. Set it to an empty 
        string if you don't want to apply it, defaults to '-zone'.
    :type postfix: str, optional
    
    :param include_tqdm: A flag to include tqdm bars for mounts, defaults to False.
    :type include_tqdm: bool, optional
    
    :param verbose: A flag to indicate whether to show tqdm progress bar or not, defaults to False.
    :type verbose: bool, optional
    
    :return: None
    :rtype: None
    """
    def __mount(mount_name: str) -> None:
        """
        Mounts a single zone to the system.

        :param mount_name: The name of the zone to mount.
        :type mount_name: str

        :return: None
        :rtype: None
        """
        if not f"{mount_point}{mount_name}" in list(map(lambda mount: mount.mountPoint, dbutils.fs.mounts())):
            dbutils.fs.mount(
                source=f"wasbs://{mount_name}@{storage}.blob.core.windows.net/",
                mount_point=f"{mount_point}{mount_name}",
                extra_configs={f"fs.azure.account.key.{storage}.blob.core.windows.net": key}
            )

    lib_required("tqdm")
    from tqdm import tqdm

    if include_tqdm:
        mount_iterator = tqdm(mounts, desc="Mounts", position=0, leave=True)
    else:
        mount_iterator = mounts

    if verbose:
        mount_iterator = tqdm(mount_iterator)

    for mount_name in mount_iterator:
        __mount(mount_name)