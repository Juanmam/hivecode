### imports
# Required to get spark context, dbutils and for data convertion
from hiveadb.function import get_spark, get_dbutils, data_convert

lib_required("databricks.koalas")

spark   = get_spark()   # Get the spark context
dbutils = get_dbutils() # Get the dbutils object

# To force install a library that is required if its not installed yet
from hivecore.function import lib_required

# For deprecating old or not implemented functions
from hivecore.decorator import deprecated

# Required Constants.
from hivecore.constant import PANDAS_TYPES, PYSPARK_TYPES, KOALAS_TYPES, PANDAS_ON_SPARK_TYPES, PANDAS, KOALAS, SPARK, PYSPARK, PANDAS_ON_SPARK, IN_PANDAS_ON_SPARK

# Required for type hinting.
from typing import Optional, Union, List, Dict, Tuple, Any, Type

from pandas import DataFrame as PandasDataFrame            # Used for type hinting
from pyspark.sql import DataFrame as PysparkDataFrame      # Used for type hinting
from databricks.koalas import DataFrame as KoalasDataFrame # Used for type hinting

from pandas import concat
from pyspark.sql import SparkSession  # For creating Spark sessions
from pyspark.sql import DataFrame  # For working with Spark DataFrames
from pyspark.sql.types import StructType  # For defining StructType schema
from pyspark.sql.functions import col  # For working with columns in DataFrames
from pyspark.sql.functions import when  # For working with conditional expressions in DataFrames
from pyspark.sql.functions import concat_ws  # For concatenating strings with a separator
from pyspark.sql.functions import regexp_replace  # For replacing characters with regular expressions in DataFrames
from pyspark.sql.functions import split  # For splitting a string into an array of substrings in DataFrames
from pyspark.sql.functions import from_unixtime  # For converting Unix timestamp to datetime format in DataFrames
from pyspark.sql.functions import lit  # For creating a new column with a constant value in DataFrames
from pyspark.sql.functions import current_timestamp  # For getting the current timestamp in DataFrames
from pyspark.sql.functions import first  # For getting the first value in a group in DataFrames
from pyspark.sql.functions import desc  # For sorting in descending order in DataFrames
from pyspark.sql.functions import asc  # For sorting in ascending order in DataFrames
from pyspark.sql.functions import sum as sql_sum  # For calculating the sum of a column in DataFrames
from pyspark.sql.functions import countDistinct  # For counting distinct values in a column in DataFrames
from pyspark.sql.functions import udf  # For creating user-defined functions in PySpark
from pyspark.sql.types import IntegerType  # For defining integer data type
from pyspark.sql.types import DoubleType  # For defining double data type
from pyspark.sql.types import StringType  # For defining string data type
from pyspark.sql.types import BooleanType  # For defining boolean data type
from pyspark.sql.types import TimestampType  # For defining timestamp data type
from py4j.protocol import Py4JJavaError # Required for error handling
from os import system, path as ospath # Used for path finding and command input
from functools import lru_cache # Used to keep a cache
from json import loads # Used to read JSON files
from typing import Union
from typing import List

# Required for Threading
from multiprocessing.pool import ThreadPool
from threading import Thread
from queue import Queue

lib_required('paramiko')

# import the required module
from paramiko import SSHClient, AutoAddPolicy

from string import digits, ascii_letters, punctuation
from random import choice

from databricks.koalas import read_excel as koalas_read_excel, read_delta as koalas_read_delta, read_table as koalas_read_table,\
read_json as koalas_read_json, read_csv as koalas_read_csv, read_parquet as koalas_read_parquet, read_orc as koalas_read_orc,\
read_sql as koalas_read_sql
from pyspark.pandas import read_delta as ps_read_delta, read_table as ps_read_table, read_orc as ps_read_orc, \
read_sql_query
from pandas import DataFrame, concat, ExcelWriter, read_csv as pandas_read_csv, read_json as pandas_read_json, read_parquet as pandas_read_parquet,\
read_excel as pandas_read_excel, read_orc as pandas_read_orc

### Raw functions
## Delta Tables
# Read

def read_table(table_name: str, 
               db: str = "default", 
               as_type: str = PANDAS, 
               index: Union[str, List[str], None] = None, 
               engine: str = KOALAS, 
               threads: int = 2) -> Union[PandasDataFrame, PysparkDataFrame, KoalasDataFrame]:
    """
    Reads data from a specified table in a database.

    :param table_name: The name of the table to read from.
    :type table_name: str

    :param db: The name of the database to read from. Defaults to "default".
    :type db: str

    :param as_type: The type of dataframe to return. Valid values are "pandas" or "koalas". Defaults to "koalas".
    :type as_type: str

    :param index: Column name or list of column names to set as index after reading the table. If None, the default index is used. Defaults to None.
    :type index: Union[str, List[str], None]

    :param engine: The engine to use for reading the table. Valid values are "pandas" or "koalas". Defaults to "koalas".
    :type engine: str

    :param threads: The number of threads to use for reading the table. Defaults to 2.
    :type threads: int

    :return: A pandas or koalas DataFrame representing the contents of the specified table.
    :rtype: Union[PandasDataFrame, PysparkDataFrame, KoalasDataFrame]
    """
    def read_table_f(table_name: str, db: str = "default", as_type: str = KOALAS, index: Union[str, List[str], None] = None, engine: str = KOALAS):
        if engine == KOALAS:
            return data_convert(koalas_read_table(f"{db}.{table_name}", index_col = index), as_type=as_type)
        elif engine == PYSPARK:
            query = f"SELECT * FROM {db}.{table_name}"
            if index:
                return data_convert(spark.sql(query), as_type=as_type).set_index(index)
            else:
                return data_convert(spark.sql(query), as_type=as_type)
        elif engine == PANDAS:
            # Pandas doesn't have this originally, we use ps for this.
            # Remove .as_pandas when data_convert supports ps.
            return data_convert(ps_read_table(f"{db}.{table_name}", index_col = index).to_pandas(), as_type=as_type)
    if isinstance(table_name, list):
        pool = ThreadPool(threads)
        return list(map(lambda table: pool.apply_async(read_table_f, kwds={"table_name": table, "db": db, "as_type": as_type, "index": index, "engine": engine}).get(), table_name))
    elif isinstance(table_name, str):
        return read_table_f(table_name, db, as_type, index, engine)


# Write
def write_table(df: Union[PandasDataFrame, PysparkDataFrame, KoalasDataFrame, List[Union[PandasDataFrame, PysparkDataFrame, KoalasDataFrame]]], 
                table_name: Union[str, List[str]], 
                db: str = "default", 
                delta_path: str = "/FileStore/tables/", 
                mode: str = "overwrite", 
                partitions: Optional[Union[str, List[str]]] = None, 
                index: Optional[Union[str, List[str]]] = None, 
                threads: int = 2) -> None:
    """
    Writes a Delta table in Databricks with the given dataframe, table name, and database name. It can create multiple tables using multithreading.

    :param df: DataFrame or list of DataFrames to be written to Delta
    :type df: Union[PandasDataFrame, PysparkDataFrame, KoalasDataFrame, List[Union[PandasDataFrame, PysparkDataFrame, KoalasDataFrame]]]

    :param table_name: Table name or list of table names to be created
    :type table_name: Union[str, List[str]]

    :param db: Database name in which the table will be created. Defaults to "default".
    :type db: str

    :param delta_path: Path to save the Delta table. Defaults to "/FileStore/tables/".
    :type delta_path: str

    :param mode: Writing mode. "overwrite" (default) will replace existing tables. "append" will add to existing tables.
    :type mode: str

    :param partitions: The column names or expressions used to partition the table. Defaults to None.
    :type partitions: Optional[Union[str, List[str]]]

    :param index: Column name(s) to be used as the table index. Defaults to None.
    :type index: Optional[Union[str, List[str]]]

    :param threads: Number of threads to use for multithreading. Defaults to 2.
    :type threads: int
    """
    # We pack items in a list to be able to use them in he threads process.
    if not isinstance(df, list):
        df = [df]
    if not isinstance(table_name, list):
        table_name = [table_name]
        
    # This is the normal definition that will create a single table.
    def write_table_f(df, table_name, db: str = "default", delta_path = "/FileStore/tables/", mode: str = "overwrite", partitions: Union[str, List[str], None] = None, index: Union[str, List[str], None] = None) -> None:
        # Write .delta file in datalake
        delta_path = f'{delta_path}/{db}/{table_name}/'.replace('//', '/')
        data_convert(df, as_type=KOALAS).to_delta(delta_path, mode=mode, partition_cols = partitions, index_col = index)
        # Create DB if not exist
        spark.sql(f"CREATE DATABASE IF NOT EXISTS {db}")
        # Create Table
        ddl_query = f"CREATE TABLE IF NOT EXISTS {db}.{table_name} USING DELTA LOCATION '{delta_path}'"
        spark.sql(ddl_query)
    
    # Create a queue
    q = Queue()

    # Define the amount of threads
    worker_count = threads

    # Organize args into queue
    for _df, table in zip(df, table_name):
        kwargs = locals()
        args = dict()
        args["table_name"] = table
        args["df"] = _df
        args["db"] = kwargs.get("db")
        args["delta_path"] = kwargs.get("delta_path")
        args["mode"] = kwargs.get("mode")
        args["partitions"] = kwargs.get("partitions")
        args["index"] = kwargs.get("index")
        q.put(args)

    # Wrapper to run tasks
    def run_tasks(function, q):
        while not q.empty():
            kwargs = q.get()
            df = kwargs.get("df")
            table_name = kwargs.get("table_name")
            db = kwargs.get("db")
            delta_path = kwargs.get("delta_path")
            mode = kwargs.get("mode")
            function(df = df, table_name = table_name, db = db, delta_path = delta_path, mode = mode)
            q.task_done()

    # Run tasks in threads
    for i in range(worker_count):
        t=Thread(target=run_tasks, args=(write_table_f, q))
        t.daemon = True
        t.start()

    # Finish process
    return q.join()


## CSV
# Read
def read_csv(file_name: Union[str, List[str]], 
             path: str, 
             sep: str = ",", 
             header: Union[bool, int] = 0,
             source: str = "dbfs", 
             as_type: type = PANDAS, 
             engine: str = KOALAS,
             threads: int = 2) -> Union[PandasDataFrame, PysparkDataFrame, KoalasDataFrame, List[Union[PandasDataFrame, PysparkDataFrame, KoalasDataFrame]]]:
    """
    Read one or multiple CSV files from a given path and return a dataframe object.

    :param file_name: Name or list of names of the CSV file(s) to be read.
    :type file_name: Union[str, List[str]]

    :param path: Path where the CSV file(s) are located.
    :type path: str

    :param sep: Separator used in the CSV file(s).
    :type sep: str

    :param header: Whether to use the first row of the CSV file(s) as column names or not. If an integer is passed, it will use that row as the header.
    :type header: Union[bool, int]

    :param source: Data source where the CSV file(s) are located. Default is "dbfs" for Databricks File System.
    :type source: str

    :param as_type: Type of dataframe object to be returned. Default is KoalasDataFrame.
    :type as_type: type

    :param engine: CSV reading engine to be used. Options are "koalas", "pandas", or "pyspark". Default is "koalas".
    :type engine: str

    :param threads: Number of threads to use when reading multiple CSV files. Default is 2.
    :type threads: int

    :return: A dataframe object or list of dataframe objects depending on the type of `file_name` parameter and the `as_type` parameter.
    :rtype: Union[PandasDataFrame, PysparkDataFrame, KoalasDataFrame, List[Union[PandasDataFrame, PysparkDataFrame, KoalasDataFrame]]]
    """
    def read_csv_f(file_name: str, path: str, sep: str = ",", header: bool = 0, source: str = "dbfs", as_type: str = KOALAS, engine: str = KOALAS):
        if engine == KOALAS:
            if header and isinstance(header, bool):
                header = 'infer'
            elif not header and isinstance(header, bool):
                header = None
            if isinstance(header, int): 
                header_index = header
                header = False

            df = koalas_read_csv(f"{source}:{path}/{file_name}", sep = sep, header = header)

            if header_index:
                for df_column in df.columns:
                    df = df.rename(columns = {df_column: f"{df.loc[header_index][df_column]}"})
                df = df.loc[header_index:]
        elif engine == PANDAS:
            if header and isinstance(header, bool):
                header = 'infer'
            elif not header and isinstance(header, bool):
                header = None
            else:
                header = header

            df = pandas_read_csv(f"/{source}{path}/{file_name}", sep = sep, header = header)
        elif engine == PYSPARK:
            if isinstance(header, int): 
                header_index = header
                header = False
            else:
                header_index = False

            df = spark.read.format("csv")\
                           .option("header", header)\
                           .option("sep", sep)\
                           .load(f"{source}:{path}/{file_name}")

            if isinstance(header_index, int):
                for df_column in df.columns:
                    df = df.withColumnRenamed(df_column, df.limit(header_index + 1).tail(1)[0][df_column])
                    df = df.join(df.limit(header_index + 1), df.columns, how = "leftanti")
            else:
                if not header:
                    for df_column in df.columns:
                        df = df.withColumnRenamed(df_column, df_column.replace("_c", ""))
        return data_convert(df, as_type=as_type)
    if isinstance(file_name, list):
        pool = ThreadPool(threads)
        return list(map(lambda file: pool.apply_async(read_csv_f, kwds={'file_name': file, 'path': path, 'sep': sep, 'header': header, 'source': source, 'as_type': as_type, 'engine': engine}).get(), file_name))
    elif isinstance(file_name, str):
        return read_csv_f(file_name, path, sep, header, source, as_type, engine)

# Write
def write_csv(df: Union[PandasDataFrame, PysparkDataFrame, KoalasDataFrame, List[Union[PandasDataFrame, PysparkDataFrame, KoalasDataFrame]]], 
              file_name: Union[List[str], str], 
              path: str, 
              sep: str = ',', 
              nas: str = '', 
              header: bool = True, 
              mode: str = 'overwrite', 
              num_files: int = 1, 
              threads: int = 2) -> List[None]:
    """
    Writes one or more pandas or Koalas dataframes to one or more CSV files.
    
    :param df: One or more pandas or Koalas dataframes to write to CSV.
    :type df: Union[PandasDataFrame, PysparkDataFrame, KoalasDataFrame, List[Union[PandasDataFrame, PysparkDataFrame, KoalasDataFrame]]]

    :param file_name: One or more names for the CSV file(s) to write to.
    :type file_name: Union[List[str], str]

    :param path: The path of the directory where the CSV file(s) will be saved.
    :type path: str

    :param sep: The separator to use in the CSV file(s). Defaults to ','.
    :type sep: str, optional

    :param nas: The string to use to represent missing values. Defaults to ''.
    :type nas: str, optional

    :param header: Whether to include a header row in the CSV file(s). Defaults to True.
    :type header: bool, optional

    :param mode: Whether to overwrite or append to an existing CSV file. Defaults to 'overwrite'.
    :type mode: str, optional

    :param num_files: The number of output files to create. Defaults to 1.
    :type num_files: int, optional

    :param threads: The number of threads to use for writing the CSV file(s). Defaults to 2.
    :type threads: int, optional

    :return: A list of None values, one for each CSV file written.
    :rtype: List[None]
    """

    # Remove the threads argument from kwds since we don't want to pass it to write_csv_f
    kwds = locals()
    del kwds['threads']

    def write_csv_f(df, file_name: str, path: str, sep: str = ',', nas: str = '', header: bool = True, mode: str = 'overwrite', num_files: int = 1):
        # Create a temporary directory and save the dataframe as CSV
        _temp = f"temp_{file_name.split('.')[0]}"
        data_convert(df, as_type=KOALAS).to_csv(path=fr'{path}/{_temp}/', sep = sep, na_rep = nas, header = header, mode = mode, num_files= num_files)
        
        # Get the list of files written to the temporary directory
        files = [file.name for file in dbutils.fs.ls(f"{path}/{_temp}/") if not file.name.startswith("_")]
        i = 0
        for file in files:
            if len(files)>1:
                f_name = f"{file_name.split('.')[0]}_{str(i)}{file_name.split('.')[1]}"
                i += 1
            else:
                f_name = file_name
            if not file_name.endswith(".csv"):
                file_name = f"{f_name}.csv"
            dbutils.fs.mv(f"{path}/{_temp}/{file}", f"{path}/{f_name}")
            dbutils.fs.rm(f"{path}/{_temp}", True)
    
    # We encapsulate the data into a list if it's not already a list.
    if not isinstance(df, list):
        df = [df]
    if not isinstance(file_name, list):
        file_name = [file_name]
    pool = ThreadPool(threads)
    def gen_args(kwds, file):
        kwds["file_name"] = file
        return kwds
    kwds = list(map(lambda file: gen_args(kwds, file), file_name))
    return list(map(lambda kwd: pool.apply_async(write_csv_f, kwds=kwd).get(), kwds))

## Parquet
# Read
def read_parquet(file_name: str, 
                 path: str, 
                 source: str = "dbfs", 
                 as_type: str = PANDAS, 
                 engine: str = KOALAS, 
                 threads: int = 2) -> Union[PandasDataFrame, KoalasDataFrame, PysparkDataFrame, List[PandasDataFrame, KoalasDataFrame, PysparkDataFrame]]:
    """
    Read a Parquet file and convert it to the specified data format.

    :param file_name: Name of the Parquet file to read.
    :type file_name: str

    :param path: Path of the directory containing the file.
    :type file_name: str

    :param source: Source of the file. Defaults to "dbfs".
    :type file_name: str

    :param as_type: Type of data format to convert to. Can be "pandas", "koalas", or "pyspark". Defaults to "pandas".
    :type file_name: str

    :param engine: Engine to use for reading Parquet files. Can be "pandas", "koalas", or "spark". Defaults to "koalas".
    :type file_name: str

    :param threads: Number of threads to use for parallel reading. Defaults to 2.
    :type file_name: int

    :return: The Parquet file data converted to the specified format(s).
    :rtype: Union[PandasDataFrame, KoalasDataFrame, PysparkDataFrame, List[PandasDataFrame, KoalasDataFrame, PysparkDataFrame]]
    """
    def read_parquet_f(file_name: str, path: str, source: str = "dbfs", as_type: str = KOALAS, engine: str = KOALAS):
        if engine.lower() == KOALAS:
            return data_convert(koalas_read_parquet(f"{source}:{path}/{file_name}"), as_type=as_type)
        elif engine.lower() == PYSPARK:
            try:
                return data_convert(spark.read.format("parquet").load(f"{source}:{path}/{file_name}"), as_type=as_type)
            except:
                try:
                    return data_convert(spark.read.format("parquet").load(f"{source}:{path}/{file_name}/"), as_type=as_type)
                except:
                    raise
        elif engine.lower() == PANDAS:
            # Pandas has an engine option that can be pyarrow or fastparquet. Defaults to pyarrow.
            return data_convert(pandas_read_parquet(f"/{source}/{path}/{file_name}"), as_type=as_type)
        
    if isinstance(file_name, list):
        pool = ThreadPool(threads)
        return list(map(lambda file: pool.apply_async(read_parquet_f, kwds={"file_name": file, "path": path, "source": source, "as_type": as_type, "engine": engine}).get(), file_name))
    elif isinstance(file_name, str):
        if not file_name.endswith(".parquet"):
            file_name = f"{file_name}.parquet"
        return read_parquet_f(file_name, path, source, as_type, engine)

# Write
def write_parquet(df: Union[PandasDataFrame, PysparkDataFrame, KoalasDataFrame, List[Union[PandasDataFrame, PysparkDataFrame, KoalasDataFrame]]], 
                  file_name: Union[str, List[str]], 
                  path: str, 
                  mode: str = "overwrite", 
                  num_files: int = 1, 
                  threads: int = 2) -> List[None]:
    """
    Writes one or multiple Koalas DataFrames as parquet files.

    :param df: A Koalas DataFrame or a list of Koalas DataFrames.
    :type df: Union[PandasDataFrame, PysparkDataFrame, KoalasDataFrame, List[Union[PandasDataFrame, PysparkDataFrame, KoalasDataFrame]]]
    :param file_name: The name or a list of names of the parquet files to be created. If multiple DataFrames are passed, the number of file names must match the number of DataFrames.
    :type file_name: Union[str, List[str]]
    :param path: The path where the parquet files will be stored.
    :type path: str
    :param mode: Specifies the behavior of the write operation when the file or directory already exists. Defaults to "overwrite".
    :type mode: str, optional
    :param num_files: The number of output files to be produced. Defaults to 1.
    :type num_files: int, optional
    :param threads: The number of threads to be used for the write operation. Defaults to 2.
    :type threads: int, optional

    :return: A list of None values indicating that the operation was completed successfully.
    :rtype: List[None]
    """
    kwds = locals()
    del kwds['threads']
    def write_parquet_f(df, file_name: str, path: str, mode: str = "overwrite", num_files: int = 1):
        _temp = f"temp_{file_name.split('.')[0]}"
        data_convert(df, as_type=KOALAS).to_parquet(path=fr'{path}/{_temp}/', mode = mode, num_files= num_files)
        files = [file.name for file in dbutils.fs.ls(f"{path}/{_temp}/") if not file.name.startswith("_")]
        i = 0
        for file in files:
            if len(files)>1:
                f_name = f"{file_name.split('.')[0]}_{str(i)}{file_name.split('.')[1]}"
                i += 1
            else:
                f_name = file_name
            if not file_name.endswith(".csv"):
                file_name = f"{f_name}.csv"
            dbutils.fs.mv(f"{path}/{_temp}/{file}", f"{path}/{f_name}")
            dbutils.fs.rm(f"{path}/{_temp}", True)
    
    # We encapsulate the data into a list if it's not already a list.
    if not isinstance(df, list):
        df = [df]
    if not isinstance(file_name, list):
        file_name = [file_name]
    pool = ThreadPool(threads)
    def gen_args(kwds, file):
        kwds["file_name"] = file
        return kwds
    kwds = list(map(lambda file: gen_args(kwds, file), file_name))
    return list(map(lambda kwd: pool.apply_async(write_parquet_f, kwds=kwd).get(), kwds))

## JSON
# Read
def read_json(file_name: str, 
              path: str, 
              source: str = "dbfs", 
              as_type: str = PANDAS, 
              engine: str = KOALAS, 
              threads: int = 2) -> Union[PandasDataFrame, KoalasDataFrame, PysparkDataFrame, List[PandasDataFrame, KoalasDataFrame, PysparkDataFrame]]:
    """
    Read JSON file from a specified path and return a converted data object.

    :param file_name: Name of the file(s) to be read.
    :type file_name: str or list
    :param path: Path where the file(s) is located.
    :type path: str
    :param source: Type of source to read the file(s) from ("dbfs" or "file").
    :type source: str
    :param as_type: Type of the returned data object ("koalas", "pyspark", or "pandas").
    :type as_type: str
    :param engine: Engine to use for reading the file ("koalas", "pyspark", or "pandas").
    :type engine: str
    :param threads: Number of threads to use for multithreading.
    :type threads: int

    :returns: Converted data object(s).
    :rtype: Union[PandasDataFrame, KoalasDataFrame, PysparkDataFrame, List[PandasDataFrame, KoalasDataFrame, PysparkDataFrame]]
    """
    def read_json_f(file_name: str, path: str, source: str = "dbfs", as_type: str = KOALAS, engine: str = KOALAS):
        if engine == KOALAS:
            try:
                return data_convert(koalas_read_json(f"{source}:{path}/{file_name}", lines = True), as_type=as_type)
            except:
                try:
                    return data_convert(koalas_read_json(f"{source}:{path}/{file_name}", lines = False), as_type=as_type)
                except:
                    raise
        elif engine == PYSPARK:
            try:
                return data_convert(spark.read.format("json").load(f"{source}:{path}/{file_name}"), as_type=as_type)
            except:
                try:
                    return data_convert(spark.read.format("json").load(f"{source}:{path}/{file_name}/"), as_type=as_type)
                except:
                    raise
        elif engine == PANDAS:
            try:
                return data_convert(pandas_read_json(f"/{source}/{path}/{file_name}", lines = True), as_type=as_type)
            except:
                try:
                    return data_convert(pandas_read_json(f"/{source}/{path}/{file_name}", lines = False), as_type=as_type)
                except:
                    raise
        
    if isinstance(file_name, list):
        pool = ThreadPool(threads)
        return list(map(lambda file: pool.apply_async(read_json_f, kwds={"file_name": file, "path": path, "source": source, "as_type": as_type, "engine": engine}).get(), file_name))
    elif isinstance(file_name, str):
        return read_json_f(file_name, path, source, as_type, engine)
    
# Write
def write_json(df: Union[PandasDataFrame, PysparkDataFrame, KoalasDataFrame, List[Union[PandasDataFrame, PysparkDataFrame, KoalasDataFrame]]], 
               file_name: str, 
               path: str, 
               num_files: int = 1) -> None:
    """
    Writes a DataFrame to one or more JSON files in DBFS or local file system.

    :param df: The DataFrame to write.
    :type df: Any
    :param file_name: The name of the file to write to.
    :type file_name: str
    :param path: The path of the file to write to.
    :type path: str
    :param num_files: The number of output files to write. Default is 1.
    :type num_files: int

    :return: None
    :rtype: None
    """
    # Create temporary directory for intermediate files
    _temp = f"temp_{file_name.split('.')[0]}"
    data_convert(df, as_type=KOALAS).to_json(path=fr'{path}/{_temp}/', num_files=num_files)

    # Move and rename output files from temporary directory
    files = [file.name for file in dbutils.fs.ls(f"{path}/{_temp}/") if not file.name.startswith("_")]
    i = 0
    for file in files:
        if len(files)>1:
            f_name = f"{file_name.split('.')[0]}_{str(i)}{file_name.split('.')[1]}"
            i += 1
        else:
            f_name = file_name
        if not file_name.endswith(".json"):
            file_name = f"{f_name}.json"
        dbutils.fs.mv(f"{path}/{_temp}/{file}", f"{path}/{f_name}")
        # Remove temporary directory
        dbutils.fs.rm(f"{path}/{_temp}", True)


## Excel
# Read
def read_excel(file_name: Union[str, List[str]], 
               path: str, 
               source: str = "dbfs", 
               as_type: str = PANDAS, 
               header: bool = True, 
               engine: str = KOALAS, 
               threads: int = 2) -> Union[KoalasDataFrame, PandasDataFrame, PysparkDataFrame, List[Union[KoalasDataFrame, PandasDataFrame, PysparkDataFrame]]]:
    """
    Reads one or more Excel files from a specified path and returns a DataFrame or a list of DataFrames.

    :param file_name: A single file name or a list of file names to read.
    :type file_name: Union[str, List[str]]
    :param path: The path where the Excel files are located.
    :type path: str
    :param source: The source of the files. Defaults to "dbfs".
    :type source: str, optional
    :param as_type: The type of DataFrame to return. Defaults to KOALAS.
    :type as_type: str, optional
    :param header: Whether to infer the header from the first row. Defaults to True.
    :type header: bool, optional
    :param engine: The engine to use to read the Excel files. Defaults to KOALAS.
    :type engine: str, optional
    :param threads: The number of threads to use when reading multiple files. Defaults to 2.
    :type threads: int, optional

    :raises Exception: Raised when there's an error reading the Excel file(s).

    :return: A single DataFrame or a list of DataFrames depending on the number of files read.
    :rtype: Union[KoalasDataFrame, PandasDataFrame, PysparkDataFrame, List[Union[KoalasDataFrame, PandasDataFrame, PysparkDataFrame]]]
    """
    def read_excel_f(file_name: str, path: str, source: str = "dbfs", as_type: str = KOALAS, header: bool = True, engine: str = KOALAS):
        if engine == KOALAS:
            return data_convert(koalas_read_excel(f"{source}:{path}/{file_name}"), as_type=as_type)
        elif engine == PYSPARK:
            try:
                return data_convert(spark.read.format("com.crealytics.spark.excel").option("location", f"{path}/{file_name}").option("header", header).option("inferSchema", True).load(f"{path}/{file_name}"), as_type=as_type)
            except:
                try:
                    return data_convert(spark.read.format("com.crealytics.spark.excel").option("location", f"{path}/{file_name}").option("header", header).option("inferSchema", True).load(f"{path}/{file_name}"), as_type=as_type)
                except Py4JJavaError as e:
                    if "java.lang.ClassNotFoundException:" in str(e):
                        raise Exception("Maven dependency not installed in cluster. Install com.crealytics.spark.excel.")
        elif engine == PANDAS:
            return data_convert(pandas_read_excel(f"/{source}/{path}/{file_name}"), as_type=as_type)
        
    if isinstance(file_name, list):
        pool = ThreadPool(threads)
        return list(map(lambda file: pool.apply_async(read_excel_f, kwds={"file": file, "path": path, "source": source, "as_type": as_type, "header": header, "engine": engine}).get(), file_name))
    elif isinstance(file_name, str):
        try:
            if not file_name.endswith(".xlsx"):
                file_name = f"{file_name}.xlsx"
            return read_excel_f(file_name, path, source, as_type, header, engine)
        except:
            try:
                if not file_name.endswith(".xltx"):
                    file_name = f"{file_name}.xltx"
                return read_excel_f(file_name, path, source, as_type, header, engine)
                
            except:
                try:
                    if not file_name.endswith(".xlsm"):
                        file_name = f"{file_name}.xlsm"
                    return read_excel_f(file_name, path, source, as_type, header, engine)
                except:
                    try:
                        if not file_name.endswith(".xltm"):
                            file_name = f"{file_name}.xltm"
                        return read_excel_f(file_name, path, source, as_type, header, engine)
                    except:
                        raise


# Write
def write_excel(df: Union[PandasDataFrame, PysparkDataFrame, KoalasDataFrame, List[Union[PandasDataFrame, PysparkDataFrame, KoalasDataFrame]]], 
                file_name: Union[str, List[str]], 
                path: str, 
                source: str = "dbfs", 
                extension: str = "xlsx", 
                sheet_name: List[str] = list(), 
                threads: int = 2) -> Union[None, List[None]]:
    """
    Write dataframes to an Excel file.
    
    :param df: Dataframe or list of dataframes to be written to Excel.
    :type df: Union[pandas.DataFrame, pyspark.sql.DataFrame, databricks.koalas.DataFrame, List[Union[pandas.DataFrame, pyspark.sql.DataFrame, databricks.koalas.DataFrame]]]
    
    :param file_name: Name of the Excel file to be created.
    :type file_name: str
    
    :param path: Path where the Excel file will be saved.
    :type path: str
    
    :param source: Source where the Excel file will be saved. Defaults to "dbfs".
    :type source: str
    
    :param extension: Extension of the Excel file. Defaults to "xlsx".
    :type extension: str
    
    :param sheet_name: List of names for the sheets in the Excel file. Defaults to empty list.
    :type sheet_name: List[str]
    
    :param threads: Number of threads to use when writing Excel file. Defaults to 2.
    :type threads: int
    
    :return: None if a single dataframe is passed, list of None if multiple dataframes are passed.
    :rtype: Union[None, List[None]]
    
    :raises: Exception if an error occurs while writing to the Excel file.
    
    Note: This operation writes data into the driver local file system. If not enough storage is given, the operation will fail.
    """
    kwds = locals()
    del kwds['threads']
    def write_excel_f(df, file_name: str, path: str, source: str = "dbfs", extension: str = "xlsx", sheet_name = list()):
        sheet_name = sheet_name + [f'Sheet_{i}' for i in range(1, len(df) - len(sheet_name) + 1)]
        sheet_pos = 0

        file_name = f'{file_name.split(".")[0]}.{extension.replace(".", "")}'
        
        try:
            with ExcelWriter(fr"{file_name}") as writer:
                for dfs in df:
                    data_convert(dfs, as_type = KOALAS).to_excel(writer, sheet_name=sheet_name[sheet_pos])
                    sheet_pos = sheet_pos + 1
            system(fr"mv /databricks/driver/{file_name} /{source}/{path}/{file_name}")
        except:
            if not ospath.isdir(fr"/databricks/driver/{file_name}"):
                system(fr"rm /databricks/driver/{file_name}")
            raise
    if not isinstance(df, list):
        df = [df]
    if not isinstance(file_name, list):
        file_name = [file_name]
        
    df = [ [dfs] if not isinstance(dfs, list) else dfs for dfs in df]
    kwds["df"] = df
    pool = ThreadPool(threads)
    
    def gen_args(kwds, df, file):
        kwds["file_name"] = file
        kwds["df"] = df
        return kwds
    
    kwds = list(map(lambda item: gen_args(kwds, item[0], item[1]), zip(kwds["df"], file_name)))
    return list(map(lambda kwd: pool.apply_async(write_excel_f, kwds=kwd).get(), kwds))

## Delta
# Read
def read_delta(file_name: Union[str, List[str]], 
               path: str, 
               source: str = "dbfs", 
               version: int = None, 
               timestamp: str = None, 
               as_type: str = PANDAS, 
               engine: str = SPARK, 
               threads: int = 2) -> Union[Union[PandasDataFrame, PysparkDataFrame, KoalasDataFrame], List[Union[PandasDataFrame, PysparkDataFrame, KoalasDataFrame]]]:
    """
    Reads a Delta Lake table from the specified location using PySpark or Koalas.

    :param file_name: Name of the Delta table file(s) to read. If multiple file names are provided as a list, reads all tables in parallel. 
    :type file_name: Union[str, List[str]]
    :param path: Location of the Delta table to read.
    :type path: str
    :param source: Source of the Delta table. Defaults to "dbfs".
    :type source: str
    :param version: The version of the Delta table to read. If None, reads the latest version of the table. Defaults to None.
    :type version: int
    :param timestamp: The timestamp of the Delta table to read. If None, reads the latest version of the table. Defaults to None.
    :type timestamp: str
    :param as_type: The type of data to return. If "koalas", returns a Koalas DataFrame. If "pandas", returns a Pandas DataFrame. Defaults to "koalas".
    :type as_type: str
    :param engine: The engine to use for reading the Delta table. If "pyspark", uses PySpark. If "koalas", uses Koalas. Defaults to "pyspark".
    :type engine: str
    :param threads: The number of threads to use for parallel reading. Defaults to 2.
    :type threads: int
    :return: A single or list of DataFrame(s) with the data from the Delta table.
    :rtype: Union[Union[PandasDataFrame, PysparkDataFrame, KoalasDataFrame], List[Union[PandasDataFrame, PysparkDataFrame, KoalasDataFrame]]]
    """
    def read_delta_f(file_name: str, 
                   path: str, 
                   source: str = "dbfs", 
                   version: int = None, 
                   timestamp: str = None, 
                   as_type: str = KOALAS,
                   engine: str  = KOALAS):
        if engine == KOALAS:
            return data_convert(koalas_read_delta(f"{source}:{path}/{file_name}", version=version, timestamp=timestamp), as_type=as_type)
        elif engine == PYSPARK:
            if version:
                return data_convert(spark.read.format("delta").option("versionAsOf", version).load(f"{source}:{path}/{file_name}"), as_type = as_type)
            elif timestamp:
                return data_convert(spark.read.format("delta").option('timestampAsOf', timestamp).load(f"{source}:{path}/{file_name}"), as_type = as_type)
            else:
                return data_convert(spark.read.format("delta").load(f"{source}:{path}/{file_name}"), as_type = as_type)
        elif engine == PANDAS:
            # Pandas doesn't have this originally, we use ps for this.
            # Remove .as_pandas when data_convert supports ps.
            return data_convert(ps_read_delta(f"/{source}/{path}/{file_name}").to_pandas(), as_type = as_type)
    if isinstance(file_name, list):
        pool = ThreadPool(threads)
        return list(map(lambda file: pool.apply_async(read_delta_f, kwds={"file_name": file_name, "path": path, "source": source, "version": version, "timestamp": timestamp, "as_type": as_type, "engine": engine}).get(), file_name))
    elif isinstance(file_name, str):
        return read_delta_f(file_name, path, source, version, timestamp, as_type, engine)


# Write
def write_delta(df: Union[PandasDataFrame, PysparkDataFrame, KoalasDataFrame, List[Union[PandasDataFrame, PysparkDataFrame, KoalasDataFrame]]], 
                file_name: Union[str, List[str]], 
                path: str, 
                source: str = "dbfs", 
                mode: str = "overwrite", 
                threads: int = 2) -> Union[None, List[None]]:
    """
    Writes Delta Lake tables to the specified file path.
    
    :param df: A DataFrame or a list of DataFrames to be written to Delta Lake tables.
    :type df: Union[pandas.DataFrame, pyspark.sql.DataFrame, databricks.koalas.DataFrame, List[Union[pandas.DataFrame, pyspark.sql.DataFrame, databricks.koalas.DataFrame]]]
    
    :param file_name: The name of the Delta Lake file to be written.
    :type file_name: str
    
    :param path: The file path to write the Delta Lake tables.
    :type path: str
    
    :param source: The source type of the file system, default to "dbfs".
    :type source: str
    
    :param mode: The save mode, default to "overwrite".
    :type mode: str
    
    :param threads: The number of threads to use for writing Delta Lake tables.
    :type threads: int
    
    :return: None if `df` is a single DataFrame; otherwise, a list of None.
    :rtype: Union[None, List[None]]
    """
    kwds = locals()
    del kwds['threads']
    def write_delta_f(df, file_name: str, path: str, source: str = "dbfs", mode = "overwrite"):
        delta_path = path
        _temp = file_name#f"temp_{file_name.split('.')[0]}"
        data_convert(df, as_type=KOALAS).to_delta(path=fr'{path}/{_temp}/', mode = mode)
        files = [file.name for file in dbutils.fs.ls(f"{path}/{_temp}/") if not file.name.startswith("_")]
        i = 0
        for file in files:
            if len(files)>1:
                f_name = f"{file_name.split('.')[0]}_{str(i)}{file_name.split('.')[1]}"
                i += 1
            else:
                f_name = file_name
            if not file_name.endswith(".delta"):
                file_name = f"{f_name}.delta"
    
    # We encapsulate the data into a list if it's not already a list.
    if not isinstance(df, list):
        df = [df]
    if not isinstance(file_name, list):
        file_name = [file_name]
        
    pool = ThreadPool(threads)
    
    def gen_args(kwds, file):
        kwds["file_name"] = file
        return kwds
    
    kwds = list(map(lambda file: gen_args(kwds, file), file_name))
    return list(map(lambda kwd: pool.apply_async(write_delta_f, kwds=kwd).get(), kwds))


## ORC
# Read
def read_orc(file_name: Union[str, List[str]], 
             path: str, 
             source: str = "dbfs", 
             as_type: str = PANDAS, 
             engine: str = KOALAS, 
             threads: int = 2) -> Union[Union[PandasDataFrame, PysparkDataFrame, KoalasDataFrame], List[Union[PandasDataFrame, PysparkDataFrame, KoalasDataFrame]]]:
    """
    Read ORC file(s) from DBFS or local file system and return as a Pandas or Koalas DataFrame.

    :param file_name: Name of the ORC file(s) to read. A string for a single file or a list of strings for multiple files.
    :param path: Path of the directory where the file(s) is located.
    :param source: Source of the file system where the file(s) is located. Default is "dbfs".
    :param as_type: Type of the DataFrame to be returned. Either "pandas" or "koalas". Default is "koalas".
    :param engine: Engine to be used for reading ORC file. Either "koalas", "pyspark", or "pandas". Default is "koalas".
    :param threads: Number of threads to be used for reading multiple files in parallel. Default is 2.

    :return: A DataFrame.
    :rtype: Union[Union[PandasDataFrame, PysparkDataFrame, KoalasDataFrame], List[Union[PandasDataFrame, PysparkDataFrame, KoalasDataFrame]]]
    """
    def read_orc_f(file_name: str, path: str, source: str = "dbfs", as_type: str = KOALAS, engine: str = KOALAS):
        if engine == KOALAS:
            return data_convert(koalas_read_orc(f"{source}:{path}/{file_name}"), as_type=as_type)
        elif engine == PYSPARK:
            return data_convert(spark.read.format("orc").load(f"{source}:{path}/{file_name}"), as_type = as_type)
        elif engine == PANDAS:
            # Remove ps.to_pandas() when data_convert can support ps convertions.
            return data_convert(ps_read_orc(f"{source}:{path}/{file_name}").to_pandas(), as_type = as_type)
            # Current pyarrow version has a bug, pyspark.pandas is recommended.
            # return data_convert(pandas_read_orc(f"/{source}/{path}/{file_name}"), as_type = as_type)
    if isinstance(file_name, list):
        for file in file_name:
            if not file.endswith(".orc"):
                file = f"{file}.orc"
        pool = ThreadPool(threads)
        return list(map(lambda file: pool.apply_async(read_orc_f, kwds={"file_name": file, "path": path, "source": source, "as_type": as_type, "engine": engine}).get(), file_name))
    elif isinstance(file_name, str):
        if not file_name.endswith(".orc"):
            file_name = f"{file_name}.orc"
        return read_orc_f(file_name, path, source, as_type, engine)


# Write
def write_orc(df: Union[PandasDataFrame, PysparkDataFrame, KoalasDataFrame, List[Union[PandasDataFrame, PysparkDataFrame, KoalasDataFrame]]], 
              file_name: Union[str, List[str]], 
              path: str, 
              source: str = "dbfs", 
              mode: str = "overwrite", 
              num_files: int = 1, 
              threads: int = 2) -> List[str]:
    """
    Writes a PySpark DataFrame as an ORC file.

    :param df: The DataFrame to write.
    :type df: Union[pandas.DataFrame, pyspark.sql.DataFrame, databricks.koalas.DataFrame, List[Union[pandas.DataFrame, pyspark.sql.DataFrame, databricks.koalas.DataFrame]]]

    :param file_name: The name of the file to create. The '.orc' extension will be added if it's not present.
    :type file_name: str
    
    :param path: The path to write the file to.
    :type path: str
    
    :param source: The source of the file. Defaults to 'dbfs'.
    :type source: str

    :param mode: The write mode. Defaults to 'overwrite'.
    :type mode: str

    :param num_files: The number of files to write. Defaults to 1.
    :type num_files: int

    :param threads: The number of threads to use for parallelization. Defaults to 2.
    :type threads: int


    :return: A list of the names of the written files.
    :rtype: Union[None, List[None]]
    """
    kwds = locals()
    del kwds['threads']
    def write_orc_f(df, file_name: str, path: str, source: str = "dbfs", mode = "overwrite", num_files: int = 1):
        _temp = file_name#f"temp_{file_name.split('.')[0]}"
        data_convert(df, as_type=KOALAS).to_orc(path=fr'{path}/{_temp}/', mode = mode, num_files = num_files)
        files = [file.name for file in dbutils.fs.ls(f"{path}/{_temp}/") if not file.name.startswith("_")]
        i = 0
        for file in files:
            if len(files)>1:
                f_name = f"{file_name.split('.')[0]}_{str(i)}{file_name.split('.')[1]}"
                i += 1
            else:
                f_name = file_name
            if not file_name.endswith(".orc"):
                file_name = f"{f_name}.orc"
    
    # We encapsulate the data into a list if it's not already a list.
    if not isinstance(df, list):
        df = [df]
    if not isinstance(file_name, list):
        file_name = [file_name]
        
    pool = ThreadPool(threads)
    
    def gen_args(kwds, file):
        kwds["file_name"] = file
        return kwds
    
    kwds = list(map(lambda file: gen_args(kwds, file), file_name))
    return list(map(lambda kwd: pool.apply_async(write_orc_f, kwds=kwd).get(), kwds))

## Avro
# Read
def read_avro(file_name: Union[str, List[str]], 
              path: str, 
              source: str = "dbfs", 
              as_type: str = PANDAS, 
              engine: str = SPARK, 
              threads: int = 2) -> Union[PandasDataFrame, PysparkDataFrame, KoalasDataFrame, List[Union[PandasDataFrame, PysparkDataFrame, KoalasDataFrame]]]:
    """
    Reads Avro files from a given path and returns a DataFrame. If `file_name` is a list, this function will return a list of DataFrames, one for each file.

    :param file_name: The name or list of names of the Avro files to read.
    :param path: The path to the directory containing the Avro file(s).
    :param source: The source of the file system. Default is 'dbfs'.
    :param as_type: The type of the output DataFrame. Default is `ks.DataFrame` (for Koalas). Can also be `pd.DataFrame` (for Pandas).
    :param engine: The processing engine to use. Default is 'pyspark'.
    :param threads: The number of threads to use when reading multiple files. Default is 2.

    :return: A DataFrame if `file_name` is a string, or a list of DataFrames if `file_name` is a list of strings.
    :rtype: Union[PandasDataFrame, PysparkDataFrame, KoalasDataFrame, List[Union[PandasDataFrame, PysparkDataFrame, KoalasDataFrame]]]
    """
    def read_avro_f(file_name: str, path: str, source: str = "dbfs", as_type: str = KOALAS, engine: str = PYSPARK):
        if engine == KOALAS:
            raise NotImplementedError("Current version of Koalas does not support this operation. Use Spark as engine.")
        elif engine == PYSPARK:
            return data_convert(spark.read.format("avro").load(f"{source}:{path}/{file_name}"), as_type = as_type)
        elif engine == PANDAS:
            raise NotImplementedError("Current version of Pandas does not support this operation. Use Spark as engine.")
    if isinstance(file_name, list):
        for file in file_name:
            if not file.endswith(".avro"):
                file = f"{file}.avro"
        pool = ThreadPool(threads)
        return list(map(lambda file: pool.apply_async(read_avro_f, kwds={"file_name": file, "path": path, "source": source, "as_type": as_type, "engine": engine}).get(), file_name))
    elif isinstance(file_name, str):
        if not file_name.endswith(".avro"):
            file_name = f"{file}.avro"
        return read_avro_f(file_name, path, source, as_type, engine)
    
# Write
# TODO
@deprecated("Current version not supported.")
def write_avro():
    return

## Cosmos
# Read
def read_sql(table_name: str, 
             database: str, 
             server: str, 
             port: str, 
             user: str, 
             password: str, 
             sql_type: str = "sqlserver", 
             cert: str = ".database.windows.net", 
             as_type: str = KOALAS, 
             engine: str=KOALAS, 
             threads: int = 2) -> Union[PandasDataFrame, PysparkDataFrame, KoalasDataFrame, List[Union[PandasDataFrame, PysparkDataFrame, KoalasDataFrame]]]:
    """
    Read data from a SQL Server database table.

    :param table_name: Name of the table to read.
    :type table_name: str
    :param database: Name of the database.
    :type database: str
    :param server: Name of the server.
    :type server: str
    :param port: Port to connect to the server.
    :type port: str
    :param user: Username for the database.
    :type user: str
    :param password: Password for the database.
    :type password: str
    :param sql_type: Type of SQL Server. Default is "sqlserver".
    :type sql_type: str
    :param cert: Certificate for the database. Default is ".database.windows.net".
    :type cert: str
    :param as_type: Data type to return. Default is KOALAS.
    :type as_type: str
    :param engine: Engine to use for data processing. Default is KOALAS.
    :type engine: str
    :param threads: Number of threads to use for parallel processing. Default is 2.
    :type threads: int

    :return: DataFrame containing data from the table.
    :rtype: Union[PandasDataFrame, PysparkDataFrame, KoalasDataFrame, List[Union[PandasDataFrame, PysparkDataFrame, KoalasDataFrame]]]
    """
    def read_sql_f(table_name: str, database: str, server: str, port: str, user: str, password: str, sql_type: str = "sqlserver", cert: str = ".database.windows.net", as_type: str = KOALAS, engine = KOALAS):
        if engine == KOALAS:
            return data_convert(koalas_read_sql(table_name, con=f"jdbc:{sql_type}://{server}{cert}:{port};database={database};user={user}@{server};password={password}"), as_type=as_type)
        elif engine == PYSPARK:
            raise NotImplementedError("Current version of Hivecode doesn't support Spark connection for SQL.")
        elif engine == PANDAS:
            # Remove to_pandas() after data_convert supports ps transformations.
            return data_convert(read_sql_query(f"SELECT * FROM {table_name}", f"jdbc:{sql_type}://{server}{cert}:{port};database={database};user={user}@{server};password={password}").to_pandas(), as_type = as_type)
    if isinstance(table_name, list):
        pool = ThreadPool(threads)
        return list(map(lambda file: pool.apply_async(read_sql_f, kwds={"table_name": file, "database": database, "server": server, "port": port, "user": user, "password": password, "sql_type": sql_type, "cert": cert, "as_type": as_type, "engine": engine}).get(), table_name))
    elif isinstance(table_name, str):
        return read_sql_f(table_name, database, server, port, user, password, sql_type, cert, as_type, engine)
    
# Write
def write_sql(df: Union[PandasDataFrame, PysparkDataFrame, KoalasDataFrame, List[Union[PandasDataFrame, PysparkDataFrame, KoalasDataFrame]]], 
              table_name: str, 
              database: str, 
              server: str, 
              port: str, 
              user: str, 
              password: str, 
              sql_type: str = "sqlserver", 
              cert: str = ".database.windows.net", 
              engine: str = "pyspark", 
              threads: int = 2) -> None:
    """
    Writes a DataFrame to SQL Server.

    :param df: A DataFrame to be written to SQL Server.
    :type df: Any
    :param table_name: The name of the table to be written to.
    :type table_name: str
    :param database: The name of the database to write to.
    :type database: str
    :param server: The name of the server to write to.
    :type server: str
    :param port: The port number to use for the connection.
    :type port: str
    :param user: The username for the connection.
    :type user: str
    :param password: The password for the connection.
    :type password: str
    :param sql_type: The type of SQL Server to use. Default is "sqlserver".
    :type sql_type: str
    :param cert: The certificate for the connection. Default is ".database.windows.net".
    :type cert: str
    :param engine: The engine to use for writing to SQL Server. Default is "pyspark".
    :type engine: str
    :param threads: The number of threads to use for writing. Default is 2.
    :type threads: int
    :raises NotImplementedError: If the engine parameter is set to "koalas" or "pandas".
    :return: None
    :rtype: None
    """
    kwds = locals()
    del kwds['threads']
    def write_sql_f(df, table_name: str, database: str, server: str, port: str, user: str, password: str, sql_type: str = "sqlserver", cert: str = ".database.windows.net", mode = "append", engine = PYSPARK):
        if engine == KOALAS:
            raise NotImplementedError("Current version of Hivecode doesn't support writing to a sqldb using Koalas.")
        elif engine == PYSPARK:
            jdbcUrl = f"jdbc:sqlserver://{server}.database.windows.net:{port};database={database};"
            connectionProperties = {
              "user" : user,
              "password" : password,
              "driver" : "com.microsoft.sqlserver.jdbc.SQLServerDriver"
            }

            try:
                data_convert(df, as_type=PYSPARK).write.jdbc(url=jdbcUrl, table=table_name, properties=connectionProperties, mode = mode)
            except Exception as e:
                if "com.microsoft.sqlserver.jdbc.SQLServerException: The specified schema name" in str(e.java_exception):
                    # We create a new db and try again.
                    raise Exception("Table not created.")
                elif "No suitable driver" == str(e.java_exception):
                    raise Exception("Driver not installed in cluster. Please install it using Maven Central 'com.microsoft.sqlserver.msi:msi-mssql-jdbc:2.0.3'")
                raise
        elif engine == PANDAS:
            raise NotImplementedError("Current version of Hivecode doesn't support writing to a sqldb using Pandas.")
    
    # We encapsulate the data into a list if it's not already a list.
    if not isinstance(df, list):
        df = [df]
    if not isinstance(table_name, list):
        table_name = [table_name]
    pool = ThreadPool(threads)
    def gen_args(kwds, table):
        kwds["table_name"] = table
        return kwds
    kwds = list(map(lambda table: gen_args(kwds, table), table_name))
    list(map(lambda kwd: pool.apply_async(write_sql_f, kwds=kwd).get(), kwds))

## Cosmos
# Read
def read_cosmos(endpoint: str, 
                key: str, 
                database: str, 
                container: str, 
                as_type: str = KOALAS, 
                engine: str = PANDAS, 
                threads: int = 2) -> Union[PandasDataFrame, PysparkDataFrame, KoalasDataFrame, List[Union[PandasDataFrame, PysparkDataFrame, KoalasDataFrame]]]:
    """
    Reads data from an Azure Cosmos DB container and returns a DataFrame.

    :param endpoint: The URL of the Cosmos DB account.
    :type endpoint: str
    :param key: The primary or secondary key of the Cosmos DB account.
    :type key: str
    :param database: The name of the database.
    :type database: str
    :param container: The name of the container within the database.
    :type container: str
    :param as_type: The format of the DataFrame that the data is returned in. Default is 'koalas'.
    :type as_type: str
    :param engine: The engine to use for reading the data. Default is 'pandas'.
    :type engine: str
    :param threads: The number of threads to use. Default is 2.
    :type threads: int

    :return: DataFrame, the data from the container in a DataFrame.
    :rtype: Union[PandasDataFrame, PysparkDataFrame, KoalasDataFrame, List[Union[PandasDataFrame, PysparkDataFrame, KoalasDataFrame]]]
    """

    # Azure
    lib_required("azure-cosmos")
    from azure.cosmos import CosmosClient, PartitionKey
    def read_cosmos_f(endpoint: str, key: str, database: str, container: str, as_type: str = KOALAS, engine: str = PANDAS):
        # CLIENT CONNECT
        client    = CosmosClient(endpoint, credential=key)

        # SELECT DB
        db        = client.get_database_client(database)

        # SELECT CONTAINER
        container = db.get_container_client(container)
        if engine == KOALAS:
            raise NotImplementedError("Current version of PyArrow does not support this operation. Use Pandas as engine, transform all complex variables into str type and transform.")
        elif engine == PYSPARK:
            raise NotImplementedError("Current version of PyArrow does not support this operation. Use Pandas as engine, transform all complex variables into str type and transform.")
        elif engine == PANDAS:
            return data_convert(DataFrame(container.read_all_items()), as_type=as_type)
    if isinstance(container, list):
        pool = ThreadPool(threads)
        return list(map(lambda contain: pool.apply_async(read_cosmos_f, kwds={"endpoint": endpoint, "key": key, "database": database, "container": contain, "as_type": as_type, "engine": engine}).get(), container))
    elif isinstance(container, str):
        return read_cosmos_f(container, key, database, container, as_type, engine)


# Write
def write_cosmos(df: Union[PandasDataFrame, PysparkDataFrame, KoalasDataFrame, List[Union[PandasDataFrame, PysparkDataFrame, KoalasDataFrame]]], 
                 endpoint: str, 
                 key: str, 
                 database: str, 
                 container: str, 
                 unique_keys: Optional[str] = None, 
                 id: str = 'id', 
                 threads: int = 2) -> List[Union[PandasDataFrame, PysparkDataFrame, KoalasDataFrame]]:
    """
    Write a pandas DataFrame or a list of pandas DataFrames to Azure Cosmos DB.

    :param df: A pandas DataFrame or a list of pandas DataFrames to write to Cosmos DB.
    :type df: Union[PandasDataFrame, PysparkDataFrame, KoalasDataFrame, List[Union[PandasDataFrame, PysparkDataFrame, KoalasDataFrame]]]
    :param endpoint: The URL of the Cosmos DB account.
    :type endpoint: str
    :param key: The primary or secondary key for the Cosmos DB account.
    :type key: str
    :param database: The name of the Cosmos DB database.
    :type database: str
    :param container: The name of the Cosmos DB container.
    :type container: str
    :param unique_keys: A column in the DataFrame(s) that will be used as the unique key. If None, a unique key is generated for each row. Defaults to None.
    :type unique_keys: Optional[str]
    :param id: The name of the key column in the DataFrame(s) that will be used as the partition key. Defaults to 'id'.
    :type id: str
    :param threads: The number of threads to use. Defaults to 2.
    :type threads: int
    :return: A list of the items that were created in Cosmos DB.
    :rtype: List[Union[PandasDataFrame, PysparkDataFrame, KoalasDataFrame]]
    """
    # Azure
    lib_required("azure-cosmos")
    from azure.cosmos import CosmosClient, PartitionKey
    if not isinstance(df, list):
        df = [df]
        
    char_list = list(digits) + list(ascii_letters) + list(punctuation)
    def generate_unique_keys(length: int = 32) -> str:
        """
        Generate a random unique key.

        Args:
            length (int, optional): The length of the unique key. Defaults to 32.

        Returns:
            str: The generated unique key.
        """
        return ''.join(choice(char_list) for i in range(length))
    if not unique_keys:
        df["__unique_key"] = ""
        df["__unique_key"] = df["__unique_key"].apply(lambda _: generate_unique_keys(32))
    
    # CLIENT CONNECT
    client    = CosmosClient(endpoint, credential=key)

    # SELECT DB
    db        = client.get_database_client(database)
    
    # SELECT CONTAINER
    container = db.get_container_client(container)
    
    pool = ThreadPool(threads)

    def store_data(container: Any, 
                   request_body: Any) -> Any:
        """
        Store data in Cosmos DB.

        Args:
            container (Any): The Cosmos DB container.
            request_body (Any): The data to store in Cosmos DB.

        Returns:
            Any: The result of the create_item() operation.
        """
        return container.create_item(body=request_body)
    
    return list(map(lambda item: pool.apply_async(store_data, kwds={"container": container, "request_body": item}), loads(df.to_json(orient='records'))))


## COMTRADE
# Read
def read_comtrade(file_name_cfg: str, 
                  path_cfg: str, 
                  file_name_dat: str, 
                  path_dat: str, 
                  source: str = "dbfs", 
                  return_mode: str = 'all') -> Union[DataFrame, Tuple[DataFrame, DataFrame], Tuple[DataFrame, DataFrame, Dict[str, Any]]]:
    """
    Reads a COMTRADE file and returns a pandas DataFrame with analog and/or digital values.

    :param file_name_cfg: Name of the .cfg file.
    :type file_name_cfg: str

    :param path_cfg: Path of the .cfg file.
    :type path_cfg: str
    
    :param file_name_dat: Name of the .dat file.
    :type file_name_dat: str
    
    :param path_dat: Path of the .dat file.
    :type path_dat: str
    
    :param source: Source of the file. Default is "dbfs".
    :type source: str
    
    :param return_mode: Determines what to return. Can be "analog", "digital", "signals", or "all". Default is "all".
    :type return_mode: str

    :return: Depending on the return_mode parameter, it returns either a pandas DataFrame with analog values, a pandas DataFrame with digital values, a tuple of both DataFrames, or a tuple with both DataFrames and a dictionary containing station information.
    :rtype: Union[pandas.DataFrame, Tuple[pandas.DataFrame, pandas.DataFrame], Tuple[pandas.DataFrame, pandas.DataFrame, Dict[str, Any]]]
    """


    # We make sure comtrade is installed, if not we will force install it.
    lib_required('comtrade')

    from comtrade import Comtrade
    # We read the comtrade with all posible combinations of .cfg and .dat
    rec = Comtrade()
    try:
        rec.load(f'/{source}/{path_cfg}/{file_name_cfg.split(".")[0]}.cfg', f'/{source}/{path_dat}/{file_name_dat.split(".")[0]}.dat')
    except:
        try:
            rec.load(f'/{source}/{path_cfg}/{file_name_cfg.split(".")[0]}.cfg', f'/{source}/{path_dat}/{file_name_dat.split(".")[0]}.DAT')
        except:
            try:
                rec.load(f'/{source}/{path_cfg}/{file_name_cfg.split(".")[0]}.CFG', f'/{source}/{path_dat}/{file_name_dat.split(".")[0]}.dat')
            except:
                try:
                    rec.load(f'/{source}/{path_cfg}/{file_name_cfg.split(".")[0]}.CFG', f'/{source}/{path_dat}/{file_name_dat.split(".")[0]}.DAT')
                except:
                    raise

    # We capture analog variables
    df_analog = [DataFrame({"time": rec.time.tolist()})]
    df_analog = [*df_analog, *list(map(lambda i: DataFrame({rec.analog_channel_ids[i]: rec.analog[i].tolist()}), range(len(rec.analog))))]
    df_analog = concat(df_analog, axis = 1)
    time = df_analog.time.copy()
    timestep = df_analog.mode().time
    del(time)
    df_analog.set_index("time", inplace=True)

    # We capture digital variables
    df_digital = [DataFrame({"time": rec.time.tolist()})]
    df_digital = [*df_digital, *list(map(lambda i: DataFrame({rec.status_channel_ids[i]: rec.status[i].tolist()}), range(len(rec.status))))]
    df_digital = concat(df_digital, axis = 1)
    df_digital.set_index("time", inplace=True)
    
    if return_mode == "analog":
        return df_analog
    elif return_mode == "digital":
        return df_digital
    elif return_mode == "signals":
        return df_analog, df_digital
    elif return_mode == "all":
        return df_analog, df_digital, {"station_name": rec.station_name, "frequency": rec.frequency, "start_timestamp": rec.start_timestamp, "trigger_timestamp": rec.trigger_timestamp, "timestep": timestep}
    else:
        raise Exception(f"Parameter return_mode only accepts 'analog', 'digital' or 'all', but '' was given.")


# Write
# TODO
@deprecated("Current version not supported.")
def write_comtrade():
    return


### Utility
## File Transfer
@lru_cache(maxsize=None)
def createSSHClient(server: str, 
                    port: str, 
                    user: str, 
                    password: str) -> SSHClient:
    """
    Creates a new SSHClient object and connects to the given server using the provided credentials.

    :param server: The IP address or hostname of the server to connect to.
    :param port: The port number to use for the SSH connection.
    :param user: The username to use for the SSH connection.
    :param password: The password to use for the SSH connection.
    :return: An SSHClient object representing the SSH connection to the server.
    """
    
    # create a new SSHClient object
    client = SSHClient()

    # load the system host keys
    client.load_system_host_keys()

    # set the missing host key policy to automatically add new hosts
    client.set_missing_host_key_policy(AutoAddPolicy())

    # connect to the server using the provided credentials
    client.connect(server, port, user, password)

    # return the SSHClient object
    return client


def transfer_file(file_name: str, 
                  path: str, 
                  fs_path: str = "/FileStore/tables/", 
                  server: Optional[str] = None, 
                  port: Optional[str] = None, 
                  user: Optional[str] = None, 
                  password: Optional[str] = None) -> None:
    """
    Transfers a file from a remote server to the local filesystem.

    :param file_name: The name of the file to transfer.
    :param path: The path to the file on the remote server.
    :param fs_path: The path to the local filesystem directory where the file should be saved.
    :param server: The IP address or hostname of the remote server. If not provided, the file will be transferred locally.
    :param port: The port number to use for the SSH connection. Defaults to 22 if not provided.
    :param user: The username to use for the SSH connection. Defaults to the current user if not provided.
    :param password: The password to use for the SSH connection. If not provided, the user will be prompted for their password.
    """
    lib_required('scp')
    
    # import the required modules
    from scp import SCPClient
    
    # create an SSH connection to the remote server
    if server is not None:
        ssh = createSSHClient(server, port, user, password)
    else:
        ssh = None

    # create an SCP connection using the SSH connection (if available)
    with SCPClient(ssh.get_transport() if ssh else None) as scp:

        # transfer the file to the local filesystem
        scp.get(rf"{path}/{file_name}", fs_path)


### Classes
## Read
class Reader:
    """
    The Reader class is a utility class that provides methods for reading data from various file formats
    based on the environment being used. It is designed to be used in a PySpark environment, with the
    ability to read data from Azure Databricks and local file systems.
    
    :param adb_env: A boolean flag indicating whether the class is being used in an Azure Databricks
                    environment or a local environment. Default is False.
    :type adb_env: bool

    :ivar ADB_ENV: A boolean flag indicating whether the class is being used in an Azure Databricks
                   environment or a local environment.
    :vartype ADB_ENV: bool
    :ivar env: A string indicating the environment being used, either 'adb' or 'local'.
    :vartype env: str
    """
    
    def __init__(self, adb_env: bool = False):
        self.ADB_ENV = adb_env
        self.env = 'adb' if adb_env else 'local'
        
        
    def read(self, *args, **kwargs) -> Union[PandasDataFrame, PysparkDataFrame, KoalasDataFrame, List[Union[PandasDataFrame, PysparkDataFrame, KoalasDataFrame]]]:
        """
        Reads data from different file formats or sources based on the file extension or provided source type.
        
        :param args: Optional positional arguments containing the file path or table name to read from.
        :param kwargs: Optional keyword arguments containing the file path or table name to read from or the source type.
        :keyword file_name: The path of the file to read.
        :type file_name: str
        :keyword table_name: The name of the table to read.
        :type table_name: str
        :keyword source_type: The source type to read from. Can be one of ['csv', 'parquet', 'json', 'excel', 'delta', 'orc', 'avro', 'cosmos', 'sql'].
        :type source_type: str
        
        :returns: A PySpark DataFrame containing the data from the source.
        :rtype: pyspark.sql.DataFrame or None
        :raises Exception: If the file format is not supported by the read() function.
        """
        try:
            name = args[0]
        except:
            name = kwargs.get('file_name') or kwargs.get('table_name')
        
        try:
            file_format = name.split('.')[-1].lower()
        except:
            file_format = None
            
        if kwargs.get('table_name') or not file_format:
            # This case defaults to delta table.
            return self.read_table(*args, **kwargs)
        elif file_format == 'csv':
            return self.read_csv(*args, **kwargs)
        elif file_format == 'parquet':
            return self.read_parquet(*args, **kwargs)
        elif file_format == 'json':
            return self.read_json(*args, **kwargs)
        elif file_format == 'excel':
            return self.read_excel(*args, **kwargs)
        elif file_format == 'delta':
            return self.read_delta(*args, **kwargs)
        elif file_format == 'orc':
            return self.read_orc(*args, **kwargs)
        elif file_format == 'avro':
            return self.read_avro(*args, **kwargs)
        elif kwargs.get('source_type') == 'cosmos':
            return self.read_cosmos(*args, **kwargs)
        elif kwargs.get('source_type') == 'sql':
            return self.read_sql(*args, **kwargs)
        else:
            raise Exception('File format not supported by the read() function, try using a specific method.')
        
        
    def delta_table(self, *args, **kwargs) -> Union[PandasDataFrame, PysparkDataFrame, KoalasDataFrame, List[Union[PandasDataFrame, PysparkDataFrame, KoalasDataFrame]]]:
        """
        Reads data from a delta table.
        
        :param args: Optional positional arguments containing the table name to read from.
        :param kwargs: Optional keyword arguments containing the table name to read from.
        :keyword table_name: The name of the table to read.
        :type table_name: str
        
        :returns: A PySpark DataFrame containing the data from the table.
        :rtype: pyspark.sql.DataFrame or None
        """
        if self.ADB_ENV:
            return read_table(self, *args, **kwargs)

    def csv(self, *args, **kwargs) -> Union[PandasDataFrame, PysparkDataFrame, KoalasDataFrame, List[Union[PandasDataFrame, PysparkDataFrame, KoalasDataFrame]]]:
        """
        Reads a CSV file from the specified path, if the environment is ADB.

        :param args: Variable length argument list.
        :param kwargs: Arbitrary keyword arguments.
        :return: None
        """
        if self.ADB_ENV:
            return read_csv(self, *args, **kwargs)
    
    def parquet(self, *args, **kwargs) -> Union[PandasDataFrame, PysparkDataFrame, KoalasDataFrame, List[Union[PandasDataFrame, PysparkDataFrame, KoalasDataFrame]]]:
        """
        Reads a Parquet file from the specified path, if the environment is ADB.

        :param args: Variable length argument list.
        :param kwargs: Arbitrary keyword arguments.
        :return: None
        """
        if self.ADB_ENV:
            return read_parquet(self, *args, **kwargs)

    def json(self, *args, **kwargs) -> Union[PandasDataFrame, PysparkDataFrame, KoalasDataFrame, List[Union[PandasDataFrame, PysparkDataFrame, KoalasDataFrame]]]:
        """
        Reads a JSON file from the specified path, if the environment is ADB.

        :param args: Variable length argument list.
        :param kwargs: Arbitrary keyword arguments.
        :return: None
        """
        if self.ADB_ENV:
            return read_json(self, *args, **kwargs)

    def excel(self, *args, **kwargs) -> Union[PandasDataFrame, PysparkDataFrame, KoalasDataFrame, List[Union[PandasDataFrame, PysparkDataFrame, KoalasDataFrame]]]:
        """
        Reads an Excel file from the specified path, if the environment is ADB.

        :param args: Variable length argument list.
        :param kwargs: Arbitrary keyword arguments.
        :return: None
        """
        if self.ADB_ENV:
            return read_excel(self, *args, **kwargs)

    def delta(self, *args, **kwargs) -> Union[PandasDataFrame, PysparkDataFrame, KoalasDataFrame, List[Union[PandasDataFrame, PysparkDataFrame, KoalasDataFrame]]]:
        """
        Reads a Delta file from the specified path, if the environment is ADB.

        :param args: Variable length argument list.
        :param kwargs: Arbitrary keyword arguments.
        :return: None
        """
        if self.ADB_ENV:
            return read_delta(self, *args, **kwargs)

    def orc(self, *args, **kwargs) -> Union[PandasDataFrame, PysparkDataFrame, KoalasDataFrame, List[Union[PandasDataFrame, PysparkDataFrame, KoalasDataFrame]]]:
        """
        Reads an Orc file from the specified path, if the environment is ADB.

        :param args: Variable length argument list.
        :param kwargs: Arbitrary keyword arguments.
        :return: None
        """
        if self.ADB_ENV:
            return read_orc(self, *args, **kwargs)

    def avro(self, *args, **kwargs) -> Union[PandasDataFrame, PysparkDataFrame, KoalasDataFrame, List[Union[PandasDataFrame, PysparkDataFrame, KoalasDataFrame]]]:
        """
        Reads an Avro file from the specified path, if the environment is ADB.

        :param args: Variable length argument list.
        :param kwargs: Arbitrary keyword arguments.
        :return: None
        """
        if self.ADB_ENV:
            return read_avro(self, *args, **kwargs)

    def cosmos(self, *args, **kwargs) -> Union[PandasDataFrame, PysparkDataFrame, KoalasDataFrame, List[Union[PandasDataFrame, PysparkDataFrame, KoalasDataFrame]]]:
        """
        Reads data from Azure Cosmos DB, if the environment is ADB.

        :param args: Variable length argument list.
        :param kwargs: Arbitrary keyword arguments.
        :return: None
        """
        if self.ADB_ENV:
            return read_cosmos(self, *args, **kwargs)

    def sql(self, *args, **kwargs) -> Union[PandasDataFrame, PysparkDataFrame, KoalasDataFrame, List[Union[PandasDataFrame, PysparkDataFrame, KoalasDataFrame]]]:
        """
        Reads data from an SQL Server database, if the environment is ADB.

        :param args: Variable length argument list.
        :param kwargs: Arbitrary keyword arguments.
        :return: None
        """
        if self.ADB_ENV:
            return read_sql(self, *args, **kwargs)
    

## Write
class Writer:
    """
    The Writer class is a utility class that provides methods for writing data to various file formats
    based on the environment being used. It is designed to be used in a PySpark environment, with the
    ability to write data to Azure Databricks and local file systems.

    :param adb_env: A boolean flag indicating whether the class is being used in an Azure Databricks
                    environment or a local environment. Default is False.
    :type adb_env: bool

    :ivar ADB_ENV: A boolean flag indicating whether the class is being used in an Azure Databricks
                   environment or a local environment.
    :vartype ADB_ENV: bool
    :ivar env: A string indicating the environment being used, either 'adb' or 'local'.
    :vartype env: str
    """

    def __init__(self, adb_env: bool = False):
        self.ADB_ENV: bool = adb_env
        self.env: str = 'adb' if adb_env else 'local'

    def delta_table(self, table_name: str, df, mode: str = 'overwrite') -> None:
        """
        Writes a DataFrame to a Delta Lake table.

        :param table_name: The name of the Delta Lake table.
        :type table_name: str
        :param df: The PySpark DataFrame to write to the Delta Lake table.
        :param mode: Specifies the behavior of the write operation when the table already exists.
                     Options are 'overwrite', 'append', 'ignore', and 'error'. Default is 'overwrite'.
        :type mode: str
        """
        if self.ADB_ENV:
            write_table(table_name, df, mode)

    def csv(self, file_path: str, df, header: bool = True, mode: str = 'overwrite') -> None:
        """
        Writes a DataFrame to a CSV file.

        :param file_path: The path to the CSV file.
        :type file_path: str
        :param df: The PySpark DataFrame to write to the CSV file.
        :param header: Specifies whether to include the header in the CSV file. Default is True.
        :type header: bool
        :param mode: Specifies the behavior of the write operation when the file already exists.
                     Options are 'overwrite', 'append', 'ignore', and 'error'. Default is 'overwrite'.
        :type mode: str
        """
        if self.ADB_ENV:
            write_csv(file_path, df, header, mode)

    def parquet(self, file_path: str, df, mode: str = 'overwrite') -> None:
        """
        Writes a DataFrame to a Parquet file.

        :param file_path: The path to the Parquet file.
        :type file_path: str
        :param df: The PySpark DataFrame to write to the Parquet file.
        :param mode: Specifies the behavior of the write operation when the file already exists.
                     Options are 'overwrite', 'append', 'ignore', and 'error'. Default is 'overwrite'.
        :type mode: str
        """
        if self.ADB_ENV:
            write_parquet(file_path, df, mode)

    def json(self, file_path: str, df, mode: str = 'overwrite') -> None:
        """
        Writes a DataFrame to a JSON file.

        :param file_path: The path to the JSON file.
        :type file_path: str
        :param df: The PySpark DataFrame to write to the JSON file.
        :param mode: Specifies the behavior of the write operation when the file already exists.
                    Options are 'overwrite', 'append', 'ignore', and 'error'. Default is 'overwrite'.
        :type mode: str
        """
        if self.ADB_ENV:
            write_json(file_path, df, mode)

    def excel(self, file_path: str, df, header: bool = True, mode: str = 'overwrite') -> None:
        """
        Writes a DataFrame to an Excel file.

        :param file_path: The path to the Excel file.
        :type file_path: str
        :param df: The PySpark DataFrame to write to the Excel file.
        :param header: Specifies whether to include the header in the Excel file. Default is True.
        :type header: bool
        :param mode: Specifies the behavior of the write operation when the file already exists.
                    Options are 'overwrite', 'append', 'ignore', and 'error'. Default is 'overwrite'.
        :type mode: str
        """
        if self.ADB_ENV:
            write_excel(file_path, df, header, mode)

    def delta(self, file_path: str, df, mode: str = 'overwrite') -> None:
        """
        Writes a DataFrame to a Delta Lake table.

        :param file_path: The path to the Delta Lake table.
        :type file_path: str
        :param df: The PySpark DataFrame to write to the Delta Lake table.
        :param mode: Specifies the behavior of the write operation when the table already exists.
                    Options are 'overwrite', 'append', 'ignore', and 'error'. Default is 'overwrite'.
        :type mode: str
        """
        if self.ADB_ENV:
            write_delta(file_path, df, mode)

    def orc(self, file_path: str, df, mode: str = 'overwrite') -> None:
        """
        Writes a DataFrame to an ORC file.

        :param file_path: The path to the ORC file.
        :type file_path: str
        :param df: The PySpark DataFrame to write to the ORC file.
        :param mode: Specifies the behavior of the write operation when the file already exists.
                    Options are 'overwrite', 'append', 'ignore', and 'error'. Default is 'overwrite'.
        :type mode: str
        """
        if self.ADB_ENV:
            write_orc(file_path, df, mode)

    def avro(self, file_path: str, df, mode: str = 'overwrite') -> None:
        """
        Writes a DataFrame to an Avro file.

        :param file_path: The path to the Avro file.
        :type file_path: str
        :param df: The PySpark DataFrame to write to the Avro file.
        :param mode: Specifies the behavior of the write operation when the file already exists.
                    Options are 'overwrite', 'append', 'ignore', and 'error'. Default is 'overwrite'.
        :type mode: str
        """
        if self.ADB_ENV:
            write_avro(file_path, df, mode)

    def cosmos(self, database: str, container: str, df, connection_string: str) -> None:
        """
        Writes a DataFrame to a Cosmos DB container.

        :param database: The name of the Cosmos DB database.
        :type database: str
        :param container: The name of the Cosmos DB container.
        :type container: str
        :param df: The PySpark DataFrame to write to the Cosmos DB container.
        :param connection_string: The connection string for the Cosmos DB account.
        :type connection_string: str
        """
        if self.ADB_ENV:
            write_cosmos(database, container, df, connection_string)

    def sql(self, database: str, table_name: str, df, mode: str = 'overwrite',
            connection_string: str = None, url: str = None, properties: dict = None) -> None:
        """
        Writes a DataFrame to a SQL database table.

        :param database: The name of the SQL database.
        :type database: str
        :param table_name: The name of the SQL table.
        :type table_name: str
        :param df: The PySpark DataFrame to write to the SQL table.
        :param mode: Specifies the behavior of the write operation when the table already exists.
                     Options are 'overwrite', 'append', 'ignore', and 'error'. Default is 'overwrite'.
        :type mode: str
        :param connection_string: The connection string for the SQL database. Either the connection
                                  string or the URL and properties must be provided.
        :type connection_string: str
        :param url: The URL of the SQL server. Either the connection string or the URL and properties
                    must be provided.
        :type url: str
        :param properties: A dictionary of properties to set for the SQL connection. Either the
                            connection string or the URL and properties must be provided.
        :type properties: dict
        """
        if self.ADB_ENV:
            write_sql(database, table_name, df, mode, connection_string, url, properties)
