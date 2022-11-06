from .functions import get_spark, get_dbutils, data_convert
from .constants import PANDAS_TYPES, PYSPARK_TYPES, KOALAS_TYPES, PANDAS_ON_SPARK_TYPES, PANDAS, KOALAS, SPARK, PYSPARK, PANDAS_ON_SPARK, IN_PANDAS_ON_SPARK

# Azure
from azure.cosmos import CosmosClient, PartitionKey

# Data
from databricks.koalas import read_excel as koalas_read_excel, read_delta as koalas_read_delta, read_table as koalas_read_table,\
read_json as koalas_read_json, read_csv as koalas_read_csv, read_parquet as koalas_read_parquet, read_orc as koalas_read_orc,\
read_sql as koalas_read_sql
from pyspark.pandas import read_delta as ps_read_delta, read_table as ps_read_table, read_orc as ps_read_orc, \
read_sql_query
from pandas import DataFrame, ExcelWriter, read_csv as pandas_read_csv, read_json as pandas_read_json, read_parquet as pandas_read_parquet,\
read_excel as pandas_read_excel, read_orc as pandas_read_orc
from os import system, path as ospath
from py4j.protocol import Py4JJavaError
from native.decorators import deprecated

# SSH, SCP
from paramiko import SSHClient, AutoAddPolicy
from scp import SCPClient

from functools import lru_cache

spark   = get_spark()
dbutils = get_dbutils()

from multiprocessing.pool import ThreadPool
from threading import Thread
from queue import Queue
from typing import Union, List

from string import digits, ascii_letters, punctuation
from random import choice
from json import loads

# TABLE
def read_table(table_name: str, db: str = "default", as_type: str = KOALAS, index: Union[str, List[str], None] = None, engine: str = KOALAS, threads: int = 2):
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


def write_table(df, table_name, db: str = "default", delta_path = "/FileStore/tables/", mode: str = "overwrite", partitions: Union[str, List[str], None] = None, index: Union[str, List[str], None] = None, threads: int = 2):
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

# CSV
def read_csv(file_name: str, path: str, sep: str = ",", header: bool = 0, source: str = "dbfs", as_type: str = KOALAS, engine: str = KOALAS, threads: int = 2):
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

def write_csv(df, file_name: str, path: str, sep: str = ',', nas: str = '', header: bool = True, mode: str = 'overwrite', num_files: int = 1, threads: int = 2):
    kwds = locals()
    del kwds['threads']
    def write_csv_f(df, file_name: str, path: str, sep: str = ',', nas: str = '', header: bool = True, mode: str = 'overwrite', num_files: int = 1):
        _temp = f"temp_{file_name.split('.')[0]}"
        data_convert(df, as_type=KOALAS).to_csv(path=fr'{path}/{_temp}/', sep = sep, na_rep = nas, header = header, mode = mode, num_files= num_files)
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

# PARQUET
def read_parquet(file_name: str, path: str, source: str = "dbfs", as_type: str = KOALAS, engine: str = KOALAS, threads: int = 2) -> Union[DataFrame, List[DataFrame]]:
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

def write_parquet(df, file_name: str, path: str, mode: str = "overwrite", num_files: int = 1, threads: int = 2):
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

# JSON
def read_json(file_name: str, path: str, source: str = "dbfs", as_type: str = KOALAS, engine: str = KOALAS, threads: int = 2):
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

def write_json(df, file_name: str, path: str, num_files: int = 1):
    _temp = f"temp_{file_name.split('.')[0]}"
    data_convert(df, as_type=KOALAS).to_json(path=fr'{path}/{_temp}/', num_files=num_files)
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
        dbutils.fs.rm(f"{path}/{_temp}", True)

# EXCEL
def read_excel(file_name: str, path: str, source: str = "dbfs", as_type: str = KOALAS, header: bool = True, engine: str = KOALAS, threads: int = 2):
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
                        
def write_excel(df, file_name: str, path: str, source: str = "dbfs", extension: str = "xlsx", sheet_name = list(), threads: int = 2):
    """
    
    Note: This operation write data into the driver local file system. 
    If not enough storage is given, the operation will fail.
    
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
                        
# DELTA
def read_delta(file_name: str, 
               path: str, 
               source: str = "dbfs", 
               version: int = None, 
               timestamp: str = None, 
               as_type: str = KOALAS,
               engine: str  = PYSPARK,
               threads: int = 2):
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
      
def write_delta(df, file_name: str, path: str, source: str = "dbfs", mode = "overwrite", threads: int = 2):
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

# ORC
def read_orc(file_name: str, path: str, source: str = "dbfs", as_type: str = KOALAS, engine: str = KOALAS, threads: int = 2):
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
    
def write_orc(df, file_name: str, path: str, source: str = "dbfs", mode = "overwrite", num_files: int = 1, threads: int = 2):
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

# Cosmos
def read_cosmos(endpoint: str, key: str, database: str, container: str, as_type: str = KOALAS, engine: str = PANDAS, threads: int = 2):
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


def write_cosmos(df, endpoint: str, key: str, database: str, container: str, unique_keys: str = None, id: str = 'id', threads: int = 2):
    if not isinstance(df, list):
        df = [df]
        
    char_list = list(digits) + list(ascii_letters) + list(punctuation)
    def generate_unique_keys(length: int = 32):
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

    def store_data(container, request_body):
        return container.create_item(body=request_body)
    
    return list(map(lambda item: pool.apply_async(store_data, kwds={"container": container, "request_body": item}), loads(df.to_json(orient='records'))))

# SQL
def read_sql(table_name: str, database: str, server: str, port: str, user: str, password: str, sql_type: str = "sqlserver", cert: str = ".database.windows.net", as_type: str = KOALAS, engine = KOALAS, threads: int = 2):
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
    
def write_sql(df, table_name: str, database: str, server: str, port: str, user: str, password: str, sql_type: str = "sqlserver", cert: str = ".database.windows.net", engine = KOALAS, threads: int = 2):
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


# AVRO
def read_avro(file_name: str, path: str, source: str = "dbfs", as_type: str = KOALAS, engine: str = PYSPARK, threads: int = 2):
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


@deprecated("Current version is not tested, not recommended for use.")
def write_avro():
    return

# File Transfer
@lru_cache(maxsize=None)
def createSSHClient(server: str, port: str, user: str, password: str):
    client = SSHClient()
    client.load_system_host_keys()
    client.set_missing_host_key_policy(AutoAddPolicy())
    client.connect(server, port, user, password)
    return client


def transfer_file(file_name: str, path: str, fs_path: str = "/FileStore/tables/", server: str = None, port: str = None, user: str = None, password: str = None) -> None:
    # BUILD SSH AND SCP CONNECTIONS
    ssh = createSSHClient(server, port, user, password)
    scp = SCPClient(ssh.get_transport())
    
    # PERFORM SCP OPERATION.
    scp.get(rf"{path}/{file_name}", fs_path)