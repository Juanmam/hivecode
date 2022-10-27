from .functions import get_spark, data_convert

from databricks.koalas import read_excel as koalas_read_excel, read_delta as koalas_read_delta, read_table as koalas_read_table,\
read_json as koalas_read_json, read_csv as koalas_read_csv, read_parquet as koalas_read_parquet, read_orc as koalas_read_orc,\
read_sql as koalas_read_sql
from pandas import ExcelWriter
from os import system, path as ospath

spark   = get_spark()

from multiprocessing.pool import ThreadPool
from threading import Thread
from queue import Queue


def read_table(table_name: str, db: str = "default", as_type: str = "spark", threads: int = 2):
    def read_table_f(table_name: str, db: str = "default", as_type: str = "spark"):
            return data_convert(koalas_read_table(f"{db}.{table_name}"), as_type=as_type)
    if isinstance(table_name, list):
        pool = ThreadPool(threads)
        return list(map(lambda table: pool.apply_async(read_table_f, kwds={"table_name": table, "db": db, "as_type": as_type}).get(), table_name))
    elif isinstance(table_name, str):
        return read_table_f(table_name, db, as_type)


def write_table(df, table_name, db: str = "default", delta_path = "/FileStore/tables/", mode: str = "overwrite", threads: int = 2):
    # We pack items in a list to be able to use them in he threads process.
    if not isinstance(df, list):
        df = [df]
    if not isinstance(table_name, list):
        table_name = [table_name]
        
    # This is the normal definition that will create a single table.
    def write_table_f(df, table_name, db: str = "default", delta_path = "/FileStore/tables/", mode: str = "overwrite"):
        # Write .delta file in datalake
        delta_path = f'{delta_path}/{db}/{table_name}/'.replace('//', '/')
        data_convert(df, as_type="koalas").to_delta(delta_path, mode=mode)
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


def read_csv(file_name: str, path: str, source: str = "dbfs", as_type: str = "koalas"):
    return data_convert(koalas_read_csv(f"{source}:{path}/{file_name}"), as_type=as_type)


def write_csv(df, 
           file_name: str, 
           path: str, sep: str = ',', 
           nas: str = '', header: bool = True, 
           mode: str = 'overwrite', 
           num_files: int = 1):
    data_convert(df, as_type="koalas").to_csv(path=fr'{path}/{file_name}', sep = sep, na_rep = nas, header = header, mode = mode, num_files= num_files)


def read_excel(file_name: str, path: str, source: str = "dbfs", as_type: str = "koalas"):
    return data_convert(koalas_read_excel(f"{source}:{path}/{file_name}"), as_type=as_type)


def write_excel(dfs, file_name: str, path: str, source: str = "dbfs", sheet_name = list()):
    """
    
    Note: This operation write data into the driver local file system. 
    If not enough storage is given, the operation will fail.
    
    """
    if not isinstance(dfs, list):
        dfs = [dfs]
    
    sheet_name = sheet_name + [f'Sheet_{i}' for i in range(1, len(dfs) - len(sheet_name) + 1)]
    sheet_pos = 0
    
    try:
        with ExcelWriter(fr"{file_name}") as writer:
            for df in dfs:
                data_convert(df, as_type = "koalas").to_excel(writer, sheet_name=sheet_name[sheet_pos])
                sheet_pos = sheet_pos + 1

        system(fr"mv /databricks/driver/{file_name} /{source}/{path}/{file_name}")
    except:
        if not ospath.isdir(fr"/databricks/driver/{file_name}"):
            system(fr"rm /databricks/driver/{file_name}")


def read_json(file_name: str, path: str, source: str = "dbfs", as_type: str = "koalas"):
    return data_convert(koalas_read_json(f"{source}:{path}/{file_name}"), as_type=as_type)


def write_json(df, file_name: str, path: str, num_files: int = 1):
    data_convert(df, as_type="koalas").to_json(path=fr'{path}/{file_name}', num_files=num_files)


def read_parquet(file_name: str, path: str, source: str = "dbfs", as_type: str = "koalas"):
    return data_convert(koalas_read_parquet(f"{source}:{path}/{file_name}"), as_type=as_type)


def write_parquet(df, file_name: str, path: str, mode: str = "overwrite"):
    data_convert(df, as_type="koalas").to_parquet(path=fr'{path}/{file_name}', mode = mode)


def read_delta(file_name: str, 
               path: str, 
               source: str = "dbfs", 
               version: int = None, 
               timestamp: str = None, 
               as_type: str = "koalas"):
    return data_convert(koalas_read_delta(f"{source}:{path}/{file_name}", version=version, timestamp=timestamp), as_type=as_type)


def write_delta(df, file_name: str, delta_path: str, mode = "overwrite"):
    delta_path = f'{delta_path}/{file_name}/'.replace('//', '/')
    data_convert(df, as_type="koalas").to_delta(delta_path, mode=mode)


def read_orc(file_name: str, path: str, source: str = "dbfs", as_type: str = "koalas"):
    return data_convert(koalas_read_orc(f"{source}:{path}/{file_name}"), as_type=as_type)


def write_orc(df, file_name: str, path: str, mode: str = "overwrite"):
    data_convert(df, as_type="koalas").to_orc(path=fr'{path}/{file_name}', mode = mode)


def read_sql(table_name: str, db: str, sql_type: str = "sqlite", as_type: str = "koalas"):
    return data_convert(koalas_read_sql(table_name, con=f"jdbc:{sql_type}:{db}"), as_type=as_type)


def write_sql(df, table_name: str, db: str, sql_type: str = "sqlite", mode: str = "append"):
    data_convert(df, as_type="koalas").to_spark_io(format="jdbc", mode=mode, dbtable=table_name, url=f"jdbc:{sql_type}:{db}")
