"""
This module is design to include pre-build functionalities for Azure that helps with data governance.

- Load Control: Keep track of what files you have, who owns them, how often you have to load them and more, keeping all information related to data manipulation centralized.
- Data Quality: Build a solid framework to validate you data integrity, by creating a repository, applying rules and visualizing the current state of your data.
"""

##### IMPORTS #####
from hivecore.exceptions import StrategyNotDefinedError
from hivecore.patterns import ConcreteStrategy, Context
from hiveadb.functions import spark, dbutils

from re import search
from itertools import compress
from pandas import DataFrame
from typing import List
from re import search
from pandas import DataFrame
from pandas import DataFrame as PandasDataFrame
from typing import List
from pandas import DataFrame as PandasDataFrame
from pyspark.sql import DataFrame as SparkDataFrame
from pyspark.sql.functions import col
import logging
from pandas import read_csv as pandas_read_csv, read_excel as pandas_read_excel, read_parquet as pandas_read_parquet
from hivecore.patterns import ConcreteStrategy, Context

@Context
class DataReader:

    def __init__(self, strategy = 'spark'):

        if strategy == 'spark':

            self.set_strategy(SparkReader())

        elif strategy == 'pandas':

            self.set_strategy(PandasReader())

        else:

            raise NotImplementedError("Unsupported reading strategy.")

 

    @staticmethod

    def format_map(format: str):

        formats = {

            "xlsx": "excel",

        }

        return formats.get(format, format)

 

@ConcreteStrategy('excel', custom_strategy='DataReader')
class PandasReader:

    """

    A concrete strategy class for reading data using various formats with pandas.

 

    This class provides methods to read data from different file formats such as Excel, CSV, Parquet,

    and Delta using the pandas library.

   

    :methods:

        - excel(path: str, header: bool = True, sheet_name: str = None, **params) -> DataFrame

        - csv(path: str, delimiter: str = ',', encoding: str = 'windows-1252', verbose: bool = False, **params) -> DataFrame

        - parquet(path: str, **params) -> DataFrame

        - delta(path: str, **params) -> DataFrame

    """

 

    def __init__(self):

        pass

 

    def excel(self, path: str, header: bool = True, sheet_name: str = None, **params):

        """

            Read data from an Excel file using pandas.

 

            :param path: The path to the Excel file.

            :type path: str

            :param header: Whether the Excel file has headers.

            :type header: bool

            :param sheet_name: The name of the sheet to read data from.

            :type sheet_name: str

            :param params: Additional parameters for pandas_read_excel.

            :type params: dict

            :return: The DataFrame containing the data from the Excel file.

            :rtype: pandas.DataFrame

        """

        path = self._normalize_path(path)

 

        if isinstance(header, bool):

            if header:

                header = self._find_header_start_row(path, sheet_name)

            else:

                header = None

 

        if isinstance(header, dict):

            result = dict()

            for sheet, row_start in header.items():

                result[sheet] = pandas_read_excel(path, header = row_start)

            return result

        else:

            return pandas_read_excel(path, header = header)

   

    def csv(self, path: str, delimeter: str = ',', encoding: str = 'windows-1252', verbose: bool = False, **params):

        """

            Read data from a CSV file using pandas.

 

            :param path: The path to the CSV file.

            :type path: str

            :param delimiter: The delimiter used in the CSV file.

            :type delimiter: str

            :param encoding: The character encoding of the CSV file.

            :type encoding: str

            :param verbose: Whether to display warnings for bad lines.

            :type verbose: bool

            :param params: Additional parameters for pandas_read_csv.

            :type params: dict

            :return: The DataFrame containing the data from the CSV file.

            :rtype: pandas.DataFrame

        """

        path = self._normalize_path(path)

 

        return pandas_read_csv(path, delimiter= delimeter, on_bad_lines = 'warn' if verbose else 'skip')

   

    def parquet(self, path, **params):

        """

            Read data from a Parquet file using pandas.

 

            :param path: The path to the Parquet file.

            :type path: str

            :param params: Additional parameters for pandas_read_parquet.

            :type params: dict

            :return: The DataFrame containing the data from the Parquet file.

            :rtype: pandas.DataFrame

        """

        path = self._normalize_path(path)

 

        return pandas_read_parquet(path)

   

    def delta(self, path: str, **params):

        """

            Read data from a Delta table using Spark and convert to pandas DataFrame.

 

            :param path: The path to the Delta table.

            :type path: str

            :param params: Additional parameters for Spark DataFrameReader.

            :type params: dict

            :return: The DataFrame containing the data from the Delta table.

            :rtype: pandas.DataFrame

        """

        return spark.read.format('delta').load(path).toPandas()

 

    def _normalize_path(self, path: str) -> str:

        """

            Normalize the path for DBFS usage.

 

            :param path: The original path.

            :type path: str

            :return: The normalized path for DBFS.

            :rtype: str

        """

        return f"/dbfs{path.replace('/dbfs', ''). replace('dbfs:','')}"

   

    def _find_header_start_row(self, excel_path: str, sheet_name: str = None):

        """

            Find the starting row of the header in an Excel sheet.

 

            :param excel_path: The path to the Excel file.

            :type excel_path: str

            :param sheet_name: The name of the sheet to analyze.

            :type sheet_name: str

            :return: The row index where the header starts.

            :rtype: int or dict

        """

        dfs = pandas_read_excel(excel_path, sheet_name=sheet_name, header=None)

 

        def analyze_df(df):

            # Calculate the percentage of missing values in each row

            missing_percentages = df.isnull().mean(axis=1)

 

            # Identify the row with the lowest percentage of missing values

            header_start_row = missing_percentages.idxmin()

 

            return header_start_row

 

        if isinstance(dfs, dict):

            if len(dfs.keys()) > 1:

                output = dict()

                for sheet, df in dfs.items():

                    output[sheet] = analyze_df(df)

                return output

            else:

                df = dfs[list(dfs.keys())[0]]

                return analyze_df(df)

        elif isinstance(df, PandasDataFrame):

            return analyze_df(df)

        else:

            raise ValueError("Unexpected data format")
        

@ConcreteStrategy('excel', custom_strategy='DataReader')
class SparkReader:
    """

    A concrete strategy class for reading data using Spark DataFrame API.

 

    This class provides methods to read data from different file formats such as Excel, CSV, Parquet,

    and Delta using the Spark DataFrame API.

 

    :methods:

        - excel(path: str, header: bool = True, sheet_name: str = None, infer: bool = False, encoding: str = 'windows-1252', **params) -> DataFrame

        - csv(path: str, delimiter: str = ',', encoding: str = 'windows-1252', verbose: bool = False, **params) -> DataFrame

        - parquet(path: str, **params) -> DataFrame

        - delta(path: str, **params) -> DataFrame

    """

    def __init__(self):

        pass

 

    def excel(self, path: str, header: bool = True, sheet_name: str = None, infer: bool = False, encoding: str = 'windows-1252', **params):

        """

            Read data from an Excel file using the Spark DataFrame API.

 

            :param str path: The path to the Excel file.

            :param bool header: Whether the Excel file has headers.

            :param str sheet_name: The name of the sheet to read data from.

            :param bool infer: Whether to infer the schema from the data.

            :param str encoding: The character encoding of the Excel file.

            :param params: Additional parameters for Spark Excel reader.

            :return: The DataFrame containing the data from the Excel file.

            :rtype: pyspark.sql.DataFrame

        """

        try:

            if sheet_name:

                # TRY USING com.crealytics.spark.excel

                return spark.read.format("com.crealytics.spark.excel")\

                        .option("header",header)\

                        .option("inferSchema",f"{infer}")\

                        .option("dataAddress", f"'{sheet_name}'!")\

                        .option("maxRowsInMemory", 20)\

                        .option("encoding", f"{encoding}")\

                        .option("treatEmptyValuesAsNulls", "true")\

                        .load(path)

            else:

                return spark.read.format("com.crealytics.spark.excel")\

                        .option("header",header)\

                        .option("inferSchema",f"{infer}")\

                        .option("maxRowsInMemory", 20)\

                        .option("encoding", f"{encoding}")\

                        .option("treatEmptyValuesAsNulls", "true")\

                        .load(path)

        except:

            raise

 

    def csv(self, path: str, delimeter: str = ',', encoding: str = 'windows-1252', verbose: bool = False, **params):

        """

            Read data from a CSV file using the Spark DataFrame API.

 

            :param str path: The path to the CSV file.

            :param str delimiter: The delimiter used in the CSV file.

            :param str encoding: The character encoding of the CSV file.

            :param bool verbose: Whether to display warnings for bad lines.

            :param params: Additional parameters for Spark CSV reader.

            :return: The DataFrame containing the data from the CSV file.

            :rtype: pyspark.sql.DataFrame

        """

        return spark.read.options( header = 'true', sep = delimeter, encoding = encoding).csv(path)

   

    def parquet(self, path: str, **params):

        """

            Read data from a Parquet file using the Spark DataFrame API.

 

            :param str path: The path to the Parquet file.

            :param params: Additional parameters for Spark Parquet reader.

            :return: The DataFrame containing the data from the Parquet file.

            :rtype: pyspark.sql.DataFrame

        """

        return spark.read.format('parquet').load(path)

 

    def delta(self, path: str, **params):

        """

            Read data from a Delta table using the Spark DataFrame API.

 

            :param str path: The path to the Delta table.

            :param params: Additional parameters for Spark Delta reader.

            :return: The DataFrame containing the data from the Delta table.

            :rtype: pyspark.sql.DataFrame

        """

        return spark.read.format('delta').load(path)

 
@Context
class DataWriter:

    def __init__(self, strategy = 'spark'):

        if strategy == 'spark':

            self.set_strategy(SparkWriter())

        elif strategy == 'pandas':

            self.set_strategy(PandasWriter())

        else:

            raise NotImplementedError("Unsupported writing strategy.")

 

    @staticmethod

    def format_map(format: str):

        formats = {

            "xlsx": "excel",

        }

        return formats.get(format, format)

 
@ConcreteStrategy('excel', custom_strategy='DataWriter')
class PandasWriter:

    def __init__(self):

        pass

 

    def excel(self, df: DataFrame, path: str, sheet_name: str = 'Sheet1', **params) -> None:

        """

        Write a DataFrame to an Excel file.

 

        :param df: The DataFrame to be written.

        :type df: pandas.DataFrame

        :param path: The path where the Excel file will be saved.

        :type path: str

        :param sheet_name: The name of the sheet within the Excel file.

        :type sheet_name: str

        :param params: Additional parameters to pass to pandas.DataFrame.to_excel.

        :type params: dict

        """

        path = self._normalize_path(path)

        df.to_excel(path, sheet_name=sheet_name, **params)

 

    def csv(self, df: DataFrame, path: str, delimiter: str = ',', **params) -> None:

        """

        Write a DataFrame to a CSV file.

 

        :param df: The DataFrame to be written.

        :type df: pandas.DataFrame

        :param path: The path where the CSV file will be saved.

        :type path: str

        :param delimiter: The delimiter used between fields in the CSV file.

        :type delimiter: str

        :param params: Additional parameters to pass to pandas.DataFrame.to_csv.

        :type params: dict

        """

        path = self._normalize_path(path)

        df.to_csv(path, sep=delimiter, **params)

 

    def parquet(self, df: DataFrame, path: str, **params) -> None:

        """

        Write a DataFrame to a Parquet file.

 

        :param df: The DataFrame to be written.

        :type df: pandas.DataFrame

        :param path: The path where the Parquet file will be saved.

        :type path: str

        :param params: Additional parameters to pass to pandas.DataFrame.to_parquet.

        :type params: dict

        """

        path = self._normalize_path(path)

        df.to_parquet(path, **params)

 

    def delta(self, df: DataFrame, path: str, **params) -> None:

        """

        Write a DataFrame to a Delta table.

 

        :param df: The DataFrame to be written.

        :type df: pandas.DataFrame

        :param path: The path where the Delta table will be saved.

        :type path: str

        :param params: Additional parameters to pass to Spark DataFrameWriter.

        :type params: dict

        """

        path = self._normalize_path(path)

        delta_writer = self.spark.createDataFrame(df).write.format("delta")

        delta_writer.option("mergeSchema", params.get("mergeSchema", False)).save(path)

 

    def _normalize_path(self, path: str) -> str:

        """

        Normalize the path for DBFS usage.

 

        :param path: The original path.

        :type path: str

        :return: The normalized path for DBFS.

        :rtype: str

        """

        return f"/dbfs{path.replace('/dbfs', '').replace('dbfs:', '')}"

 
@ConcreteStrategy('excel', custom_strategy='DataWriter')
class SparkWriter:

    def __init__(self):

        """

        Initialize a SparkWriter instance.

        """

        pass

 

    def excel(self, df: DataFrame, path: str, mode: str = 'overwrite', header=True, sheet_name: str = 'Sheet1', freeze_panes: tuple = None, date_format: str = None, **params) -> DataFrame:

        """

        Write a Spark DataFrame to an Excel file.

 

        :param df: The Spark DataFrame to be written.

        :type df: pyspark.sql.DataFrame

        :param path: The path where the Excel file will be saved.

        :type path: str

        :param mode: The write mode, 'overwrite' or 'append'.

        :type mode: str

        :param header: Whether to include headers in the Excel file.

        :type header: bool

        :param sheet_name: The name of the sheet within the Excel file.

        :type sheet_name: str

        :param freeze_panes: A tuple of row and column indices to freeze panes in the Excel sheet.

        :type freeze_panes: tuple

        :param date_format: The date format to use for formatting date cells.

        :type date_format: str

        :param params: Additional parameters to pass to Spark DataFrameWriter.

        :type params: dict

        :return: The input DataFrame.

        :rtype: pyspark.sql.DataFrame

        """

        if mode not in ['overwrite', 'append']:

            raise ValueError(f"Expected mode to be 'overwrite' or 'append', not '{mode}'")

 

        if freeze_panes and len(freeze_panes) != 2:

            raise ValueError("freeze_panes should be a tuple of row and column indices")

 

        if date_format and not isinstance(date_format, str):

            raise ValueError("date_format should be a string")

 

        if not isinstance(df, SparkDataFrame):

            raise ValueError(f"Expected a pyspark dataframe, got {type(df)}.")

 

        df.write.format("com.crealytics.spark.excel").options(header=header, dataAddress=f"{sheet_name}!A1").mode(mode).save(path)

        return df

 

    def csv(self, df: DataFrame, path: str, delimiter: str = ',', quote: str = '"', **params) -> DataFrame:

        """

        Write a Spark DataFrame to a CSV file.

 

        :param df: The Spark DataFrame to be written.

        :type df: pyspark.sql.DataFrame

        :param path: The path where the CSV file will be saved.

        :type path: str

        :param delimiter: The delimiter used between fields in the CSV file.

        :type delimiter: str

        :param quote: The character used to quote fields containing special characters.

        :type quote: str

        :param params: Additional parameters to pass to Spark DataFrameWriter.

        :type params: dict

        :return: The input DataFrame.

        :rtype: pyspark.sql.DataFrame

        """

        if not isinstance(delimiter, str) or len(delimiter) != 1:

            raise ValueError("delimiter should be a single character")

 

        if not isinstance(quote, str) or len(quote) != 1:

            raise ValueError("quote should be a single character")

 

        df.write.option("delimiter", delimiter).option("quote", quote).csv(path, **params)

        return df

 

    def parquet(self, df: DataFrame, path: str, compression: str = None, **params) -> DataFrame:

        """

        Write a Spark DataFrame to a Parquet file.

 

        :param df: The Spark DataFrame to be written.

        :type df: pyspark.sql.DataFrame

        :param path: The path where the Parquet file will be saved.

        :type path: str

        :param compression: The compression codec to use for Parquet file.

        :type compression: str

        :param params: Additional parameters to pass to Spark DataFrameWriter.

        :type params: dict

        :return: The input DataFrame.

        :rtype: pyspark.sql.DataFrame

        """

        if compression and compression not in ["snappy", "gzip"]:

            raise ValueError("compression should be 'snappy' or 'gzip'")

 

        df.write.option("compression", compression).parquet(path, **params)

        return df

 

    def delta(self, df: DataFrame, path: str, mergeSchema: bool = False, **params) -> DataFrame:

        """

        Write a Spark DataFrame to a Delta table.

 

        :param df: The Spark DataFrame to be written.

        :type df: pyspark.sql.DataFrame

        :param path: The path where the Delta table will be saved.

        :type path: str

        :param mergeSchema: Whether to merge schemas if the table already exists.

        :type mergeSchema: bool

        :param params: Additional parameters to pass to Spark DataFrameWriter.

        :type params: dict

        :return: The input DataFrame.

        :rtype: pyspark.sql.DataFrame

        """

        if not isinstance(mergeSchema, bool):

            raise ValueError("mergeSchema should be a boolean")

 

        df.write.format("delta").option("mergeSchema", mergeSchema).save(path, **params)

        return df


@ConcreteStrategy('create', custom_strategy='LoadControl')
class DatabricksLoadControl:

    """

    This class represents a Databricks Load Control strategy for managing data loads.

 

    :Attributes:

        - has_unity (bool): Indicates whether the Unity catalog is enabled in the Spark configuration.

        - loads (dict[DataFrame]): A dictionary with all loaded dataframes.

 

    :Methods:

        - build: Create the load control schema and tables.

        - read: Read data from load control tables.

        - write: Write data to load control tables.

        - delete: Delete data from load control tables.

        - drop: Drop the load control schema.

 

    """

    def __init__(self):

        """

        Initialize a DatabricksLoadControl instance.

        """

        self.has_unity: bool = spark.conf.get('spark.databricks.unityCatalog.enabled').lower() == 'true'

        self.loads = dict()

        self._loaded_paths = dict()

 

    def build(self, db_name: str = None) -> None:

        """

            Create the load control schema and tables.

 

            :param db_name: The name of the load control database.

            :type db_name: str or None

        """

        db_name = "data_governance" if self.has_unity else "load_control"

        try:

            # Fetch current database

            current_db = spark.sql("SELECT current_database() AS current_db").collect()[0]

            current_db = current_db["current_db"]

 

            ##### LOAD CONTROL TABLES #####

            # Create database and select it

            spark.sql(f"""

                CREATE SCHEMA IF NOT EXISTS {db_name}

                COMMENT '{'A database to centralize source definitions for files, ownership, metadata, schemas and more.' if self.has_unity else 'A database to centralize governance.'}'

                WITH DBPROPERTIES ('TAGS' = '{'load_control_database' if self.has_unity else 'data_gov_database'}')

            """)

            spark.sql(f"USE {db_name}")

 

 

            ### CREATE Files Table

            # This table is a main pilar to route the others

            spark.sql(f"""

                CREATE TABLE IF NOT EXISTS files (

                    source STRING NOT NULL,

                    subsource STRING,

                    path STRING NOT NULL,

                    file_name STRING NOT NULL,

                    file_extension STRING

                    {', PRIMARY KEY (source, subsource)' if self.has_unity else ''}

                )

                COMMENT 'Stores file location.'

            """)

 

            # COLUMN DESCRIPTIONS

            column_descriptions = [

                """

                    ALTER TABLE files

                    ALTER COLUMN source COMMENT 'The source refers to a general location or process. Ex. "datalake.bronze", "datalake.silver", "cosmos.database.container"';

                """,

                """

                    ALTER TABLE files

                    ALTER COLUMN subsource COMMENT 'The subsource refers to a specific file or subprocess.';

                """,

                """

                    ALTER TABLE files

                    ALTER COLUMN path COMMENT 'The path to the file. Consider using {MASK} to adjust dynamic parameters in the path.';

                """,

                """

                    ALTER TABLE files

                    ALTER COLUMN file_name COMMENT 'The name of the file. It is recommended NOT to add the extension here.';

                """,

                """

                    ALTER TABLE files

                    ALTER COLUMN file_extension COMMENT 'The extension of the file. Ex. "csv", "parquet", "xlsx".';

                """

            ]

 

            for query in column_descriptions:

                spark.sql(query)

 

            ### Create Metadata Table

            # This table stores metadata information from each table.

            spark.sql(f"""

                CREATE TABLE IF NOT EXISTS meta (

                    source STRING NOT NULL,

                    subsource STRING,

                    delimeter STRING,

                    encoding STRING,

                    header STRING

                    {', PRIMARY KEY (source, subsource)' if self.has_unity else ''}

                )

                COMMENT 'Describes the metadata of the files.'

            """)

 

            ### CREATE SCHEMAS TABLE

            # Describes the structure of each file

            spark.sql(f"""

                CREATE TABLE IF NOT EXISTS schema (

                    source STRING NOT NULL,

                    subsource STRING,

                    sheet_name STRING,

                    column_name STRING,

                    column_type STRING,

                    default_value STRING

                    {', PRIMARY KEY (source, subsource)' if self.has_unity else ''}

                )

                COMMENT 'Describes the structure of the files.'

            """)

 

            ### CREATE SCOPE TABLE

            # Describes the scope where each file is set in.

            spark.sql(f"""

                CREATE TABLE IF NOT EXISTS scope (

                    source STRING NOT NULL,

                    subsource STRING,

                    environment STRING,

                    zone STRING,

                    region STRING

                    {', PRIMARY KEY (source, subsource)' if self.has_unity else ''}

                )

                COMMENT 'Describes the scope of the file.'

            """)

 

            ### CREATE STEWARD TABLE

            # Describes ownership information of the data

            spark.sql(f"""

                CREATE TABLE IF NOT EXISTS steward (

                    source STRING NOT NULL,

                    subsource STRING,

                    user STRING,

                    email STRING,

                    status STRING

                    {', PRIMARY KEY (source, subsource)' if self.has_unity else ''}

                )

                COMMENT 'Describes the owners and stewards of the data.'

            """)

 

            ### Includes default values for steward table

            spark.sql(f"""

                ALTER TABLE steward SET TBLPROPERTIES('delta.feature.allowColumnDefaults' = 'enabled');

            """)

            

            spark.sql(f"""

                ALTER TABLE steward ALTER COLUMN status SET DEFAULT 'Active'

            """)

 

            ### CREATE AUDIT TABLE

            # Keeps track of changes to the framework.

            spark.sql(f"""

                CREATE TABLE IF NOT EXISTS audit (

                    source STRING NOT NULL,

                    subsource STRING,

                    operation STRING NOT NULL,

                    created_at TIMESTAMP NOT NULL,

                    created_by STRING NOT NULL

                    {', PRIMARY KEY (source, subsource)' if self.has_unity else ''}

                )  

                COMMENT 'Keeps track to changes performed to the framework.'      

            """)

 

            ### Includes default values for audit table

            spark.sql(f"""

                ALTER TABLE audit SET TBLPROPERTIES('delta.feature.allowColumnDefaults' = 'enabled');

            """)

           

            spark.sql(f"""

                ALTER TABLE audit ALTER COLUMN created_at SET DEFAULT current_timestamp()

            """)

 

            ### As of the current version, the current_user function is not supported in INSERT operations for some wierd reason.

            # spark.sql(f"""

            #     ALTER TABLE audit ALTER COLUMN created_by SET DEFAULT current_user()

            # """)

        except:

            spark.sql(f"USE {current_db}")

            raise

        finally:

            spark.sql(f"USE {current_db}")

 

    def select(self, source: str = None, subsource: str = None, tables: str = 'all', format: str = 'DataFrame') -> DataFrame:

        """

            Read data from load control tables.

 

            :param source: The data source to read from.

            :type source: str or None

            :param subsource: The subsource within the data source (optional).

            :type subsource: str or None

            :param tables: The tables to read (options: 'all' or specific table names).

            :type tables: str

            :param format: The format of the data (default: 'DataFrame').

            :type format: str

            :return: Data read from the load control tables.

            :rtype: DataFrame

        """

        if tables == 'all':

            tables = ['files', 'meta', 'scope', 'schema', 'steward']

 

        if isinstance(tables, str):

            tables = [tables]

 

        load_control_schema = self._get_schema_name()

 

        for table in tables:

            if table in ['file', 'files']:

                files_result = spark.sql(f"""

                    SELECT *

                    FROM {load_control_schema}.files as files

                    { f'WHERE files.source == "{source}"' if source else '' }       

                    { f'AND files.subsource == "{subsource}"' if subsource else '' }

                """)

 

            if table in ['meta', 'metadata']:

                meta_result = spark.sql(f"""

                    SELECT *

                    FROM {load_control_schema}.meta as meta

                    { f'WHERE meta.source == "{source}"' if source else '' }       

                    { f'AND meta.subsource == "{subsource}"' if subsource else '' }

                """)

 

            if table in ['scope', 'scopes']:

                scope_result = spark.sql(f"""

                    SELECT *

                    FROM {load_control_schema}.scope as scope

                    { f'WHERE scope.source == "{source}"' if source else '' }       

                    { f'AND scope.subsource == "{subsource}"' if subsource else '' }

                """)

 

            if table in ['schema', 'schemas']:

                schema_result = spark.sql(f"""

                    SELECT *

                    FROM {load_control_schema}.schema as schema

                    { f'WHERE schema.source == "{source}"' if source else '' }       

                    { f'AND schema.subsource == "{subsource}"' if subsource else '' }

                """)

 

            if table in ['steward', 'stewards', 'stewardship']:

                stewards_result = spark.sql(f"""

                    SELECT *

                    FROM {load_control_schema}.steward as steward

                    { f'WHERE steward.source == "{source}"' if source else '' }       

                    { f'AND steward.subsource == "{subsource}"' if subsource else '' }

                """)

 

        results = files_result

 

        try: results = results.join(meta_result, ['source', 'subsource'], 'inner');

        except: pass

        try: results = results.join(scope_result, ['source', 'subsource'], 'inner');

        except: pass

        try: results = results.join(schema_result, ['source', 'subsource'], 'inner');

        except: pass

        try: results = results.join(stewards_result, ['source', 'subsource'], 'inner');

        except: pass

 

        user_ = dbutils.notebook.entry_point.getDbutils().notebook().getContext().userName().get()

 

        # Audit change

        spark.sql(f"""

            INSERT INTO {load_control_schema}.audit (source, subsource, operation, created_at, created_by)

            VALUES ('{source}', '{subsource}', 'Read', DEFAULT, '{user_}')         

        """)

 

        if format.lower() == 'dataframe':

            return results

 

    def upsert(self, source: str, path: str, file_name: str, file_extension: str = '', subsource: str = '', delimeter: str = '', encoding: str = '', column_name: str = '', column_type: str = '', default_value: str = '', environment: str = '', zone: str = '', region: str = '', user: str = '', email: str = '', status: str = '', header: str = 'true') -> None:

        load_control_schema = self._get_schema_name()

 

        def upsert_table(table_name, columns, values):

            update_sql = f"UPDATE {table_name} SET "

            for col, val in zip(columns, values):

                update_sql += f"{col} = '{val}', "

            update_sql = update_sql.rstrip(', ')

            update_sql += f" WHERE source = '{source}' AND subsource = '{subsource}'"

           

            insert_sql = f"INSERT INTO {table_name} (source, subsource, {' ,'.join(columns)})"

            def define_columns(val):
                return "'" + val + "'"

            insert_sql += f" VALUES ('{source}', '{subsource}', {' ,'.join([define_columns(val) for val in values])})"

           

            existing_record = spark.sql(f"SELECT COUNT(source) > 0 as exists FROM {table_name} WHERE source = '{source}' AND subsource = '{subsource}'").first()["exists"]

           

            if existing_record:

                spark.sql(update_sql)

            else:

                spark.sql(insert_sql)

 

        # Upsert into files table

        upsert_table(f"{load_control_schema}.files", ["path", "file_name", "file_extension"], [path, file_name, file_extension])

 

        # Upsert into meta table

        upsert_table(f"{load_control_schema}.meta", ["delimeter", "encoding", "header"], [delimeter, encoding, header])

 

        # Upsert into scope table

        upsert_table(f"{load_control_schema}.scope", ["environment", "zone", "region"], [environment, zone, region])

 

        # Upsert into steward table

        upsert_table(f"{load_control_schema}.steward", ["user", "email", "status"], [user, email, status])

 

        # Upsert into audit table

        user_ = dbutils.notebook.entry_point.getDbutils().notebook().getContext().userName().get()

        upsert_table(f"{load_control_schema}.audit", ["operation", "created_at", "created_by"], ["Upsert", "DEFAULT", user_])

 

    def load(self, source, subsource, infer_schema: bool = False, delimeter: str = ',', strategy = 'spark', **masks):

        """

            Load data from a specified source and subsource.

 

            :param str source: The source of the data.

            :param str subsource: The subsource of the data.

            :param bool infer_schema: Whether to infer the schema based on the loaded data.

            :param str delimeter: The delimiter used in the data.

            :param str strategy: The loading strategy to use (e.g., 'spark', 'pandas').

            :param masks: Additional mask values for dynamic path replacement.

            :return: None

            :rtype: None

        """

        load_control_schema = self._get_schema_name()

 

        path = spark.sql(f"""SELECT CONCAT(path, file_name, '.', file_extension) AS path FROM {load_control_schema}.files WHERE source='{source}' AND subsource='{subsource}'""").first()[0]

 

        extension = spark.sql(f"""SELECT file_extension FROM {load_control_schema}.files WHERE source='{source}' AND subsource='{subsource}'""").first()[0]

 

        delimeter = spark.sql(f"""SELECT delimeter FROM {load_control_schema}.meta WHERE source='{source}' AND subsource='{subsource}' """).first()[0]

        encoding = spark.sql(f"""SELECT encoding FROM {load_control_schema}.meta WHERE source='{source}' AND subsource='{subsource}' """).first()[0]

 

        import inspect

 

        def extract_variables(keys):

            calling_frame = inspect.currentframe().f_back

            local_vars = calling_frame.f_locals

            extracted_vars = {key: local_vars[key] for key in keys if key in local_vars}

            return extracted_vars

 

        def build_param(name, value):

            if value:

                return f"{name} = '{value}'"

            return ''

 

        _params = ", ".join(list(map(lambda param: build_param(param[0], param[1]),extract_variables(['delimeter', 'encoding']).items())))

 

        for mask_name, mask_value in masks.items():

            if mask_name not in ['source', 'subsource', 'infer_schema', 'strategy']:

                path = path.replace( '{' + mask_name + '}', mask_value)

 

        self._loaded_paths[(source, subsource)] = path

        if infer_schema:

            self.loads[(source, subsource)] = self._recalculate_schema(eval(f"DataReader('{strategy}').{DataReader.format_map(extension)}('{path}', {_params})"))

        else:

            self.loads[(source, subsource)] = eval(f"DataReader('{strategy}').{DataReader.format_map(extension)}('{path}', {_params})")

       

    def save(self, path: str = None, source: str = None, subsource: str = None, dataframe: DataFrame = None, strategy: str = 'spark', **masks):

        load_control_schema = self._get_schema_name()

        path = path or spark.sql(f"""SELECT CONCAT(path, file_name, '.', file_extension) AS path FROM {load_control_schema}.files WHERE source='{source}' AND subsource='{subsource}'""").first()[0]

 

        extension = path.split('.')[-1] or spark.sql(f"""SELECT file_extension FROM {load_control_schema}.files WHERE source='{source}' AND subsource='{subsource}'""").first()[0]

 

        for mask_name, mask_value in masks.items():

            if mask_name not in ['source', 'subsource', 'dataframe', 'strategy']:

                path = path.replace( '{' + mask_name + '}', mask_value)

 

        if isinstance(dataframe, PandasDataFrame) or isinstance(dataframe, SparkDataFrame):

            eval(f"DataWriter('{strategy}').{DataReader.format_map(extension)}('{path}', dataframe)")

        elif source or subsource:

            dataframe = self.loads.get((source, subsource))

 

            eval(f"DataWriter('{strategy}').{DataReader.format_map(extension)}('{path}', dataframe)")

        else:

            raise ValueError("Writer method requires a source/subsource or a Pandas or Pyspark DataFrame.")

 

    def infer(self, source: str, subsource: str):

        """

            Infer the schema based on the loaded data and update the schema records.

 

            :param str source: The source of the data.

            :param str subsource: The subsource of the data.

            :return: None

            :rtype: None

        """

        load_control_schema = self._get_schema_name()

 

        path = self._loaded_paths.get((source, subsource))

        df = self.loads.get((source, subsource))

 

        sheet = ''

 

        if isinstance(df, PandasDataFrame):

           

            for column, column_type in dict(map(lambda item: (item[0], item[1].name), df.dtypes.to_dict().items())).items():

                if self._has_schema(source, subsource, column):

                    spark.sql(f"""

                        INSERT INTO {load_control_schema}.schema (source, subsource, sheet_name, column_name, column_type)

                        VALUES ('{source}', '{subsource}', '{sheet}', '{column}', '{column_type}')

                    """)

 

            user_ = dbutils.notebook.entry_point.getDbutils().notebook().getContext().userName().get()

            spark.sql(f"""

                INSERT INTO {load_control_schema}.audit (source, subsource, operation, created_at, created_by)

                VALUES ('{source}', '{subsource}', 'Update', DEFAULT, '{user_}')         

            """)

        elif isinstance(df, SparkDataFrame):

            for column_name, data_type in df.dtypes:

                if self._has_schema(source, subsource, column_name):

                    spark.sql(f"""

                        INSERT INTO {load_control_schema}.schema (source, subsource, column_name, column_type)

                        VALUES ('{source}', '{subsource}', '{column_name}', '{data_type}')

                    """)

 

            user_ = dbutils.notebook.entry_point.getDbutils().notebook().getContext().userName().get()

            spark.sql(f"""

                INSERT INTO {load_control_schema}.audit (source, subsource, operation, created_at, created_by)

                VALUES ('{source}', '{subsource}', 'Update', DEFAULT, '{user_}')

            """)

        elif isinstance(df, type(None)):

            raise Exception('DataFrame is not loaded yet, try using .load() first.')

 

    def set_meta(self, source: str, subsource: str, delimeter: str, encoding: str, header: bool):

        """

            Set metadata information for a specified source and subsource.

 

            :param str source: The source of the data.

            :param str subsource: The subsource of the data.

            :param str delimeter: The delimiter used in the data.

            :param str encoding: The character encoding of the data.

            :param bool header: Whether the data has headers.

            :return: None

            :rtype: None

        """

 

        load_control_schema = self._get_schema_name()

        if spark.sql(f"""

            SELECT count(source) == 1

            FROM {load_control_schema}.meta

            WHERE source = '{source}' AND subsource = '{subsource}'

        """

        ).first()[0]:

            # Update

            delimeter = "delimeter='{delimeter}'" if delimeter else ''

            encoding = "encoding='{encoding}'" if encoding else ''

            header = "header='{header}'" if header else ''

            spark.sql(f"""

                UPDATE {load_control_schema}.meta

                SET {', '.join([delimeter, encoding, header])}

                WHERE source = '{source}' AND subsource = '{subsource}'

            """)

        else:

            # Insert

            spark.sql(f"""

                INSERT INTO {load_control_schema}.meta (source, subsource, delimeter, encoding, header)

                VALUES ('{source}', '{subsource}', '{delimeter}', '{encoding}', '{header}')

            """)

 

    def set_schema(self, source, subsource, column_name, column_type: str = None, default_value: str = None, sheet_name: str = None):

        """

            Set schema information for a specified column in a source and subsource.

 

            :param str source: The source of the data.

            :param str subsource: The subsource of the data.

            :param str column_name: The name of the column.

            :param str column_type: The data type of the column.

            :param str default_value: The default value of the column.

            :param str sheet_name: The name of the sheet (if applicable).

            :return: None

            :rtype: None

        """

        load_control_schema = self._get_schema_name()

        if spark.sql(f"""

            SELECT count(source) == 1

            FROM {load_control_schema}.schema

            WHERE source = '{source}' AND subsource = '{subsource}' AND column_name='{column_name}'

        """

        ).first()[0]:

            # Update

            column_type = "column_type='{column_type}'" if column_type else ''

            default_value = "default_value='{default_value}'" if default_value else ''

            sheet_name = "sheet_name='{sheet_name}'" if sheet_name else ''

            spark.sql(f"""

                UPDATE {load_control_schema}.schema

                SET {', '.join([column_type, default_value, sheet_name])}

                WHERE source = '{source}' AND subsource = '{subsource}' AND column_name='{column_name}'

            """)

        else:

            # Insert

            spark.sql(f"""

                INSERT INTO {load_control_schema}.schema (source, subsource, sheet_name, column_name, column_type, default_value)

                VALUES ('{source}', '{subsource}', '{sheet_name}', '{column_name}', '{column_type}', '{default_value}')

            """)

 

    def set_steward(self, source, subsource, user, email, status):

        """

            Set steward information for a specified source and subsource.

 

            :param str source: The source of the data.

            :param str subsource: The subsource of the data.

            :param str user: The name of the steward.

            :param str email: The email of the steward.

            :param str status: The status of the steward.

            :return: None

            :rtype: None

        """

 

        load_control_schema = self._get_schema_name()

        if spark.sql(f"""

            SELECT count(source) == 1

            FROM {load_control_schema}.steward

            WHERE source = '{source}' AND subsource = '{subsource}'

        """

        ).first()[0]:

            # Update

            user = "user='{user}'" if user else ''

            email = "email='{email}'" if email else ''

            status = "status='{status}'" if status else ''

            spark.sql(f"""

                UPDATE {load_control_schema}.steward

                SET {', '.join([user, email, status])}

                WHERE source = '{source}' AND subsource = '{subsource}'

            """)

        else:

            # Insert

            spark.sql(f"""

                INSERT INTO {load_control_schema}.steward (source, subsource, user, email, status)

                VALUES ('{source}', '{subsource}', '{user}', '{email}', '{status}')

            """)

 

    def set_scope(self, source, subsource):

        """

            Set scope information for a specified source and subsource.

 

            :param str source: The source of the data.

            :param str subsource: The subsource of the data.

            :param str environment: The environment of the data.

            :param str zone: The zone of the data.

            :param str region: The region of the data.

            :return: None

            :rtype: None

        """

 

        load_control_schema = self._get_schema_name()

        if spark.sql(f"""

            SELECT count(source) == 1

            FROM {load_control_schema}.scope

            WHERE source = '{source}' AND subsource = '{subsource}'

        """

        ).first()[0]:

            # Update

            environment = "environment='{environment}'" if environment else ''

            zone = "zone='{zone}'" if zone else ''

            region = "region='{region}'" if region else ''

            spark.sql(f"""

                UPDATE {load_control_schema}.scope

                SET {', '.join([environment, zone, region])}

                WHERE source = '{source}' AND subsource = '{subsource}'

            """)

        else:

            # Insert

            spark.sql(f"""

                INSERT INTO {load_control_schema}.scope (source, subsource, environment, zone, region)

                VALUES ('{source}', '{subsource}', '{environment}', '{zone}', '{region}')

            """)

 

    def delete(self, source: str, subsource: str = None) -> None:

        """

        Delete data from load control tables.

 

        :param source: The data source to delete.

        :type source: str

        :param subsource: The subsource to delete (optional).

        :type subsource: str or None

        """

        load_control_schema = self._get_schema_name()

 

        spark.sql(f"""

            DELETE FROM {load_control_schema}.files

            WHERE source == '{source}' AND subsource == '{subsource}'

        """)

 

        spark.sql(f"""

            DELETE FROM {load_control_schema}.meta

            WHERE source == '{source}' AND subsource == '{subsource}'

        """)

 

        spark.sql(f"""

            DELETE FROM {load_control_schema}.schema

            WHERE source == '{source}' AND subsource == '{subsource}'

        """)

 

        spark.sql(f"""

            DELETE FROM {load_control_schema}.scope

            WHERE source == '{source}' AND subsource == '{subsource}'

        """)

 

        spark.sql(f"""

            DELETE FROM {load_control_schema}.steward

            WHERE source == '{source}' AND subsource == '{subsource}'

        """)

 

        user_ = dbutils.notebook.entry_point.getDbutils().notebook().getContext().userName().get()

 

        # Audit change

        spark.sql(f"""

            INSERT INTO {load_control_schema}.audit (source, subsource, operation, created_at, created_by)

            VALUES ('{source}', '{subsource}', 'Delete', DEFAULT, '{user_}')         

        """)

 

    def drop(self, cascade: bool = False) -> None:

        """

            Drop the load control schema.

 

            :param cascade: Whether to perform a cascading drop.

            :type cascade: bool

        """

 

 

        load_control_schema = self._get_schema_name()

 

        spark.sql(f"""

            DROP SCHEMA {load_control_schema} {'CASCADE' if cascade else ''}

        """)

   

    def _get_schema_name(self):

        """

            Get the name of the Load Control database schema.

 

            :return: The name of the Load Control database schema.

            :rtype: str

            :raises: Exception if no Load Control database is found.

        """

        def extract_tags(properties_str):

            if not properties_str:

                return

            properties_str = properties_str[0]

            match = search(r"\(\(TAGS,([^)]+)\)\)", properties_str)

            if match:

                tags_str = match.group(1)

                return [tag.strip() for tag in tags_str.split(',') if tag.strip()]

            return []

 

        for row in spark.sql("SHOW DATABASES").collect():

            table_name = row["databaseName"]

            properties = list(filter(None, map(lambda row: row["database_description_value"] if row["database_description_item"] == "Properties" else None, spark.sql(f"""DESCRIBE SCHEMA EXTENDED {table_name}""").collect())))

            tags = extract_tags(properties)

            for db_tag in ['load_control_database', 'data_gov_database']:

                if tags and db_tag in tags:

                    return table_name

 

        raise Exception("No Load Control DB found. Create one before trying this operation.")

 

    def _has_schema(self, source, subsource, column):

        """

            Check if a schema exists for a specified source, subsource, and column.

 

            :param str source: The source of the data.

            :param str subsource: The subsource of the data.

            :param str column: The name of the column.

            :return: Whether the schema exists.

            :rtype: bool

        """

        load_control_schema = self._get_schema_name()

 

        return spark.sql(f"""

            SELECT count(source) < 2

            FROM {load_control_schema}.schema

            WHERE source = '{source}' AND subsource = '{subsource}' AND column_name='{column}'

        """).first()[0]

 

    # Function to recalculate schema of a DataFrame based on actual values

    def _recalculate_schema(self, df):

        """

            Recalculate the schema of a DataFrame based on actual values.

 

            :param Union[PandasDataFrame, SparkDataFrame] df: The DataFrame to recalculate the schema for.

            :return: The DataFrame with recalculated schema.

            :rtype: Union[PandasDataFrame, SparkDataFrame]

        """

        # Function to infer data type based on the actual values in a column

        def infer_data_type(values):

            """

                Infer data type based on actual values in a column.

 

                :param values: The values in the column.

                :return: The inferred data type.

                :rtype: str

            """

            num_int = 0

            num_double = 0

            num_string = 0

 

            for value in values:

                if value is not None and value.strip():

                    try:

                        int(value)

                        num_int += 1

                    except ValueError:

                        try:

                            float(value)

                            num_double += 1

                        except ValueError:

                            num_string += 1

 

            if num_string > num_int + num_double:

                return "string"

            elif num_int > num_double:

                return "integer"

            else:

                return "double"

 

        new_schema = []

 

        for column_name in df.columns:

            values = df.select(column_name).rdd.flatMap(lambda x: x).collect()

            inferred_data_type = infer_data_type(values)

            new_schema.append((column_name, inferred_data_type))

 

        new_columns = [f"{column} {data_type}" for column, data_type in new_schema]

        column_select_expr = [col(column).cast(data_type) for column, data_type in new_schema]

        recalculated_df = df.select(*column_select_expr)

 

        return recalculated_df


@ConcreteStrategy('create', custom_strategy='LoadControl')
class SqlLoadControl:
    """
    This class provides a Load Control Framework for managing data loads with different strategies.

    :Attributes:
        - has_unity (bool): Indicates whether the Unity catalog is enabled in the Spark configuration.

    :Methods:
        - create: Create and populate load control tables.
        - read: Read data from load control tables.
        - write: Write data to load control tables.
        - delete: Delete data from load control tables.
        - drop: Drop the load control schema.
    """
    def __init__(self, framework: str = 'databricks'):
        """
        Initialize the SqlLoadControl.

        """
        self.has_unity: bool = spark.conf.get('spark.databricks.unityCatalog.enabled').lower() == 'true'

    def create(self, db_name: str = None) -> None:
        """
        Create a new Load Control database.

        :param db_name: The name of the database to be created.
        :type db_name: str or None
        """
        return

    def read(self, source: str = None, subsource: str = None, tables: str = 'all', format: str = 'DataFrame'):
        """
        Read data from a specified source.

        :param source: The data source to read from.
        :type source: str or None
        :param subsource: The subsource within the data source (optional).
        :type subsource: str or None
        :param tables: The tables to read (options: 'all' or specific table names).
        :type tables: str
        :param format: The format in which to read the data (default: 'DataFrame').
        :type format: str
        """
        return

    def write(self, source: str, path: str, file_name: str, file_extension: str = '', subsource: str = '', delimeter: str = '', encoding: str = '', column_name: str = '', column_type: str = '', default_value: str = '', environment: str = '', zone: str = '', region: str = '', user: str = '', email: str = '', status: str = ''):
        """
        Write data to a specified destination.

        :param source: The data source to write to.
        :type source: str
        :param path: The path where the data will be written.
        :type path: str
        :param file_name: The name of the file to be written.
        :type file_name: str
        :param file_extension: The file extension (optional).
        :type file_extension: str
        :param subsource: The subsource within the data source (optional).
        :type subsource: str
        :param delimeter: The delimiter to use for writing data (optional).
        :type delimeter: str
        :param encoding: The encoding to use for writing data (optional).
        :type encoding: str
        :param column_name: The name of the column (optional).
        :type column_name: str
        :param column_type: The data type of the column (optional).
        :type column_type: str
        :param default_value: The default value for the column (optional).
        :type default_value: str
        :param environment: The environment information (optional).
        :type environment: str
        :param zone: The zone information (optional).
        :type zone: str
        :param region: The region information (optional).
        :type region: str
        :param user: The user information (optional).
        :type user: str
        :param email: The email information (optional).
        :type email: str
        :param status: The status information (optional).
        :type status: str
        """
        return

    def delete(self, source: str, subsource: str = None):
        """
        Delete data from a specified source.

        :param source: The data source to delete from.
        :type source: str
        :param subsource: The subsource within the data source (optional).
        :type subsource: str or None
        """
        return

    def drop(self, cascade: bool = False):
        """
        Drop the LoadControl instance.

        :param cascade: Whether to perform a cascading drop (optional, default: False).
        :type cascade: bool
        """
        return


@Context
class LoadControl:
    """
    This class provides a Load Control Framework for managing data loads with different strategies.

    :Attributes:
        - has_unity (bool): Indicates whether the Unity catalog is enabled in the Spark configuration.

    :Methods:
        - create: Create and populate load control tables.
        - read: Read data from load control tables.
        - write: Write data to load control tables.
        - delete: Delete data from load control tables.
        - drop: Drop the load control schema.
    """
    def __init__(self, framework: str = 'databricks'):
        """
        Initialize the LoadControl instance with the specified framework strategy.

        :param framework: The framework strategy to use (options: 'databricks' or 'sql').
        :type framework: str
        :raises StrategyNotDefinedError: If an invalid framework is provided.
        """
        if framework.lower() in ['databricks']:
            self.set_strategy(DatabricksLoadControl())
        elif framework.lower() in ['sql']:
            self.set_strategy(SqlLoadControl())
        else:
            raise StrategyNotDefinedError(framework)

    def create(self, db_name: str = None) -> None:
        """
        Create a new Load Control database.

        :param db_name: The name of the database to be created.
        :type db_name: str or None
        """
        return

    def read(self, source: str = None, subsource: str = None, tables: str = 'all', format: str = 'DataFrame'):
        """
        Read data from a specified source.

        :param source: The data source to read from.
        :type source: str or None
        :param subsource: The subsource within the data source (optional).
        :type subsource: str or None
        :param tables: The tables to read (options: 'all' or specific table names).
        :type tables: str
        :param format: The format in which to read the data (default: 'DataFrame').
        :type format: str
        """
        return

    def write(self, source: str, path: str, file_name: str, file_extension: str = '', subsource: str = '', delimeter: str = '', encoding: str = '', column_name: str = '', column_type: str = '', default_value: str = '', environment: str = '', zone: str = '', region: str = '', user: str = '', email: str = '', status: str = ''):
        """
        Write data to a specified destination.

        :param source: The data source to write to.
        :type source: str
        :param path: The path where the data will be written.
        :type path: str
        :param file_name: The name of the file to be written.
        :type file_name: str
        :param file_extension: The file extension (optional).
        :type file_extension: str
        :param subsource: The subsource within the data source (optional).
        :type subsource: str
        :param delimeter: The delimiter to use for writing data (optional).
        :type delimeter: str
        :param encoding: The encoding to use for writing data (optional).
        :type encoding: str
        :param column_name: The name of the column (optional).
        :type column_name: str
        :param column_type: The data type of the column (optional).
        :type column_type: str
        :param default_value: The default value for the column (optional).
        :type default_value: str
        :param environment: The environment information (optional).
        :type environment: str
        :param zone: The zone information (optional).
        :type zone: str
        :param region: The region information (optional).
        :type region: str
        :param user: The user information (optional).
        :type user: str
        :param email: The email information (optional).
        :type email: str
        :param status: The status information (optional).
        :type status: str
        """
        return

    def delete(self, source: str, subsource: str = None):
        """
        Delete data from a specified source.

        :param source: The data source to delete from.
        :type source: str
        :param subsource: The subsource within the data source (optional).
        :type subsource: str or None
        """
        return

    def drop(self, cascade: bool = False):
        """
        Drop the LoadControl instance.

        :param cascade: Whether to perform a cascading drop (optional, default: False).
        :type cascade: bool
        """
        return