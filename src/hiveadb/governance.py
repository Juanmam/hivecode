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


@ConcreteStrategy('create', custom_strategy='LoadControl')
class DatabricksLoadControl:
    def _init_(self):
        self.has_unity: bool = spark.conf.get('spark.databricks.unityCatalog.enabled').lower() == 'true'

    def create(self, db_name: str = None, framework: str = "databricks") -> None:
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

            ### Create Metadata Table
            # This table stores metadata information from each table.
            spark.sql(f"""
                CREATE TABLE IF NOT EXISTS meta (
                    source STRING NOT NULL,
                    subsource STRING,
                    delimeter STRING,
                    encoding STRING
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
                CREATE TABLE IF NOT EXISTS stewards (
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
                ALTER TABLE stewards SET TBLPROPERTIES('delta.feature.allowColumnDefaults' = 'enabled');
            """)
            
            spark.sql(f"""
                ALTER TABLE stewards ALTER COLUMN status SET DEFAULT 'Active'
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

    def read(self, source: str = None, subsource: str = None, tables: str = 'all', format: str = 'DataFrame'):
        if tables == 'all':
            tables = ['files', 'meta', 'scope', 'schema', 'steward']

        if isinstance(tables, str):
            tables = [tables]

        try:
            def extract_tags(properties_str):
                if not properties_str:
                    return
                properties_str = properties_str[0]
                match = search(r"\(\(TAGS,([^)]+)\)\)", properties_str)
                if match:
                    tags_str = match.group(1)
                    return [tag.strip() for tag in tags_str.split(',') if tag.strip()]
                return []

            load_control_schema = list(compress(list(map(lambda row: row["databaseName"], spark.sql("SHOW DATABASES").collect())), list(map(lambda properties: not None == extract_tags(properties), list(map(lambda table_name: list(filter(None, map(lambda row: row["database_description_value"] if row["database_description_item"] == "Properties" else None, spark.sql(f"""DESCRIBE SCHEMA EXTENDED {table_name}""").collect()))), list(map(lambda row: row["databaseName"], spark.sql("SHOW DATABASES").collect()))))))))[0]
        except:
            raise Exception("No load control database detected.")

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
                    FROM {load_control_schema}.stewards as stewards
                    { f'WHERE stewards.source == "{source}"' if source else '' }        
                    { f'AND stewards.subsource == "{subsource}"' if subsource else '' }
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

    def write(self, source: str, path: str, file_name: str, file_extension: str = '', subsource: str = '', delimeter: str = '', encoding: str = '', column_name: str = '', column_type: str = '', default_value: str = '', environment: str = '', zone: str = '', region: str = '', user: str = '', email: str = '', status: str = ''):
        def extract_tags(properties_str):
            if not properties_str:
                return
            properties_str = properties_str[0]
            match = search(r"\(\(TAGS,([^)]+)\)\)", properties_str)
            if match:
                tags_str = match.group(1)
                return [tag.strip() for tag in tags_str.split(',') if tag.strip()]
            return []

        try:
            load_control_database = list(compress(list(map(lambda row: row["databaseName"], spark.sql("SHOW DATABASES").collect())), list(map(lambda properties: not None == extract_tags(properties), list(map(lambda table_name: list(filter(None, map(lambda row: row["database_description_value"] if row["database_description_item"] == "Properties" else None, spark.sql(f"""DESCRIBE SCHEMA EXTENDED {table_name}""").collect()))), list(map(lambda row: row["databaseName"], spark.sql("SHOW DATABASES").collect()))))))))[0]
        except:
            raise Exception("No load control database detected.")

        spark.sql(f"""
            INSERT INTO {load_control_database}.files (source, subsource, path, file_name, file_extension)
            VALUES ('{source}', '{subsource}', '{path}', '{file_name}', '{file_extension}')
        """)

        spark.sql(f"""
            INSERT INTO {load_control_database}.meta (source, subsource, delimeter, encoding)
            VALUES ('{source}', '{subsource}', '{delimeter}', '{encoding}')       
        """)

        spark.sql(f"""
            INSERT INTO {load_control_database}.schema (source, subsource, column_name, column_type, default_value)
            VALUES ('{source}', '{subsource}', '{column_name}', '{column_type}', '{default_value}')
        """)

        spark.sql(f"""
            INSERT INTO {load_control_database}.scope (source, subsource, environment, zone, region)
            VALUES ('{source}', '{subsource}', '{environment}', '{zone}', '{region}')     
        """)

        spark.sql(f"""
            INSERT INTO {load_control_database}.stewards (source, subsource, user, email, status)
            VALUES ('{source}', '{subsource}', '{user}', '{email}', '{status}')      
        """)

        user_ = dbutils.notebook.entry_point.getDbutils().notebook().getContext().userName().get()

        spark.sql(f"""
            INSERT INTO {load_control_database}.audit (source, subsource, operation, created_at, created_by)
            VALUES ('{source}', '{subsource}', 'Insert', DEFAULT, '{user_}')          
        """)

    def delete(self, source: str, subsource: str = None):
        try:
            def extract_tags(properties_str):
                if not properties_str:
                    return
                properties_str = properties_str[0]
                match = search(r"\(\(TAGS,([^)]+)\)\)", properties_str)
                if match:
                    tags_str = match.group(1)
                    return [tag.strip() for tag in tags_str.split(',') if tag.strip()]
                return []

            load_control_schema = list(compress(list(map(lambda row: row["databaseName"], spark.sql("SHOW DATABASES").collect())), list(map(lambda properties: not None == extract_tags(properties), list(map(lambda table_name: list(filter(None, map(lambda row: row["database_description_value"] if row["database_description_item"] == "Properties" else None, spark.sql(f"""DESCRIBE SCHEMA EXTENDED {table_name}""").collect()))), list(map(lambda row: row["databaseName"], spark.sql("SHOW DATABASES").collect()))))))))[0]
        except:
            raise Exception("No load control database detected.")

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
            DELETE FROM {load_control_schema}.stewards
            WHERE source == '{source}' AND subsource == '{subsource}'
        """)

        user_ = dbutils.notebook.entry_point.getDbutils().notebook().getContext().userName().get()

        # Audit change
        spark.sql(f"""
            INSERT INTO {load_control_schema}.audit (source, subsource, operation, created_at, created_by)
            VALUES ('{source}', '{subsource}', 'Delete', DEFAULT, '{user_}')          
        """)

    def drop(self, cascade: bool = False):
        try:
            def extract_tags(properties_str):
                if not properties_str:
                    return
                properties_str = properties_str[0]
                match = search(r"\(\(TAGS,([^)]+)\)\)", properties_str)
                if match:
                    tags_str = match.group(1)
                    return [tag.strip() for tag in tags_str.split(',') if tag.strip()]
                return []

            load_control_schema = list(compress(list(map(lambda row: row["databaseName"], spark.sql("SHOW DATABASES").collect())), list(map(lambda properties: not None == extract_tags(properties), list(map(lambda table_name: list(filter(None, map(lambda row: row["database_description_value"] if row["database_description_item"] == "Properties" else None, spark.sql(f"""DESCRIBE SCHEMA EXTENDED {table_name}""").collect()))), list(map(lambda row: row["databaseName"], spark.sql("SHOW DATABASES").collect()))))))))[0]
        except:
            raise

        spark.sql(f"""
            DROP SCHEMA {load_control_schema} {'CASCADE' if cascade else ''}
        """)


@ConcreteStrategy('create', custom_strategy='LoadControl')
class SqlLoadControl:
    def _init_(self):
        self.has_unity: bool = spark.conf.get('spark.databricks.unityCatalog.enabled').lower() == 'true'


@Context
class LoadControl:
    def _init_(self, framework = 'databricks'):
        if framework.lower() in ['databricks']:
            self.set_strategy(DatabricksLoadControl())
        elif framework.lower() in ['sql']:
            self.set_strategy(SqlLoadControl())
        else:
            raise StrategyNotDefinedError(framework)