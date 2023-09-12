AuditManager
============

.. autoclass:: hiveadb.functions.AuditManager
    :members: on_error, on_exit, define_interaction, reset_timer

Example
-------
The AuditManager helps with the process of Auditing any Read, Write, Update, Delete or any possible information needed on behalf of Azure Data Factory.

To use the AuditManager you will need a databricks token. Find out more about them here!

Declaration
~~~~~~~~~~~

..  code-block:: python

    from hiveadb.audit_manager import AuditManager

    token = dbutils.secrets.get("MyScope", "DatabricksToken")

    audit_manager = AuditManager(token)

Definitions
~~~~~~~~~~~
In order to use the framework you will have to define an interaction. Here are some recommendations:

- Define all your sources at the beggining of your code.
- The special parameter _primary_key is used to define a list of attributes used as a primary key for your definitions.
- By default, the base interaction attrbiutes are "rowsRead", "rowsInsert", "rowsUpdate" and "rowsDelete".
- You can change the default interactions by modifying the OPERATIONS attrbiute of an interaction.
- You can add as many extra parameters you want, as this method recibes any kwargs and sends them to the interaction.

..  code-block:: python

    # Define inputs
    audit_manager.define_interaction(_primary_key=["File"], File="sales.csv")
    audit_manager.define_interaction(_primary_key=["File"], File="clients.parquet")

    # Define outputs
    audit_manager.define_interaction(_primary_key=["Schema", "Table"], Schema="default", Table="ranks")

Update
~~~~~~

..  code-block:: python

    audit_manager.reset_timer(my_file_name)
    # Read operation
    audit_manager.interactions[(my_file_name,)].update(rowsRead=df_rows_read)
    
    # Transformations

    audit_manager.reset_timer(my_schema_name, my_table_name)
    # Write operation
    audit_manager.interactions[(my_schema_name, my_table_name,)].update(rowsRead=df_rows_written)

Future Changes
--------------
The following is a list of not implemented functionalities that will be added in future versions.

- Direct audit to the governance framework ecosystem.