Mount
=====

.. autofunction:: hiveadb.functions.mount
   :noindex:

Example
^^^^^^^
..  code-block:: python

    from hiveadb.functions import mount
    
    storage_name = "stmystorage"
    storage_key  = dbutils.secrets.get("MyScope", "stmystorage_key")
    mount(storage_name, storage_key, mounts=["bronze-zone", "silver-zone", "gold-zone"])