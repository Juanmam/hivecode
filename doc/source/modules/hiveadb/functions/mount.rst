Mount
=====

.. autofunction:: hiveadb.functions.mount
   :noindex:

Example
^^^^^^^
..  code-block:: python
    
    storage_name = "stmystorage"
    storage_key  = "qs3fny5bMT5fdfG2JzAuO72Jzg+7hkJlAXEW/lXsi0bIo8hxI2xMfB9zvEjQ24AUHX2HkO5XFO+A+AStk8FGzT=="
    mount(storage_name, storage_key, mounts=["raw-zone", "silver-zone", "gold-zone"])