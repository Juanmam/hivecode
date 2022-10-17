Mount
=====

.. role:: method
.. role:: param

hivecode.hiveadb.functions. :method:`mount` ( :param:`storage: str, key: str, mount_point: str, mounts: List[str], verbose: bool` )

    Mounts a list of containers from Azure Storage to DataBricks.

Example
^^^^^^^
..  code-block:: python
    
    storage_name = "stmystorage"
    storage_key  = "qs3fny5bMT5fdfG2JzAuO72Jzg+7hkJlAXEW/lXsi0bIo8hxI2xMfB9zvEjQ24AUHX2HkO5XFO+A+AStk8FGzT=="
    mount(storage_name, storage_key, mounts=["raw-zone", "silver-zone", "gold-zone"])