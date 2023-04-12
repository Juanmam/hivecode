Read Cosmos
===========

.. autofunction:: hiveadb.io.read_cosmos
   :noindex:

Read Single File Example
^^^^^^^^^^^^^^^^^^^^^^^^
Read Cosmos supports reading a single container into a DataFrame by just passing a single string parameter, by default using Koalas as an engine and will return a Koalas DataFrame. Parameters as_type and engine can be used to specify the use of others engines and return types.

..  code-block:: python

    cosmosEndpoint      = "<my_cosmos_endpoint>"
    cosmosMasterKey     = "<my_cosmos_master_key>"
    cosmosDatabaseName  = "formats"
    cosmosContainerName = "123"
    
    df = read_cosmos(cosmosEndpoint, cosmosMasterKey, cosmosDatabaseName, cosmosContainerName)

Read Multiple File Example
^^^^^^^^^^^^^^^^^^^^^^^^^^
Read cosmos can also support reading multiple DataFrame at the same time. This is achived using threading and is recommended over a simple loop using single calls as multiple read operations can be performed at a time. It is recommended to keep the threads parameter between 2-8, but further tuning can be used to improve performance.

..  code-block:: python

    cosmosEndpoint      = "<my_cosmos_endpoint>"
    cosmosMasterKey     = "<my_cosmos_master_key>"
    cosmosDatabaseName  = "formats"
    cosmosContainerName = ["123", "456", "789"]

    df = read_cosmos(cosmosEndpoint, cosmosMasterKey, cosmosDatabaseName, cosmosContainerName, threads = 3)