get_memory_usage
================

.. autofunction:: hivecore.functions.get_memory_usage

Example
-------

To use the `get_memory_usage()` function, simply call it as follows:

.. code-block:: python

    from hivecore.functions import get_memory_usage

    memory_usage = get_memory_usage()
    print(f"Current memory usage: {memory_usage:.2f} MB")

Expected Output
---------------

The output will vary based on the memory usage of the process. It will display the current memory usage in megabytes.

.. code-block:: text

    Current memory usage: 153.45 MB