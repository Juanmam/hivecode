LazyImport
==========

.. autoclass:: hivecore.functions.LazyImport
    :members: import_from

Example
-------

To use the `LazyImport` class, simply call it as follows:

.. code-block:: python

    from hivecore.functions import get_memory_usage, LazyImport

    # Baseline memory usage
    baseline_memory = get_memory_usage()
    print(f"Baseline memory usage: {baseline_memory} MB")

    # Set up a lazy import with Lazy, but don't actually use the imported object
    Lazy.import_from("numpy", np="")
    setup_memory_lazy = get_memory_usage()
    print(f"Memory usage after setting up lazy import with Lazy: {setup_memory_lazy} MB")

    # Use the imported object for the first time
    np = Lazy.np
    _ = np.array([1, 2, 3])
    used_memory_lazy = get_memory_usage()
    print(f"Memory usage after using the imported object with Lazy: {used_memory_lazy} MB")



Expected Output
---------------

The output will vary based on the memory usage of the process. It will display the current memory usage in megabytes.

.. code-block:: text

    Baseline memory usage: 62.92578125 MB
    Memory usage after setting up lazy import with Lazy: 62.9296875 MB
    Memory usage after using the imported object with Lazy: 72.51953125 MB