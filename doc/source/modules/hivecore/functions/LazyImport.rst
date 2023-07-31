LazyImport
==========

.. autoclass:: hivecore.functions.LazyImport
    :members: import_, from_

Example
-------

To use the `LazyImport` class, simply call it as follows:

.. code-block:: python

    from hivecore.functions import get_memory_usage, LazyImport

    # Baseline memory usage
    print(f"Memory usage 1: {get_memory_usage()} MB")

    # Define LazyImports
    lazy_import = LazyImport()
    lazy_import.from_("numpy", "pi")
    lazy_import.from_("pandas", "DataFrame", "Series", DataFrame="DF")

    # No memory change should be observed yet
    print(f"Memory usage 2: {get_memory_usage()} MB")

    # Numpy should be imported here
    print("np.pi:", pi)
    print(f"Memory usage 3: {get_memory_usage()} MB")

    # Type-hinting does not trigger an import but still works
    x: DF = 5
    print(f"Memory usage 4: {get_memory_usage()} MB")

    # This should trigger the pandas import
    print("Series:", Series)
    print("DataFrame:", DataFrame)
    print(f"Memory usage 5: {get_memory_usage()} MB")



Expected Output
---------------

The output will vary based on the memory usage of the process. It will display the current memory usage in megabytes.

.. code-block:: text

    Memory usage 1: 71.3671875 MB
    Memory usage 2: 71.37109375 MB

    np.pi: 3.141592653589793
    
    Memory usage 3: 81.4375 MB
    Memory usage 4: 81.4375 MB
    
    Series: <class 'pandas.core.series.Series'>
    DataFrame: <class 'pandas.core.frame.DataFrame'>
    
    Memory usage 5: 110.8984375 MB