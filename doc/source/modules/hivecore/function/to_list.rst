to_list
=======

.. autofunction:: hivecore.function.to_list
   :noindex:

Example
^^^^^^^
..  code-block:: python
    
    from hivecore.function import to_list
    from pandas import DataFrame

    # Pandas DataFrame
    df = DataFrame({'a':[1,2,3]})

    # Apply function
    a = to_list(df.a)