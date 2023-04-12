To List
=======

.. autofunction:: hiveadb.function.to_list
   :noindex:

Example
^^^^^^^
..  code-block:: python

    from hiveadb.function import to_list
 
    first_names, last_names = to_list(df, ['first_name', 'last_name'])