Select
======

.. autofunction:: hiveadb.function.select
   :noindex:

Example
^^^^^^^
..  code-block:: python

    from hiveadb.function import select

    sub_df = select(df, ['first_name','last_name','salary'])