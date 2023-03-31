Singleton
=========

.. autofunction:: hivecore.decorator.singleton
   :noindex:

Example
^^^^^^^
..  code-block:: python
    
    from hivecore.decorator import singleton

    @singleton
    class my_class:
        def __init__(self):
            my_attr = None