Singleton
=========

.. deprecated:: future
   The `singleton` decorator will be moved to `hivecore.patterns` module in the future. Please update your imports accordingly.

.. autofunction:: hivecore.decorator.singleton
   :noindex:

Example
^^^^^^^
.. code-block:: python

    from hivecore.decorator import singleton

    @singleton
    class MyClass:
        def __init__(self):
            my_attr = None

